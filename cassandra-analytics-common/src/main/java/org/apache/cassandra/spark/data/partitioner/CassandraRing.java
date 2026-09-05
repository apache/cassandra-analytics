/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import java.util.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.utils.RangeUtils;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.SimpleStrategy;

/**
 * CassandraRing is designed to have one unique way of handling
 * Cassandra token/topology information across all Cassandra tooling.
 * This class is made Serializable so it's easy to use it from Hadoop/Spark.
 * As Cassandra token ranges are dependent on Replication strategy, ring makes sense for a specific keyspace only.
 * It is made to be immutable for the sake of simplicity.
 * <p>
 * Token ranges are calculated assuming Cassandra racks are not being used, but controlled by assigning tokens properly.
 * Callers that need rack-aware placement (e.g. sidecar {@code tokenRangeReplicas}, or any
 * external source-of-truth topology metadata) should use the 5-arg constructor and supply
 * an authoritative {@link RangeMap} keyed by {@code (start, end]} sub-ranges; see
 * CASSANALYTICS-79.
 * <p>
 * {@link #equals(Object)} and {@link #hashCode()} include {@link #replicas} and
 * {@link #tokenRangeMap}, so rings with the same {@code instances} but different
 * authoritative placements compare unequal. The fields remain {@code transient}:
 * {@link #init} rebuilds them from the serialized inputs before any comparison.
 */
@SuppressWarnings({"UnstableApiUsage", "unused", "WeakerAccess"})
public class CassandraRing implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRing.class);
    public static final Serializer SERIALIZER = new Serializer();

    private Partitioner partitioner;
    private String keyspace;
    private ReplicationFactor replicationFactor;
    private List<CassandraInstance> instances;

    // Authoritative per-range replicas supplied by the 5-arg constructor, in a flat
    // Serializable shape. Null when the 4-arg (naive) constructor was used. Carries the
    // input through Java/Kryo serde so the receiving end can rebuild the transient
    // {@code replicas} / {@code tokenRangeMap} fields identically.
    private List<RangeReplicas> authoritativeReplicas;

    private transient RangeMap<BigInteger, List<CassandraInstance>> replicas;
    private transient Multimap<CassandraInstance, Range<BigInteger>> tokenRangeMap;

    /**
     * Add a replica with given range to replicaMap (RangeMap pointing to replicas).
     * <p>
     * replicaMap starts with full range (representing complete ring) with empty list of replicas. So, it is
     * guaranteed that range will match one or many ranges in replicaMap.
     * <p>
     * Scheme to add a new replica for a range:
     *   * Find overlapping rangeMap entries from replicaMap
     *   * For each overlapping range, create new replica list by adding new replica to the existing list and add it
     *     back to replicaMap.
     */
    private static void addReplica(CassandraInstance replica,
                                   Range<BigInteger> range,
                                   RangeMap<BigInteger, List<CassandraInstance>> replicaMap)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "Range calculations assume range is not wrapped");

        RangeMap<BigInteger, List<CassandraInstance>> replicaRanges = replicaMap.subRangeMap(range);
        RangeMap<BigInteger, List<CassandraInstance>> mappingsToAdd = TreeRangeMap.create();

        replicaRanges.asMapOfRanges().forEach((key, value) -> {
            List<CassandraInstance> replicas = new ArrayList<>(value);
            replicas.add(replica);
            mappingsToAdd.put(key, replicas);
        });
        replicaMap.putAll(mappingsToAdd);
    }

    public CassandraRing(Partitioner partitioner,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Collection<CassandraInstance> instances)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.instances = instances.stream()
                                  .sorted(Comparator.comparing(instance -> new BigInteger(instance.token())))
                                  .collect(Collectors.toCollection(ArrayList::new));
        this.authoritativeReplicas = null;
        this.init();
    }

    /**
     * Authoritative-replica constructor. {@code authoritativeReplicas} is adopted verbatim
     * as the source of truth for per-range placement, bypassing the naive
     * {@link RangeUtils#calculateTokenRanges} derivation used by the 4-arg constructor.
     * Intended for callers that can supply a rack-aware mapping (e.g. sidecar
     * {@code tokenRangeReplicas} or any external source-of-truth topology metadata).
     * <p>
     * Validation (fail loud, {@link IllegalArgumentException}):
     * <ul>
     *   <li>each replica list is non-null and non-empty;</li>
     *   <li>each range has {@code lower < upper} (Guava's
     *       {@link Range#openClosed(Comparable, Comparable)} enforces this for individual
     *       entries; cross-entry inversion is caught here);</li>
     *   <li>every replica {@link CassandraInstance} is element-equal to one in
     *       {@code instances}, so the ring and the replica map agree on node identity;</li>
     *   <li>the union of supplied ranges equals {@code (minToken, maxToken]} (no gaps; an
     *       uncovered sub-range would be silently skipped by
     *       {@code TokenPartitioner.subRanges});</li>
     *   <li>no two supplied ranges overlap (Guava's {@link TreeRangeMap#put} would otherwise
     *       silently overwrite).</li>
     * </ul>
     */
    public CassandraRing(Partitioner partitioner,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Collection<CassandraInstance> instances,
                         RangeMap<BigInteger, List<CassandraInstance>> authoritativeReplicas)
    {
        Preconditions.checkArgument(authoritativeReplicas != null,
                                    "authoritativeReplicas must not be null; use the 4-arg constructor for naive derivation");
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.instances = instances.stream()
                                  .sorted(Comparator.comparing(instance -> new BigInteger(instance.token())))
                                  .collect(Collectors.toCollection(ArrayList::new));
        this.authoritativeReplicas = validateAndFlatten(authoritativeReplicas, this.instances,
                                                        partitioner);
        this.init();
    }

    /**
     * Validates the supplied authoritative mapping and flattens it into a serializable list.
     * See the 5-arg constructor javadoc for the invariants enforced here.
     */
    private static List<RangeReplicas> validateAndFlatten(RangeMap<BigInteger, List<CassandraInstance>> authoritative,
                                                          List<CassandraInstance> sortedInstances,
                                                          Partitioner partitioner)
    {
        Set<CassandraInstance> known = new HashSet<>(sortedInstances);
        Map<Range<BigInteger>, List<CassandraInstance>> asMap = authoritative.asMapOfRanges();
        Preconditions.checkArgument(!asMap.isEmpty(),
                                    "authoritativeReplicas must contain at least one range");

        List<RangeReplicas> flat = new ArrayList<>(asMap.size());
        for (Map.Entry<Range<BigInteger>, List<CassandraInstance>> entry : asMap.entrySet())
        {
            Range<BigInteger> range = entry.getKey();
            List<CassandraInstance> replicaList = entry.getValue();
            Preconditions.checkArgument(replicaList != null && !replicaList.isEmpty(),
                                        "replica list must be non-null and non-empty for range %s",
                                        range);
            // Defense in depth: Guava's openClosed enforces lower < upper at construction,
            // but this guard catches any non-canonical RangeMap input.
            Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) < 0,
                                        "range lower must be strictly less than upper: %s", range);
            for (CassandraInstance replica : replicaList)
            {
                Preconditions.checkArgument(known.contains(replica),
                                            "replica %s for range %s is not in the supplied instances collection",
                                            replica, range);
            }
            flat.add(new RangeReplicas(range.lowerEndpoint(), range.upperEndpoint(),
                                       new ArrayList<>(replicaList)));
        }

        // Sort by lower, then walk for contiguous full-ring tiling.
        flat.sort(Comparator.comparing(rr -> rr.lower));
        BigInteger expectedLower = partitioner.minToken();
        BigInteger expectedUpper = partitioner.maxToken();
        for (int i = 0; i < flat.size(); i++)
        {
            RangeReplicas rr = flat.get(i);
            if (i == 0)
            {
                Preconditions.checkArgument(rr.lower.equals(expectedLower),
                                            "authoritative replicas must start at partitioner minToken %s, got %s",
                                            expectedLower, rr.lower);
            }
            else
            {
                BigInteger prevUpper = flat.get(i - 1).upper;
                Preconditions.checkArgument(rr.lower.compareTo(prevUpper) >= 0,
                                            "authoritative replicas have overlapping ranges near lower=%s prevUpper=%s",
                                            rr.lower, prevUpper);
                Preconditions.checkArgument(rr.lower.equals(prevUpper),
                                            "authoritative replicas have a gap between prevUpper=%s and lower=%s",
                                            prevUpper, rr.lower);
            }
        }
        BigInteger lastUpper = flat.get(flat.size() - 1).upper;
        Preconditions.checkArgument(lastUpper.equals(expectedUpper),
                                    "authoritative replicas must end at partitioner maxToken %s, got %s",
                                    expectedUpper, lastUpper);
        return flat;
    }

    private void init()
    {
        if (authoritativeReplicas == null)
        {
            initFromInstancesNaive();
            return;
        }
        replicas = TreeRangeMap.create();
        tokenRangeMap = ArrayListMultimap.create();
        for (RangeReplicas rr : authoritativeReplicas)
        {
            Range<BigInteger> r = Range.openClosed(rr.lower, rr.upper);
            // Authoritative input is already aggregated and disjoint per validateAndFlatten,
            // so SET (put) rather than ADD-with-split-on-overlap (addReplica).
            replicas.put(r, rr.replicas);
            for (CassandraInstance i : rr.replicas)
            {
                tokenRangeMap.put(i, r);
            }
        }
    }

    /**
     * Rack-unaware naive derivation invoked when no authoritative per-range replica map is
     * supplied. Sorts tokens and assigns the next RF nodes as replicas, ignoring rack
     * placement; correct only on clusters that are not using NTS racks. Retained as a
     * fallback for the CDC and sidecar bulk-read paths; their migration to an authoritative
     * map is deferred to CASSANALYTICS-79.
     */
    private void initFromInstancesNaive()
    {
        // Setup token range map
        replicas = TreeRangeMap.create();
        tokenRangeMap = ArrayListMultimap.create();

        // Calculate instance to token ranges mapping
        switch (replicationFactor.getReplicationStrategy())
        {
            case SimpleStrategy:
                tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(instances,
                                                                     replicationFactor.getTotalReplicationFactor(),
                                                                     partitioner));
                break;
            case NetworkTopologyStrategy:
                for (String dataCenter : dataCenters())
                {
                    int rf = replicationFactor.getOptions().get(dataCenter);
                    if (rf == 0)
                    {
                        continue;
                    }
                    List<CassandraInstance> dcInstances = instances.stream()
                            .filter(instance -> instance.dataCenter().matches(dataCenter))
                            .collect(Collectors.toList());
                    tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(dcInstances,
                                                                         replicationFactor.getOptions().get(dataCenter),
                                                                         partitioner));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported replication strategy");
        }

        // Calculate token range to replica mapping
        replicas.put(Range.openClosed(partitioner.minToken(), partitioner.maxToken()), Collections.emptyList());
        tokenRangeMap.asMap().forEach((instance, ranges) -> ranges.forEach(range -> addReplica(instance, range, replicas)));
    }

    public Partitioner partitioner()
    {
        return partitioner;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public Collection<CassandraInstance> instances()
    {
        return instances;
    }

    public Collection<CassandraInstance> getReplicas(BigInteger token)
    {
        return replicas.get(token);
    }

    public RangeMap<BigInteger, List<CassandraInstance>> rangeMap()
    {
        return replicas;
    }

    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
    }

    public RangeMap<BigInteger, List<CassandraInstance>> getSubRanges(Range<BigInteger> tokenRange)
    {
        return replicas.subRangeMap(tokenRange);
    }

    public Multimap<CassandraInstance, Range<BigInteger>> tokenRanges()
    {
        return tokenRangeMap;
    }

    private Collection<String> dataCenters()
    {
        return replicationFactor.getReplicationStrategy() == SimpleStrategy
               ? Collections.emptySet()
               : replicationFactor.getOptions().keySet();
    }

    public Collection<BigInteger> tokens()
    {
        return instances.stream()
                        .map(CassandraInstance::token)
                        .map(BigInteger::new)
                        .sorted()
                        .collect(Collectors.toList());
    }

    public Collection<BigInteger> tokens(String dataCenter)
    {
        Preconditions.checkArgument(replicationFactor.getReplicationStrategy() != SimpleStrategy,
                                    "Datacenter tokens doesn't make sense for SimpleStrategy");
        return instances.stream()
                        .filter(instance -> instance.dataCenter().matches(dataCenter))
                        .map(CassandraInstance::token)
                        .map(BigInteger::new)
                        .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CassandraRing that = (CassandraRing) other;
        return this.partitioner == that.partitioner
               && Objects.equals(this.keyspace, that.keyspace)
               && Objects.equals(this.replicationFactor, that.replicationFactor)
               && Objects.equals(this.instances, that.instances)
               && Objects.equals(this.replicas, that.replicas)
               && Objects.equals(this.tokenRangeMap, that.tokenRangeMap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioner, keyspace, replicationFactor, instances, replicas, tokenRangeMap);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK deserialization");
        this.partitioner = in.readByte() == 0 ? Partitioner.RandomPartitioner : Partitioner.Murmur3Partitioner;
        this.keyspace = in.readUTF();

        ReplicationFactor.ReplicationStrategy strategy = ReplicationFactor.ReplicationStrategy.valueOf(in.readByte());
        int optionCount = in.readByte();
        Map<String, Integer> options = new HashMap<>(optionCount);
        for (int option = 0; option < optionCount; option++)
        {
            options.put(in.readUTF(), (int) in.readByte());
        }
        this.replicationFactor = new ReplicationFactor(strategy, options);

        int numInstances = in.readShort();
        this.instances = new ArrayList<>(numInstances);
        for (int instance = 0; instance < numInstances; instance++)
        {
            this.instances.add(new CassandraInstance(in.readUTF(), in.readUTF(), in.readUTF()));
        }

        // Authoritative replicas: boolean presence flag matches the readNullable/writeNullable
        // pattern used elsewhere in this package. Replica encoding mirrors the instance block
        // above (triple-UTF token/nodeName/dataCenter) so CassandraInstance itself does not need
        // to participate in serialization.
        if (in.readBoolean())
        {
            int rangeCount = in.readInt();
            List<RangeReplicas> flat = new ArrayList<>(rangeCount);
            for (int r = 0; r < rangeCount; r++)
            {
                BigInteger lower = new BigInteger(in.readUTF());
                BigInteger upper = new BigInteger(in.readUTF());
                int replicaCount = in.readShort();
                ArrayList<CassandraInstance> replicas = new ArrayList<>(replicaCount);
                for (int j = 0; j < replicaCount; j++)
                {
                    replicas.add(new CassandraInstance(in.readUTF(), in.readUTF(), in.readUTF()));
                }
                flat.add(new RangeReplicas(lower, upper, replicas));
            }
            this.authoritativeReplicas = flat;
        }
        this.init();
    }

    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK serialization");
        out.writeByte(this.partitioner == Partitioner.RandomPartitioner ? 0 : 1);
        out.writeUTF(this.keyspace);

        out.writeByte(this.replicationFactor.getReplicationStrategy().value);
        Map<String, Integer> options = this.replicationFactor.getOptions();
        out.writeByte(options.size());
        for (Map.Entry<String, Integer> option : options.entrySet())
        {
            out.writeUTF(option.getKey());
            out.writeByte(option.getValue());
        }

        out.writeShort(this.instances.size());
        for (CassandraInstance instance : this.instances)
        {
            out.writeUTF(instance.token());
            out.writeUTF(instance.nodeName());
            out.writeUTF(instance.dataCenter());
        }

        out.writeBoolean(this.authoritativeReplicas != null);
        if (this.authoritativeReplicas != null)
        {
            out.writeInt(this.authoritativeReplicas.size());
            for (RangeReplicas rr : this.authoritativeReplicas)
            {
                out.writeUTF(rr.lower.toString());
                out.writeUTF(rr.upper.toString());
                out.writeShort(rr.replicas.size());
                for (CassandraInstance i : rr.replicas)
                {
                    out.writeUTF(i.token());
                    out.writeUTF(i.nodeName());
                    out.writeUTF(i.dataCenter());
                }
            }
        }
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraRing>
    {
        @Override
        public void write(Kryo kryo, Output out, CassandraRing ring)
        {
            out.writeByte(ring.partitioner == Partitioner.RandomPartitioner ? 1 : 0);
            out.writeString(ring.keyspace);
            kryo.writeObject(out, ring.replicationFactor);
            kryo.writeObject(out, ring.instances);

            out.writeBoolean(ring.authoritativeReplicas != null);
            if (ring.authoritativeReplicas != null)
            {
                out.writeInt(ring.authoritativeReplicas.size());
                for (RangeReplicas rr : ring.authoritativeReplicas)
                {
                    out.writeString(rr.lower.toString());
                    out.writeString(rr.upper.toString());
                    out.writeShort(rr.replicas.size());
                    for (CassandraInstance i : rr.replicas)
                    {
                        out.writeString(i.token());
                        out.writeString(i.nodeName());
                        out.writeString(i.dataCenter());
                    }
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public CassandraRing read(Kryo kryo, Input in, Class<CassandraRing> type)
        {
            Partitioner partitioner = in.readByte() == 1 ? Partitioner.RandomPartitioner
                                                         : Partitioner.Murmur3Partitioner;
            String keyspace = in.readString();
            ReplicationFactor rf = kryo.readObject(in, ReplicationFactor.class);
            ArrayList<CassandraInstance> instances = kryo.readObject(in, ArrayList.class);

            if (!in.readBoolean())
            {
                return new CassandraRing(partitioner, keyspace, rf, instances);
            }
            int rangeCount = in.readInt();
            RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
            for (int r = 0; r < rangeCount; r++)
            {
                BigInteger lower = new BigInteger(in.readString());
                BigInteger upper = new BigInteger(in.readString());
                int replicaCount = in.readShort();
                List<CassandraInstance> replicas = new ArrayList<>(replicaCount);
                for (int j = 0; j < replicaCount; j++)
                {
                    replicas.add(new CassandraInstance(in.readString(), in.readString(), in.readString()));
                }
                auth.put(Range.openClosed(lower, upper), replicas);
            }
            return new CassandraRing(partitioner, keyspace, rf, instances, auth);
        }
    }

    /**
     * Flat {@link Serializable} carrier for a single {@code (start, end] -> replicas} entry.
     * Lives on {@link CassandraRing} as a non-transient list so the authoritative mapping
     * survives JDK and Kryo serialization without depending on Guava's {@link TreeRangeMap}
     * (which is not {@link Serializable}). On deserialize, {@link CassandraRing#init()}
     * rebuilds the transient {@code replicas} / {@code tokenRangeMap} fields from this list.
     */
    static final class RangeReplicas implements Serializable
    {
        private static final long serialVersionUID = 1L;
        final BigInteger lower;
        final BigInteger upper;
        final ArrayList<CassandraInstance> replicas;

        RangeReplicas(BigInteger lower, BigInteger upper, ArrayList<CassandraInstance> replicas)
        {
            this.lower = lower;
            this.upper = upper;
            this.replicas = replicas;
        }
    }
}
