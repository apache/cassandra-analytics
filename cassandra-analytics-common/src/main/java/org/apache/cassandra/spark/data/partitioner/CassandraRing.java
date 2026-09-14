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
import java.util.List;
import java.util.Map;
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
 * Token ranges are either supplied by Cassandra, or calculated locally. The local calculation assumes Cassandra racks
 * are not being used, but controlled by assigning tokens properly. Callers that cannot rely on that assumption, such
 * as the bulk reader for a mutation tracked keyspace, supply the ranges instead - see
 * {@link #CassandraRing(Partitioner, String, ReplicationFactor, Collection, Map)}.
 * <p>
 * {@link #equals(Object)} and {@link #hashCode()} don't take {@link #replicas} and {@link #tokenRangeMap}
 * into consideration as they are just derived fields.
 */
@SuppressWarnings({"UnstableApiUsage", "unused", "WeakerAccess"})
public class CassandraRing implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRing.class);
    public static final Serializer SERIALIZER = new Serializer();

    /**
     * Pinned so that the JDK serialization format change made when transient (witness) replica counts were added is
     * detected. Without it the UID is computed from the class signature, which did not change - only the
     * {@link #readObject}/{@link #writeObject} bodies did - so an older stream would be silently misread rather than
     * rejected.
     */
    private static final long serialVersionUID = 2026082800000000001L;

    /**
     * Incremented whenever the hand-rolled JDK serialization format below changes. Version 1 added the
     * per-datacenter transient (witness) replica counts after the replication options. Version 2 added the
     * optional Cassandra-reported token ranges after the instances.
     */
    private static final byte SERIALIZATION_FORMAT_VERSION = 2;

    private Partitioner partitioner;
    private String keyspace;
    private ReplicationFactor replicationFactor;
    private List<CassandraInstance> instances;
    /**
     * Token ranges supplied by Cassandra rather than derived locally from tokens and replication factor.
     * {@code null} means derive, which is the behaviour for every caller that predates witness replica support.
     * <p>
     * Deriving assumes racks are not in use (see the class javadoc), whereas Cassandra's replica assignment is
     * rack aware, so the derived ranges are only guaranteed to match for a single rack per datacenter. Supplying
     * the ranges removes that assumption.
     */
    private List<ExplicitRange> explicitRanges;

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
        this(partitioner, keyspace, replicationFactor, instances, null);
    }

    /**
     * Constructs a ring from token ranges reported by Cassandra, instead of deriving them from tokens and the
     * replication factor.
     *
     * @param partitioner       the partitioner
     * @param keyspace          the keyspace this ring describes
     * @param replicationFactor the keyspace replication factor
     * @param instances         all instances in the ring
     * @param rangeReplicas     range to replicas mapping as reported by Cassandra. Replicas must be present in
     *                          {@code instances}. Ranges must be open-closed and non-wrapping.
     */
    public CassandraRing(Partitioner partitioner,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Collection<CassandraInstance> instances,
                         Map<Range<BigInteger>, ? extends Collection<CassandraInstance>> rangeReplicas)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.instances = instances.stream()
                                  .sorted(Comparator.comparing(instance -> new BigInteger(instance.token())))
                                  .collect(Collectors.toCollection(ArrayList::new));
        this.explicitRanges = rangeReplicas == null ? null : toExplicitRanges(rangeReplicas, this.instances);
        this.init();
    }

    /**
     * Flattens the supplied mapping into a serializable form, resolving each replica to its index in
     * {@code sortedInstances} so instance details are not duplicated per range.
     */
    private static List<ExplicitRange> toExplicitRanges(
    Map<Range<BigInteger>, ? extends Collection<CassandraInstance>> rangeReplicas,
    List<CassandraInstance> sortedInstances)
    {
        Map<CassandraInstance, Integer> indexByInstance = new HashMap<>(sortedInstances.size());
        for (int index = 0; index < sortedInstances.size(); index++)
        {
            indexByInstance.put(sortedInstances.get(index), index);
        }

        List<ExplicitRange> result = new ArrayList<>(rangeReplicas.size());
        rangeReplicas.forEach((range, replicas) -> {
            Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                        "Supplied ranges must not wrap, found %s", range);
            List<Integer> indexes = new ArrayList<>(replicas.size());
            for (CassandraInstance replica : replicas)
            {
                Integer index = indexByInstance.get(replica);
                Preconditions.checkArgument(index != null,
                                            "Replica %s for range %s is not present in the ring instances",
                                            replica, range);
                indexes.add(index);
            }
            result.add(new ExplicitRange(range.lowerEndpoint(), range.upperEndpoint(), indexes));
        });
        result.sort(Comparator.comparing(r -> r.lower));
        return result;
    }

    /**
     * @return {@code true} if this ring uses token ranges reported by Cassandra rather than locally derived ones
     */
    public boolean hasExplicitRanges()
    {
        return explicitRanges != null;
    }

    private void init()
    {
        // Setup token range map
        replicas = TreeRangeMap.create();
        tokenRangeMap = ArrayListMultimap.create();

        if (explicitRanges != null)
        {
            for (ExplicitRange explicitRange : explicitRanges)
            {
                Range<BigInteger> range = Range.openClosed(explicitRange.lower, explicitRange.upper);
                for (int index : explicitRange.replicaIndexes)
                {
                    tokenRangeMap.put(instances.get(index), range);
                }
            }
        }
        else
        {
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
        }

        // Calculate token range to replica mapping
        replicas.put(Range.openClosed(partitioner.minToken(), partitioner.maxToken()), Collections.emptyList());
        tokenRangeMap.asMap().forEach((instance, ranges) -> ranges.forEach(range -> addReplica(instance, range, replicas)));
    }

    /**
     * A single Cassandra-reported token range and the instances replicating it, held by index into
     * {@link CassandraRing#instances}.
     */
    private static final class ExplicitRange implements Serializable
    {
        private static final long serialVersionUID = 2026090800000000001L;

        private final BigInteger lower;
        private final BigInteger upper;
        private final List<Integer> replicaIndexes;

        private ExplicitRange(BigInteger lower, BigInteger upper, List<Integer> replicaIndexes)
        {
            this.lower = lower;
            this.upper = upper;
            this.replicaIndexes = replicaIndexes;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            ExplicitRange that = (ExplicitRange) other;
            return Objects.equals(lower, that.lower)
                   && Objects.equals(upper, that.upper)
                   && Objects.equals(replicaIndexes, that.replicaIndexes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lower, upper, replicaIndexes);
        }

        @Override
        public String toString()
        {
            return "(" + lower + ", " + upper + "]=" + replicaIndexes;
        }
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
               && Objects.equals(this.explicitRanges, that.explicitRanges)
               && Objects.equals(this.replicas, that.replicas)
               && Objects.equals(this.tokenRangeMap, that.tokenRangeMap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioner, keyspace, replicationFactor, instances, explicitRanges, replicas, tokenRangeMap);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK deserialization");
        byte formatVersion = in.readByte();
        if (formatVersion != SERIALIZATION_FORMAT_VERSION)
        {
            throw new IOException(String.format("Unsupported CassandraRing serialization format version %d, expected %d",
                                                formatVersion, SERIALIZATION_FORMAT_VERSION));
        }
        this.partitioner = in.readByte() == 0 ? Partitioner.RandomPartitioner : Partitioner.Murmur3Partitioner;
        this.keyspace = in.readUTF();

        ReplicationFactor.ReplicationStrategy strategy = ReplicationFactor.ReplicationStrategy.valueOf(in.readByte());
        int optionCount = in.readByte();
        Map<String, Integer> options = new HashMap<>(optionCount);
        for (int option = 0; option < optionCount; option++)
        {
            options.put(in.readUTF(), (int) in.readByte());
        }
        int transientOptionCount = in.readByte();
        Map<String, Integer> transientOptions = new HashMap<>(transientOptionCount);
        for (int option = 0; option < transientOptionCount; option++)
        {
            transientOptions.put(in.readUTF(), (int) in.readByte());
        }
        this.replicationFactor = new ReplicationFactor(strategy, options, transientOptions);

        int numInstances = in.readShort();
        this.instances = new ArrayList<>(numInstances);
        for (int instance = 0; instance < numInstances; instance++)
        {
            this.instances.add(new CassandraInstance(in.readUTF(), in.readUTF(), in.readUTF()));
        }

        int numExplicitRanges = in.readInt();
        if (numExplicitRanges < 0)
        {
            this.explicitRanges = null;
        }
        else
        {
            List<ExplicitRange> ranges = new ArrayList<>(numExplicitRanges);
            for (int range = 0; range < numExplicitRanges; range++)
            {
                BigInteger lower = new BigInteger(in.readUTF());
                BigInteger upper = new BigInteger(in.readUTF());
                int numReplicas = in.readInt();
                List<Integer> indexes = new ArrayList<>(numReplicas);
                for (int replica = 0; replica < numReplicas; replica++)
                {
                    indexes.add(in.readInt());
                }
                ranges.add(new ExplicitRange(lower, upper, indexes));
            }
            this.explicitRanges = ranges;
        }
        this.init();
    }

    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK serialization");
        out.writeByte(SERIALIZATION_FORMAT_VERSION);
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
        Map<String, Integer> transientOptions = this.replicationFactor.getTransientOptions();
        out.writeByte(transientOptions.size());
        for (Map.Entry<String, Integer> option : transientOptions.entrySet())
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

        // -1 distinguishes "derive the ranges" from "explicitly zero ranges"
        if (this.explicitRanges == null)
        {
            out.writeInt(-1);
        }
        else
        {
            out.writeInt(this.explicitRanges.size());
            for (ExplicitRange range : this.explicitRanges)
            {
                out.writeUTF(range.lower.toString());
                out.writeUTF(range.upper.toString());
                out.writeInt(range.replicaIndexes.size());
                for (int index : range.replicaIndexes)
                {
                    out.writeInt(index);
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
            // -1 distinguishes "derive the ranges" from "explicitly zero ranges"
            if (ring.explicitRanges == null)
            {
                out.writeInt(-1);
            }
            else
            {
                out.writeInt(ring.explicitRanges.size());
                for (ExplicitRange range : ring.explicitRanges)
                {
                    out.writeString(range.lower.toString());
                    out.writeString(range.upper.toString());
                    out.writeInt(range.replicaIndexes.size());
                    for (int index : range.replicaIndexes)
                    {
                        out.writeInt(index);
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
            ReplicationFactor replicationFactor = kryo.readObject(in, ReplicationFactor.class);
            List<CassandraInstance> instances = kryo.readObject(in, ArrayList.class);

            int numExplicitRanges = in.readInt();
            if (numExplicitRanges < 0)
            {
                return new CassandraRing(partitioner, keyspace, replicationFactor, instances);
            }

            // Instances are sorted by the constructor, and were written in that order, so indexes still resolve
            List<CassandraInstance> sorted = instances.stream()
                                                      .sorted(Comparator.comparing(i -> new BigInteger(i.token())))
                                                      .collect(Collectors.toList());
            Map<Range<BigInteger>, List<CassandraInstance>> rangeReplicas = new HashMap<>(numExplicitRanges);
            for (int range = 0; range < numExplicitRanges; range++)
            {
                BigInteger lower = new BigInteger(in.readString());
                BigInteger upper = new BigInteger(in.readString());
                int numReplicas = in.readInt();
                List<CassandraInstance> replicas = new ArrayList<>(numReplicas);
                for (int replica = 0; replica < numReplicas; replica++)
                {
                    replicas.add(sorted.get(in.readInt()));
                }
                rangeReplicas.put(Range.openClosed(lower, upper), replicas);
            }
            return new CassandraRing(partitioner, keyspace, replicationFactor, instances, rangeReplicas);
        }
    }
}
