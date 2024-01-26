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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
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
 * <p>
 * {@link #equals(Object)} and {@link #hashCode()} don't take {@link #replicas} and {@link #tokenRangeMap}
 * into consideration as they are just derived fields.
 */
@SuppressWarnings({"UnstableApiUsage", "unused", "WeakerAccess"})
public class CassandraRing implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRing.class);

    private Partitioner partitioner;
    private String keyspace;
    private ReplicationFactor replicationFactor;
    private List<CassandraInstance> instances;

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
        this.init();
    }

    private void init()
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
        return new EqualsBuilder()
               .append(this.partitioner, that.partitioner)
               .append(this.keyspace, that.keyspace)
               .append(this.replicationFactor, that.replicationFactor)
               .append(this.instances, that.instances)
               .append(this.replicas, that.replicas)
               .append(this.tokenRangeMap, that.tokenRangeMap)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(partitioner)
               .append(keyspace)
               .append(replicationFactor)
               .append(instances)
               .append(replicas)
               .append(tokenRangeMap)
               .toHashCode();
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
        }

        @Override
        @SuppressWarnings("unchecked")
        public CassandraRing read(Kryo kryo, Input in, Class<CassandraRing> type)
        {
            return new CassandraRing(in.readByte() == 1 ? Partitioner.RandomPartitioner
                                                        : Partitioner.Murmur3Partitioner,
                                     in.readString(),
                                     kryo.readObject(in, ReplicationFactor.class),
                                     kryo.readObject(in, ArrayList.class));
        }
    }
}
