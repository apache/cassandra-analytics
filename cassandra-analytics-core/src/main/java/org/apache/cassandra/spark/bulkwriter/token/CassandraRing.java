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

package org.apache.cassandra.spark.bulkwriter.token;

import java.io.IOException;
import java.io.NotSerializableException;
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
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy;

/**
 * CassandraRing is designed to have one unique way of handling Cassandra token/topology information across all Cassandra
 * tooling. This class is made Serializable so it's easy to use it from Hadoop/Spark. As Cassandra token ranges are
 * dependent on Replication strategy, ring makes sense for a specific keyspace only. It is made to be immutable for the
 * sake of simplicity.
 *
 * Token ranges are calculated assuming Cassandra racks are not being used, but controlled by assigning tokens properly.
 * This is the case for now with managed clusters. We should re-think about this if it changes in future.
 *
 * {@link #equals(Object)} and {@link #hashCode()} doesn't take {@link #replicas} and {@link #tokenRangeMap} into
 * consideration as they are just derived fields.
 *
 * {@link CassandraInstance} doesn't extend {@link Serializable}. So, it is possible to use {@link CassandraRing} with
 * non serializable {@link CassandraInstance}. If we try to serialize ring with those instances, you will see
 * {@link NotSerializableException}. One of the serializable implementation of {@link CassandraInstance} is at
 * {@link RingInstance}.
 *
 * @param <Instance> ring instance type
 */
public class CassandraRing<Instance extends CassandraInstance> implements Serializable
{
    private final Partitioner partitioner;
    private final String keyspace;
    private transient ReplicationFactor replicationFactor;
    private transient ArrayList<Instance> instances;

    private transient RangeMap<BigInteger, List<Instance>> replicas;
    private transient Multimap<Instance, Range<BigInteger>> tokenRangeMap;

    /**
     * Add a replica with given range to replicaMap (RangeMap pointing to replicas).
     *
     * replicaMap starts with full range (representing complete ring) with empty list of replicas. So, it is
     * guaranteed that range will match one or many ranges in replicaMap.
     *
     * Scheme to add a new replica for a range:
     *   * Find overlapping rangeMap entries from replicaMap
     *   * For each overlapping range, create new replica list by adding new replica to the existing list and add it
     *     back to replicaMap.
     */
    private static <Instance extends CassandraInstance> void addReplica(Instance replica,
                                                                        Range<BigInteger> range,
                                                                        RangeMap<BigInteger, List<Instance>> replicaMap)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "Range calculations assume range is not wrapped");

        RangeMap<BigInteger, List<Instance>> replicaRanges = replicaMap.subRangeMap(range);
        RangeMap<BigInteger, List<Instance>> mappingsToAdd = TreeRangeMap.create();

        replicaRanges.asMapOfRanges().forEach((key, value) -> {
            List<Instance> replicas = new ArrayList<>(value);
            replicas.add(replica);
            mappingsToAdd.put(key, replicas);
        });
        replicaMap.putAll(mappingsToAdd);
    }

    /**
     * This is the only way of calculating {@link #replicas} and {@link #tokenRangeMap} either it is from Constructor or
     * from {@link #readObject(ObjectInputStream)}
     */
    private void setupTokenRangeMap()
    {
        replicas = TreeRangeMap.create();
        tokenRangeMap = ArrayListMultimap.create();

        // Calculate instance to token ranges mapping
        if (replicationFactor.getReplicationStrategy() == ReplicationStrategy.SimpleStrategy)
        {
            tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(instances,
                                                                 replicationFactor.getTotalReplicationFactor(),
                                                                 partitioner));
        }
        else if (replicationFactor.getReplicationStrategy() == ReplicationStrategy.NetworkTopologyStrategy)
        {
            for (String dataCenter : getDataCenters())
            {
                int rf = replicationFactor.getOptions().get(dataCenter);
                if (rf == 0)
                {
                    // Apparently, it is valid to have zero replication factor in Cassandra
                    continue;
                }
                List<Instance> dcInstances = instances.stream()
                                                      .filter(instance -> instance.getDataCenter().matches(dataCenter))
                                                      .collect(Collectors.toList());
                tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(dcInstances,
                                                                     replicationFactor.getOptions().get(dataCenter),
                                                                     partitioner));
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported replication strategy");
        }

        // Calculate token range to replica mapping
        replicas.put(Range.closed(partitioner.minToken(), partitioner.maxToken()), Collections.emptyList());
        tokenRangeMap.asMap().forEach((instance, ranges) -> ranges.forEach(range -> addReplica(instance, range, replicas)));
    }

    public CassandraRing(Partitioner partitioner,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Collection<Instance> instances)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.instances = instances.stream()
                                  .sorted(Comparator.comparing(instance -> new BigInteger(instance.getToken())))
                                  .collect(Collectors.toCollection(ArrayList::new));

        this.setupTokenRangeMap();
    }

    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public ReplicationFactor getReplicationFactor()
    {
        return replicationFactor;
    }

    public Collection<Instance> getInstances()
    {
        return instances;
    }

    public RangeMap<BigInteger, List<Instance>> getRangeMap()
    {
        return replicas;
    }

    public RangeMap<BigInteger, List<Instance>> getSubRanges(Range<BigInteger> tokenRange)
    {
        return replicas.subRangeMap(tokenRange);
    }

    public Collection<BigInteger> getTokens()
    {
        return instances.stream()
                        .map(CassandraInstance::getToken)
                        .map(BigInteger::new)
                        .sorted()
                        .collect(Collectors.toList());
    }

    public Collection<Range<BigInteger>> getTokenRanges(Instance instance)
    {
        return tokenRangeMap.get(instance);
    }

    public Multimap<Instance, Range<BigInteger>> getTokenRanges()
    {
        return tokenRangeMap;
    }

    public Collection<String> getDataCenters()
    {
        return replicationFactor.getReplicationStrategy() == ReplicationStrategy.SimpleStrategy
                ? Collections.emptySet()
                : replicationFactor.getOptions().keySet();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        CassandraRing<?> that = (CassandraRing<?>) other;
        return this.partitioner == that.partitioner
            && this.keyspace.equals(that.keyspace)
            && this.replicationFactor.getReplicationStrategy() == that.replicationFactor.getReplicationStrategy()
            && this.replicationFactor.getOptions().equals(that.replicationFactor.getOptions())
            && this.instances.equals(that.instances);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(partitioner)
               .append(keyspace)
               .append(replicationFactor)
               .append(instances)
               .hashCode();
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException
    {
        in.defaultReadObject();

        // Get replication factor from options - HashMap
        replicationFactor = new ReplicationFactor((HashMap<String, String>) in.readObject());

        // Read list of SerializableCasInstance
        instances = (ArrayList<Instance>) in.readObject();

        setupTokenRangeMap();
    }

    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.defaultWriteObject();

        // ReplicationFactor can be built from options. Build and serialize options
        HashMap<String, String> replicationOptions = new HashMap<>();
        replicationOptions.put("class", replicationFactor.getReplicationStrategy() == ReplicationStrategy.SimpleStrategy
                                         ? "org.apache.cassandra.locator.SimpleStrategy"
                                         : "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationFactor.getOptions().keySet().forEach(option -> replicationOptions.put(option, replicationFactor.getOptions().get(option).toString()));
        out.writeObject(replicationOptions);

        if (!instances.isEmpty() && !(instances.get(0) instanceof Serializable))
        {
            throw new NotSerializableException(String.format("Cassandra instances(%s) are not Serializable",
                                                             instances.get(0).getClass()));
        }

        // Write instances
        out.writeObject(instances);
    }
}
