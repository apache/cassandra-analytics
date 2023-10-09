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

package org.apache.cassandra.spark.bulkwriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RingInstanceTest
{
    public static final String DATACENTER_1 = "DATACENTER1";
    public static final String KEYSPACE = "KEYSPACE";

    static List<RingInstance> getInstances(BigInteger[] tokens, String datacenter)
    {
        List<RingInstance> instances = new ArrayList<>();
        for (int token = 0; token < tokens.length; token++)
        {
            instances.add(instance(tokens[token], "node-" + token, datacenter, "host"));
        }
        return instances;
    }

    static RingInstance instance(BigInteger token, String nodeName, String datacenter, String hostName)
    {
        return new RingInstance(new RingEntry.Builder()
                                .datacenter(datacenter)
                                .port(7000)
                                .address(nodeName)
                                .status("UP")
                                .state("NORMAL")
                                .token(token.toString())
                                .fqdn(nodeName)
                                .rack("rack")
                                .owns("")
                                .load("")
                                .hostId("")
                                .build());
    }

    static BigInteger[] getTokens(Partitioner partitioner, int nodes)
    {
        BigInteger[] tokens = new BigInteger[nodes];

        for (int node = 0; node < nodes; node++)
        {
            tokens[node] = partitioner == Partitioner.Murmur3Partitioner
                           ? getMurmur3Token(nodes, node)
                           : getRandomToken(nodes, node);
        }
        return tokens;
    }

    static BigInteger getRandomToken(int nodes, int index)
    {
        // ((2^127 / nodes) * i)
        return ((BigInteger.valueOf(2).pow(127)).divide(BigInteger.valueOf(nodes))).multiply(BigInteger.valueOf(index));
    }

    static BigInteger getMurmur3Token(int nodes, int index)
    {
        // (((2^64 / n) * i) - 2^63)
        return (((BigInteger.valueOf(2).pow(64)).divide(BigInteger.valueOf(nodes)))
                .multiply(BigInteger.valueOf(index))).subtract(BigInteger.valueOf(2).pow(63));
    }

    private static ReplicaAwareFailureHandler<CassandraInstance> ntsStrategyHandler(Partitioner partitioner)
    {
        return new ReplicaAwareFailureHandler<>(partitioner);
    }

    private static Map<String, Integer> ntsOptions(String[] names, int[] values)
    {
        assert names.length == values.length : "Invalid replication options - array lengths do not match";
        Map<String, Integer> options = Maps.newHashMap();
        for (int name = 0; name < names.length; name++)
        {
            options.put(names[name], values[name]);
        }
        return options;
    }

    @Test
    public void testEquals()
    {
        RingInstance instance1 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        RingInstance instance2 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        assertEquals(instance1, instance2);
    }

    @Test
    public void testHashCode()
    {
        RingInstance instance1 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        RingInstance instance2 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        assertEquals(instance1.hashCode(), instance2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeIgnoreHost()
    {
        RingInstance realInstance = new RingInstance(new RingEntry.Builder()
                                                     .datacenter("DATACENTER1")
                                                     .address("127.0.0.1")
                                                     .port(7000)
                                                     .rack("Rack")
                                                     .status("UP")
                                                     .state("NORMAL")
                                                     .load("0")
                                                     .token("0")
                                                     .fqdn("fqdn")
                                                     .hostId("")
                                                     .owns("")
                                                     .build());

        RingInstance questionInstance = new RingInstance(new RingEntry.Builder()
                                                         .datacenter("DATACENTER1")
                                                         .address("127.0.0.1")
                                                         .port(7000)
                                                         .rack("Rack")
                                                         .status("UP")
                                                         .state("NORMAL")
                                                         .load("0")
                                                         .token("0")
                                                         .fqdn("fqdn")
                                                         .hostId("")
                                                         .owns("")
                                                         .build());
        assertEquals(realInstance, questionInstance);
        assertEquals(realInstance.hashCode(), questionInstance.hashCode());
    }

    @Test
    public void multiMapWorksWithRingInstances()
    {
        RingInstance instance1 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        RingInstance instance2 = TokenRangeMappingUtils.getInstances(0, ImmutableMap.of("DATACENTER1", 1), 1).get(0);
        byte[] buffer;

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(instance2);
            out.close();
            buffer = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
            ObjectInputStream in = new ObjectInputStream(bais);
            instance2 = (RingInstance) in.readObject();
            in.close();
        }
        catch (final IOException | ClassNotFoundException exception)
        {
            throw new RuntimeException(exception);
        }
        Multimap<RingInstance, String> initialErrorMap = ArrayListMultimap.create();
        initialErrorMap.put(instance1, "Failure 1");
        Multimap<RingInstance, String> newErrorMap = ArrayListMultimap.create(initialErrorMap);
        newErrorMap.put(instance2, "Failure2");

        assertEquals(1, newErrorMap.keySet().size());
    }

    @Test
    public void testMultipleFailuresSingleInstanceSucceedRF3()
    {
        Partitioner partitioner = Partitioner.Murmur3Partitioner;
        BigInteger[] tokens = getTokens(partitioner, 5);
        List<RingInstance> instances = getInstances(tokens, DATACENTER_1);
        CassandraInstance instance1 = instances.get(0);
        CassandraInstance instance2 = instance(tokens[0], instance1.getNodeName(), instance1.getDataCenter(), "?");
        ReplicaAwareFailureHandler<CassandraInstance> replicationFactor3 = ntsStrategyHandler(partitioner);
        ReplicationFactor repFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                            ntsOptions(new String[]{DATACENTER_1 }, new int[]{3 }));
        Map<String, Set<String>> writeReplicas = instances.stream()
                                                          .collect(Collectors.groupingBy(CassandraInstance::getDataCenter,
                                                                                         Collectors.mapping(CassandraInstance::getNodeName,
                                                                                                            Collectors.toSet())));
        Multimap<RingInstance, Range<BigInteger>> tokenRanges = TokenRangeMappingUtils.setupTokenRangeMap(partitioner, repFactor, instances);
        TokenRangeMapping<RingInstance> tokenRange = new TokenRangeMapping<>(partitioner,
                                                                             repFactor,
                                                                             writeReplicas,
                                                                             Collections.emptyMap(),
                                                                             tokenRanges,
                                                                             Collections.emptyList(),
                                                                             Collections.emptySet(),
                                                                             Collections.emptySet());

        // This test proves that for any RF3 keyspace
        replicationFactor3.addFailure(Range.openClosed(tokens[0], tokens[1]), instance1, "Complete Failure");
        replicationFactor3.addFailure(Range.openClosed(tokens[0], tokens[0].add(BigInteger.ONE)), instance2, "Failure 1");
        replicationFactor3.addFailure(Range.openClosed(tokens[0].add(BigInteger.ONE),
                                                       tokens[0].add(BigInteger.valueOf(2L))), instance2, "Failure 2");

        replicationFactor3.getFailedEntries(tokenRange, ConsistencyLevel.CL.LOCAL_QUORUM, DATACENTER_1);
        assertFalse(replicationFactor3.hasFailed(tokenRange, ConsistencyLevel.CL.LOCAL_QUORUM, DATACENTER_1));
    }
}
