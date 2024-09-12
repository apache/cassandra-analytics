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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel.CL;
import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RingInstanceTest
{
    public static final String DATACENTER_1 = "DATACENTER1";

    static List<RingInstance> getInstances(BigInteger[] tokens, String datacenter)
    {
        List<RingInstance> instances = new ArrayList<>();
        for (int token = 0; token < tokens.length; token++)
        {
            instances.add(instance(tokens[token], "node-" + token, datacenter));
        }
        return instances;
    }

    static RingInstance instance(BigInteger token, String nodeName, String datacenter)
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

    private static ReplicaAwareFailureHandler<RingInstance> ntsStrategyHandler(Partitioner partitioner)
    {
        return new MultiClusterReplicaAwareFailureHandler<>(partitioner);
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
        RingInstance instance1 = mockRingInstance();
        RingInstance instance2 = mockRingInstance();
        assertEquals(instance1, instance2);
    }

    @Test
    public void testHashCode()
    {
        RingInstance instance1 = mockRingInstance();
        RingInstance instance2 = mockRingInstance();
        assertEquals(instance1.hashCode(), instance2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeIgnoreNonCriticalFields()
    {
        RingEntry.Builder builder = mockRingEntryBuilder();
        // the fields chained in the builder below are not considered for equality check
        RingInstance instance1 = new RingInstance(builder
                                                  .status("1")
                                                  .state("1")
                                                  .load("1")
                                                  .hostId("1")
                                                  .owns("1")
                                                  .build());
        RingInstance instance2 = new RingInstance(builder
                                                  .status("2")
                                                  .state("2")
                                                  .load("2")
                                                  .hostId("2")
                                                  .owns("2")
                                                  .build());
        assertEquals(instance1, instance2);
        assertEquals(instance1.hashCode(), instance2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeConsidersClusterId()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance c1i1 = new RingInstance(ringEntry, "cluster1");
        RingInstance c1i2 = new RingInstance(ringEntry, "cluster1");
        RingInstance c2i1 = new RingInstance(ringEntry, "cluster2");

        assertEquals(c1i1, c1i2);
        assertEquals(c1i1.hashCode(), c1i2.hashCode());

        assertNotEquals(c1i1, c2i1);
        assertNotEquals(c1i1.hashCode(), c2i1.hashCode());
    }

    @Test
    public void testHasClusterId()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance instance = new RingInstance(ringEntry);
        assertFalse(instance.hasClusterId());

        RingInstance instanceWithClusterId = new RingInstance(ringEntry, "cluster1");
        assertTrue(instanceWithClusterId.hasClusterId());
        assertEquals("cluster1", instanceWithClusterId.clusterId());
    }

    @Test
    public void multiMapWorksWithRingInstances()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance instance1 = new RingInstance(ringEntry);
        RingInstance instance2 = new RingInstance(ringEntry);
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
        RingInstance instance1 = instances.get(0);
        RingInstance instance2 = instance(tokens[0], instance1.nodeName(), instance1.datacenter());
        ReplicaAwareFailureHandler<RingInstance> replicationFactor3 = ntsStrategyHandler(partitioner);
        ReplicationFactor repFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                            ntsOptions(new String[]{DATACENTER_1 }, new int[]{3 }));
        Multimap<RingInstance, Range<BigInteger>> tokenRanges = TokenRangeMappingUtils.setupTokenRangeMap(partitioner, repFactor, instances);
        TokenRangeMapping<RingInstance> tokenRange = new TokenRangeMapping<>(partitioner,
                                                                             tokenRanges,
                                                                             Collections.emptySet());

        // This test proves that for any RF3 keyspace
        replicationFactor3.addFailure(Range.openClosed(tokens[0], tokens[1]), instance1, "Complete Failure");
        replicationFactor3.addFailure(Range.openClosed(tokens[0], tokens[0].add(BigInteger.ONE)), instance2, "Failure 1");
        replicationFactor3.addFailure(Range.openClosed(tokens[0].add(BigInteger.ONE),
                                                       tokens[0].add(BigInteger.valueOf(2L))), instance2, "Failure 2");

        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.getConsistencyLevel()).thenReturn(CL.LOCAL_QUORUM);
        when(jobInfo.getLocalDC()).thenReturn(DATACENTER_1);
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.replicationFactor()).thenReturn(repFactor);
        assertTrue(replicationFactor3.getFailedRanges(tokenRange, jobInfo, clusterInfo).isEmpty());
    }

    @NotNull
    private static RingEntry mockRingEntry()
    {
        return mockRingEntryBuilder().build();
    }

    @NotNull
    private static RingEntry.Builder mockRingEntryBuilder()
    {
        return new RingEntry.Builder()
               .datacenter("DATACENTER1")
               .address("127.0.0.1")
               .port(0)
               .rack("Rack")
               .status("UP")
               .state("NORMAL")
               .load("0")
               .token("0")
               .fqdn("DATACENTER1-i1")
               .hostId("")
               .owns("");
    }

    @NotNull
    private static RingInstance mockRingInstance()
    {
        return new RingInstance(mockRingEntry());
    }
}
