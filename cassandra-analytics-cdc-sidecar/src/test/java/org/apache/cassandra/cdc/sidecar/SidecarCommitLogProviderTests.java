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

package org.apache.cassandra.cdc.sidecar;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SidecarCommitLogProviderTests
{
    private static final List<CassandraInstance> INSTANCES = Arrays.asList(
    new CassandraInstance("0", "local1", "DC1"),
    new CassandraInstance("100", "local2", "DC1"),
    new CassandraInstance("200", "local3", "DC1"),
    new CassandraInstance("300", "local4", "DC1"),
    new CassandraInstance("400", "local5", "DC1"),
    new CassandraInstance("500", "local6", "DC1"),
    new CassandraInstance("1", "local7", "DC2"),
    new CassandraInstance("101", "local8", "DC2"),
    new CassandraInstance("201", "local9", "DC2"),
    new CassandraInstance("301", "local10", "DC2"),
    new CassandraInstance("401", "local11", "DC2"),
    new CassandraInstance("501", "local12", "DC2")
    );

    @Test
    public void testCommitLogProvider()
    {
        int aPrimeNumber = 13;
        CassandraInstance instance1 = INSTANCES.get(0);
        CassandraInstance instance2 = INSTANCES.get(1);
        CassandraInstance instance3 = INSTANCES.get(2);
        CassandraInstance instance4 = INSTANCES.get(3);
        CassandraInstance instance5 = INSTANCES.get(4);
        CassandraInstance instance6 = INSTANCES.get(5);

        ClusterConfigProvider clusterConfigProvider = getClusterConfigProvider();
        SidecarCdcClient sidecarCdcClient = mock(SidecarCdcClient.class);
        Map<CassandraInstance, Integer> instanceListCount = new HashMap<>();
        MutableInt segmentIdGenerator = new MutableInt(0);
        Answer<CompletableFuture<List<CommitLog>>> listCommitLogAnswer = invocation -> {
            CassandraInstance instance = invocation.getArgument(0, CassandraInstance.class);
            instanceListCount.compute(instance, (key, prev) -> prev == null ? 1 : prev + 1);
            int instanceId = Integer.parseInt(instance.nodeName().substring(5));
            int numLogs = instanceId * 13;
            List<CommitLog> logs = IntStream.rangeClosed(1, numLogs)
                                            .mapToObj(segmentId -> mockCommitLog(instance, segmentIdGenerator.getAndAdd(1)))
                                            .collect(Collectors.toList());
            return CompletableFuture.completedFuture(logs);
        };

        when(sidecarCdcClient.listCdcCommitLogSegments(any(CassandraInstance.class))).thenAnswer(listCommitLogAnswer);
        SidecarCommitLogProvider commitLogProvider = getSidecarCommitLogProvider(clusterConfigProvider, sidecarCdcClient);

        // list logs on instance1, instance2, instance3
        TokenRange tokenRange = TokenRange.openClosed(
        new BigInteger("-50"),
        new BigInteger("-10")
        );
        List<CommitLog> logs = commitLogProvider
                               .logs(tokenRange)
                               .sorted()
                               .collect(Collectors.toList());
        int expectedNumberOfLogs = aPrimeNumber + (2 * aPrimeNumber) + (3 * aPrimeNumber);
        assertThat(logs).hasSize(expectedNumberOfLogs);
        IntStream.range(0, logs.size())
                 .forEach(index -> assertThat(logs.get(index).segmentId()).isEqualTo(index));
        assertThat(instanceListCount).hasSize(3);
        assertThat(instanceListCount.get(instance1)).isEqualTo(1);
        assertThat(instanceListCount.get(instance2)).isEqualTo(1);
        assertThat(instanceListCount.get(instance3)).isEqualTo(1);

        // list logs on instance4, instance5, instance6
        TokenRange tokenRange2 = TokenRange.openClosed(
        new BigInteger("210"),
        new BigInteger("280")
        );
        List<CommitLog> logs2 = commitLogProvider
                                .logs(tokenRange2)
                                .collect(Collectors.toList());
        expectedNumberOfLogs = (4 * aPrimeNumber) + (5 * aPrimeNumber) + (6 * aPrimeNumber);
        assertThat(logs2).hasSize(expectedNumberOfLogs);
        IntStream.range(0, logs2.size())
                 .forEach(index -> assertThat(logs2.get(index).segmentId()).isEqualTo(index + logs.size()));
        assertThat(instanceListCount).hasSize(6);
        assertThat(instanceListCount.get(instance1)).isEqualTo(1);
        assertThat(instanceListCount.get(instance2)).isEqualTo(1);
        assertThat(instanceListCount.get(instance3)).isEqualTo(1);
        assertThat(instanceListCount.get(instance4)).isEqualTo(1);
        assertThat(instanceListCount.get(instance5)).isEqualTo(1);
        assertThat(instanceListCount.get(instance6)).isEqualTo(1);
    }

    @NotNull
    private static SidecarCommitLogProvider getSidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider, SidecarCdcClient sidecarCdcClient)
    {
        ReplicationFactorSupplier replicationFactorSupplier = new ReplicationFactorSupplier()
        {
            @Override
            public ReplicationFactor getReplicationFactor(String keyspace)
            {
                return new ReplicationFactor(ImmutableMap.of(
                "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                "DC1", "3",
                "DC2", "3"));
            }

            @Override
            public ReplicationFactor getMaximalReplicationFactor()
            {
                return getReplicationFactor("ks");
            }
        };
        return new SidecarCommitLogProvider(
        clusterConfigProvider,
        sidecarCdcClient,
        SidecarDownMonitor.STUB,
        replicationFactorSupplier
        );
    }

    private static CommitLog mockCommitLog(CassandraInstance instance, long segmentId)
    {
        CommitLog commitLog = mock(CommitLog.class);
        when(commitLog.instance()).thenReturn(instance);
        when(commitLog.name()).thenReturn("CommitLog-6-" + segmentId + ".log");
        when(commitLog.segmentId()).thenCallRealMethod();
        return commitLog;
    }

    @NotNull
    private static ClusterConfigProvider getClusterConfigProvider()
    {
        return new ClusterConfigProvider()
        {
            @Override
            public String dc()
            {
                return "DC1";
            }

            @Override
            public Set<CassandraInstance> getCluster()
            {
                return new HashSet<>(INSTANCES);
            }

            @Override
            public Partitioner partitioner()
            {
                return Partitioner.Murmur3Partitioner;
            }
        };
    }
}
