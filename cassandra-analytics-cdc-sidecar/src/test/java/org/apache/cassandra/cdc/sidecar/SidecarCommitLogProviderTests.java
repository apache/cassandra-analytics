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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    // Cassandra's rack-aware placement for DC1, which differs from the replicas derived from tokens: the range ending
    // at 0 is replicated by local1, local3 and local5 rather than by the next three nodes in ring order
    private static final List<ReplicaInfo> CASSANDRA_REPLICAS = Arrays.asList(
    new ReplicaInfo("-9223372036854775808", "0", ImmutableMap.of("DC1", replicaKeys(1, 3, 5), "DC2", replicaKeys(7))),
    new ReplicaInfo("0", "100", ImmutableMap.of("DC1", replicaKeys(2, 4, 6))),
    new ReplicaInfo("100", "9223372036854775807", ImmutableMap.of("DC1", replicaKeys(1, 2, 3))));

    @Test
    public void testReplicasAreTakenFromCassandra()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        when(client.tokenRangeReplicas(any(), eq("ks"))).thenReturn(CompletableFuture.completedFuture(cassandraReplicas()));
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, SidecarCdcOptions.DEFAULT, new AtomicLong());

        // the instances in DC2 are not read, as CDC runs in DC1
        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local3", "local5");
        // a token range spanning several ranges is read from the replicas of each
        assertThat(listedInstances(provider, listed, "-50", "50"))
        .containsExactlyInAnyOrder("local1", "local2", "local3", "local4", "local5", "local6");
    }

    @Test
    public void testReplicasAreDerivedWhenCassandraCannotSupplyThem()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        CompletableFuture<TokenRangeReplicasResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Sidecar unavailable"));
        when(client.tokenRangeReplicas(any(), eq("ks"))).thenReturn(failed);
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, SidecarCdcOptions.DEFAULT, new AtomicLong());

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local2", "local3");
    }

    @Test
    public void testReplicasAreDerivedWhenAReplicaIsNotInTheCluster()
    {
        // local5 replicates the range ending at 0, but the cluster does not list it, so reading only the other
        // replicas Cassandra reports could miss its mutations
        Set<CassandraInstance> cluster = new HashSet<>(INSTANCES);
        cluster.remove(INSTANCES.get(4));
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        when(client.tokenRangeReplicas(any(), eq("ks"))).thenReturn(CompletableFuture.completedFuture(cassandraReplicas()));
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, cluster, SidecarCdcOptions.DEFAULT, new AtomicLong());

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local2", "local3");
    }

    @Test
    public void testAFailedFetchIsRetriedSooner()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        CompletableFuture<TokenRangeReplicasResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Sidecar unavailable"));
        when(client.tokenRangeReplicas(any(), eq("ks")))
        .thenReturn(failed)
        .thenReturn(CompletableFuture.completedFuture(cassandraReplicas()));
        AtomicLong nanoTime = new AtomicLong();
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, SidecarCdcOptions.DEFAULT, nanoTime);

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local2", "local3");
        listedInstances(provider, listed, "-50", "-10");
        verify(client, times(1)).tokenRangeReplicas(any(), eq("ks"));

        // well before the refresh interval
        nanoTime.addAndGet(SidecarCommitLogProvider.RETRY_INTERVAL.toNanos());
        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local3", "local5");
        verify(client, times(2)).tokenRangeReplicas(any(), eq("ks"));
    }

    @Test
    public void testEmptyWriteReplicasFallBackToReadReplicas()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        TokenRangeReplicasResponse response = cassandraReplicas();
        TokenRangeReplicasResponse readOnly = new TokenRangeReplicasResponse(Collections.emptyList(), response.writeReplicas(),
                                                                             response.replicaMetadata());
        when(client.tokenRangeReplicas(any(), eq("ks"))).thenReturn(CompletableFuture.completedFuture(readOnly));
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, SidecarCdcOptions.DEFAULT, new AtomicLong());

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local3", "local5");
    }

    @Test
    public void testDeriveTokenRanges()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        SidecarCdcOptions options = new SidecarCdcOptions()
        {
            @Override
            public boolean deriveTokenRanges()
            {
                return true;
            }
        };
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, options, new AtomicLong());

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local2", "local3");
        verify(client, never()).tokenRangeReplicas(any(), any());
    }

    @Test
    public void testReplicasAreRefreshedAfterTheIntervalOrWhenTheClusterChanges()
    {
        Set<CassandraInstance> cluster = new HashSet<>(INSTANCES);
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        when(client.tokenRangeReplicas(any(), eq("ks"))).thenReturn(CompletableFuture.completedFuture(cassandraReplicas()));
        AtomicLong nanoTime = new AtomicLong();
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, cluster, SidecarCdcOptions.DEFAULT, nanoTime);

        listedInstances(provider, listed, "-50", "-10");
        listedInstances(provider, listed, "-50", "-10");
        verify(client, times(1)).tokenRangeReplicas(any(), eq("ks"));

        nanoTime.addAndGet(SidecarCdcOptions.DEFAULT.tokenRangeReplicasRefreshInterval().toNanos());
        listedInstances(provider, listed, "-50", "-10");
        verify(client, times(2)).tokenRangeReplicas(any(), eq("ks"));

        cluster.remove(INSTANCES.get(11));
        listedInstances(provider, listed, "-50", "-10");
        verify(client, times(3)).tokenRangeReplicas(any(), eq("ks"));
    }

    @Test
    public void testPreviousReplicasAreReusedWhenARefreshFails()
    {
        Set<String> listed = new HashSet<>();
        SidecarCdcClient client = recordingClient(listed);
        CompletableFuture<TokenRangeReplicasResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Sidecar unavailable"));
        when(client.tokenRangeReplicas(any(), eq("ks")))
        .thenReturn(CompletableFuture.completedFuture(cassandraReplicas()))
        .thenReturn(failed);
        AtomicLong nanoTime = new AtomicLong();
        SidecarCommitLogProvider provider = cassandraReplicasProvider(client, SidecarCdcOptions.DEFAULT, nanoTime);

        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local3", "local5");
        nanoTime.addAndGet(SidecarCdcOptions.DEFAULT.tokenRangeReplicasRefreshInterval().toNanos());
        assertThat(listedInstances(provider, listed, "-50", "-10")).containsExactlyInAnyOrder("local1", "local3", "local5");
        verify(client, times(2)).tokenRangeReplicas(any(), eq("ks"));
    }

    private static Set<String> listedInstances(SidecarCommitLogProvider provider,
                                               Set<String> listed,
                                               String start,
                                               String end)
    {
        listed.clear();
        provider.logs(TokenRange.openClosed(new BigInteger(start), new BigInteger(end))).collect(Collectors.toList());
        return new HashSet<>(listed);
    }

    private static SidecarCdcClient recordingClient(Set<String> listed)
    {
        SidecarCdcClient client = mock(SidecarCdcClient.class);
        when(client.listCdcCommitLogSegments(any(CassandraInstance.class))).thenAnswer(invocation -> {
            listed.add(invocation.getArgument(0, CassandraInstance.class).nodeName());
            return CompletableFuture.completedFuture(Collections.emptyList());
        });
        return client;
    }

    private static SidecarCommitLogProvider cassandraReplicasProvider(SidecarCdcClient client,
                                                                      SidecarCdcOptions options,
                                                                      AtomicLong nanoTime)
    {
        return cassandraReplicasProvider(client, new HashSet<>(INSTANCES), options, nanoTime);
    }

    private static SidecarCommitLogProvider cassandraReplicasProvider(SidecarCdcClient client,
                                                                      Set<CassandraInstance> cluster,
                                                                      SidecarCdcOptions options,
                                                                      AtomicLong nanoTime)
    {
        CqlTable table = mock(CqlTable.class);
        when(table.keyspace()).thenReturn("ks");
        when(table.cdc()).thenReturn(true);
        SchemaSupplier schemaSupplier = () -> CompletableFuture.completedFuture(Collections.singleton(table));
        ClusterConfigProvider clusterConfigProvider = new ClusterConfigProvider()
        {
            @Override
            public String dc()
            {
                return "DC1";
            }

            @Override
            public Set<CassandraInstance> getCluster()
            {
                return new HashSet<>(cluster);
            }

            @Override
            public Partitioner partitioner()
            {
                return Partitioner.Murmur3Partitioner;
            }
        };
        return new SidecarCommitLogProvider(clusterConfigProvider, client, SidecarDownMonitor.STUB, replicationFactorSupplier(),
                                            schemaSupplier, options, nanoTime::get);
    }

    private static TokenRangeReplicasResponse cassandraReplicas()
    {
        Map<String, ReplicaMetadata> metadata = new HashMap<>();
        for (int node = 1; node <= INSTANCES.size(); node++)
        {
            String dc = node <= 6 ? "DC1" : "DC2";
            metadata.put("10.0.0." + node + ":9042", new ReplicaMetadata("NORMAL", "UP", "local" + node, "10.0.0." + node, 9042, dc));
        }
        return new TokenRangeReplicasResponse(CASSANDRA_REPLICAS, Collections.emptyList(), metadata);
    }

    private static List<String> replicaKeys(int... nodes)
    {
        return Arrays.stream(nodes).mapToObj(node -> "10.0.0." + node + ":9042").collect(Collectors.toList());
    }

    @NotNull
    private static SidecarCommitLogProvider getSidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider, SidecarCdcClient sidecarCdcClient)
    {
        return new SidecarCommitLogProvider(
        clusterConfigProvider,
        sidecarCdcClient,
        SidecarDownMonitor.STUB,
        replicationFactorSupplier()
        );
    }

    private static ReplicationFactorSupplier replicationFactorSupplier()
    {
        return new ReplicationFactorSupplier()
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
