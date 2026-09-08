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

package org.apache.cassandra.spark.data;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CassandraDataLayerTests
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS = ImmutableMap.of(
    "keyspace", "big-data",
    "table", "customers",
    "sidecar_contact_points", "localhost");

    @Test
    void testDefaultClearSnapshotStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        ClientConfig clientConfig = ClientConfig.create(options);
        assertThat(clientConfig.keyspace()).isEqualTo("big-data");
        assertThat(clientConfig.table()).isEqualTo("customers");
        assertThat(clientConfig.sidecarContactPoints()).isEqualTo("localhost");
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = clientConfig.clearSnapshotStrategy();
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo("2d");
    }

    @ParameterizedTest
    @CsvSource({"false, NOOP", "true,ONCOMPLETIONORTTL 2d"})
    void testClearSnapshotOptionSupport(Boolean clearSnapshot, String expectedClearSnapshotStrategyOption)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("clearsnapshot", clearSnapshot.toString());
        ClientConfig clientConfig = ClientConfig.create(options);
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = clientConfig.clearSnapshotStrategy();
        ClientConfig.ClearSnapshotStrategy expectedClearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(false, false, expectedClearSnapshotStrategyOption);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion())
        .isEqualTo(expectedClearSnapshotStrategy.shouldClearOnCompletion());
        assertThat(clearSnapshotStrategy.hasTTL()).isEqualTo(expectedClearSnapshotStrategy.hasTTL());
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedClearSnapshotStrategy.ttl());
    }

    // Sourcing token ranges from Cassandra, used for mutation tracked keyspaces where the locally derived
    // ranges cannot identify which instance replicates which range

    private static final String KS = "big-data";

    private static CassandraDataLayer dataLayer(String datacenter)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        if (datacenter != null)
        {
            options.put("dc", datacenter);
        }
        return new CassandraDataLayer(ClientConfig.create(options), Sidecar.ClientConfig.create(), null);
    }

    private static RingResponse ringOf(String... fqdnAndTokenAndDc)
    {
        RingResponse ring = new RingResponse();
        for (int i = 0; i < fqdnAndTokenAndDc.length; i += 3)
        {
            ring.add(new RingEntry.Builder().fqdn(fqdnAndTokenAndDc[i])
                                            .token(fqdnAndTokenAndDc[i + 1])
                                            .datacenter(fqdnAndTokenAndDc[i + 2])
                                            .address(fqdnAndTokenAndDc[i])
                                            .port(9042)
                                            .rack("rack1")
                                            .status("UP")
                                            .state("NORMAL")
                                            .load("1")
                                            .owns("1")
                                            .hostId("h" + i)
                                            .build());
        }
        return ring;
    }

    /**
     * Replica keys are "address:port" and are translated to fqdn through the replica metadata, mirroring what
     * Sidecar returns.
     */
    private static TokenRangeReplicasResponse topologyOf(List<ReplicaInfo> readReplicas, String... addressToFqdn)
    {
        Map<String, ReplicaMetadata> metadata = new HashMap<>();
        for (int i = 0; i < addressToFqdn.length; i += 2)
        {
            String key = addressToFqdn[i];
            String fqdn = addressToFqdn[i + 1];
            metadata.put(key, new ReplicaMetadata("NORMAL", "UP", fqdn, key.split(":")[0], 9042, "dc1"));
        }
        return new TokenRangeReplicasResponse(readReplicas, readReplicas, metadata);
    }

    @Test
    void testRingFromTokenRangeReplicasUsesCassandraReportedRanges()
    {
        CassandraDataLayer layer = dataLayer(null);
        RingResponse ring = ringOf("n1", "0", "dc1", "n2", "100", "dc1", "n3", "200", "dc1");

        // boundary at 50 with only two replicas: a shape the local derivation cannot produce
        List<ReplicaInfo> readReplicas = Arrays.asList(
        new ReplicaInfo("-9223372036854775808", "50", ImmutableMap.of("dc1", Arrays.asList("1.1.1.1:9042", "1.1.1.2:9042"))),
        new ReplicaInfo("50", "9223372036854775807", ImmutableMap.of("dc1", Arrays.asList("1.1.1.2:9042", "1.1.1.3:9042"))));
        TokenRangeReplicasResponse topology = topologyOf(readReplicas,
                                                         "1.1.1.1:9042", "n1",
                                                         "1.1.1.2:9042", "n2",
                                                         "1.1.1.3:9042", "n3");

        CassandraRing result = layer.createCassandraRingFromTokenRangeReplicas(
        Partitioner.Murmur3Partitioner, ReplicationFactor.simpleStrategy(3), ring, topology);

        assertThat(result.hasExplicitRanges()).isTrue();
        assertThat(replicaNames(result, 10L)).containsExactly("n1", "n2");
        assertThat(replicaNames(result, 1000L)).containsExactly("n2", "n3");
    }

    @Test
    void testRingFromTokenRangeReplicasFiltersByDatacenter()
    {
        CassandraDataLayer layer = dataLayer("dc1");
        RingResponse ring = ringOf("n1", "0", "dc1", "n2", "100", "dc1", "n4", "300", "dc2");

        List<ReplicaInfo> readReplicas = Collections.singletonList(
        new ReplicaInfo("-9223372036854775808", "9223372036854775807",
                        ImmutableMap.of("dc1", Arrays.asList("1.1.1.1:9042", "1.1.1.2:9042"),
                                        "dc2", Collections.singletonList("1.1.1.4:9042"))));
        TokenRangeReplicasResponse topology = topologyOf(readReplicas,
                                                         "1.1.1.1:9042", "n1",
                                                         "1.1.1.2:9042", "n2",
                                                         "1.1.1.4:9042", "n4");

        CassandraRing result = layer.createCassandraRingFromTokenRangeReplicas(
        Partitioner.Murmur3Partitioner, ReplicationFactor.simpleStrategy(2), ring, topology);

        // the dc2 replica must not appear, even though Cassandra reported it
        assertThat(replicaNames(result, 10L)).containsExactly("n1", "n2");
    }

    @Test
    void testRingFromTokenRangeReplicasSkipsReplicaAbsentFromRing()
    {
        CassandraDataLayer layer = dataLayer(null);
        RingResponse ring = ringOf("n1", "0", "dc1", "n2", "100", "dc1");

        List<ReplicaInfo> readReplicas = Collections.singletonList(
        new ReplicaInfo("-9223372036854775808", "9223372036854775807",
                        ImmutableMap.of("dc1", Arrays.asList("1.1.1.1:9042", "1.1.1.9:9042"))));
        TokenRangeReplicasResponse topology = topologyOf(readReplicas,
                                                         "1.1.1.1:9042", "n1",
                                                         "1.1.1.9:9042", "ghost");

        CassandraRing result = layer.createCassandraRingFromTokenRangeReplicas(
        Partitioner.Murmur3Partitioner, ReplicationFactor.simpleStrategy(2), ring, topology);

        // the ring response is the source of truth for reachable nodes, so the unknown replica is dropped
        assertThat(replicaNames(result, 10L)).containsExactly("n1");
    }

    @Test
    void testRingFromTokenRangeReplicasRejectsMultiTokenNode()
    {
        CassandraDataLayer layer = dataLayer(null);
        // same fqdn twice with different tokens, i.e. vnodes
        RingResponse ring = ringOf("n1", "0", "dc1", "n1", "100", "dc1");

        List<ReplicaInfo> readReplicas = Collections.singletonList(
        new ReplicaInfo("-9223372036854775808", "9223372036854775807",
                        ImmutableMap.of("dc1", Collections.singletonList("1.1.1.1:9042"))));
        TokenRangeReplicasResponse topology = topologyOf(readReplicas, "1.1.1.1:9042", "n1");

        assertThatThrownBy(() -> layer.createCassandraRingFromTokenRangeReplicas(
        Partitioner.Murmur3Partitioner, ReplicationFactor.simpleStrategy(1), ring, topology))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("owns multiple tokens");
    }

    @Test
    void testRingFromTokenRangeReplicasFailsWhenNoReadReplicas()
    {
        CassandraDataLayer layer = dataLayer(null);
        RingResponse ring = ringOf("n1", "0", "dc1");
        TokenRangeReplicasResponse topology = topologyOf(Collections.emptyList());

        assertThatThrownBy(() -> layer.createCassandraRingFromTokenRangeReplicas(
        Partitioner.Murmur3Partitioner, ReplicationFactor.simpleStrategy(1), ring, topology))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no read replicas");
    }

    @Test
    void testForceCassandraTokenRangesDefaultsToFalse()
    {
        assertThat(ClientConfig.create(new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS))
                               .forceCassandraTokenRanges()).isFalse();

        // Spark lowercases option keys, and MapUtils looks them up lowercased, so this is how a user sets it
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("forcecassandratokenranges", "true");
        assertThat(ClientConfig.create(options).forceCassandraTokenRanges()).isTrue();
    }

    private static List<String> replicaNames(CassandraRing ring, long token)
    {
        return ring.getReplicas(BigInteger.valueOf(token))
                   .stream()
                   .map(CassandraInstance::nodeName)
                   .sorted()
                   .collect(Collectors.toList());
    }
}
