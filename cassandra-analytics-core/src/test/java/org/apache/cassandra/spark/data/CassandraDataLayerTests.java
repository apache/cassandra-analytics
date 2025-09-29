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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.apache.cassandra.spark.data.CassandraDataLayer.dcReplicasByRange;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void testDcReplicasByRangeMultiDC()
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> readReplicas = List.of(
                new TokenRangeReplicasResponse.ReplicaInfo("-5000", "5000",
                        Map.of(
                            "dc1", List.of("localhost1:9000", "localhost2:9001", "localhost3:9002"),
                            "dc2", List.of("localhost4:9003"))));

        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> replicaMetadata = Map.of(
                "localhost1:9000", new TokenRangeReplicasResponse.ReplicaMetadata("Normal", "Up", "replica1-1", "localhost1", 9000, "dc1"),
                "localhost2:9001", new TokenRangeReplicasResponse.ReplicaMetadata("Normal", "Up", "replica1-2", "localhost2", 9001, "dc1"),
                "localhost3:9002", new TokenRangeReplicasResponse.ReplicaMetadata("Normal", "Up", "replica1-3", "localhost3", 9002, "dc1"),
                "localhost4:9003", new TokenRangeReplicasResponse.ReplicaMetadata("Normal", "Up", "replica2-1", "localhost4", 9003, "dc2")
        );

        TokenRangeReplicasResponse response =
                new TokenRangeReplicasResponse(Collections.EMPTY_LIST, readReplicas, replicaMetadata);

        Map<Range<BigInteger>, List<String>> expectedDc1 = Map.of(
                Range.openClosed(new BigInteger("-5000"), new BigInteger("5000")), List.of("replica1-1", "replica1-2", "replica1-3"));
        Map<Range<BigInteger>, List<String>> actualDc1 = dcReplicasByRange(response, "dc1");

        assertThat(actualDc1).isEqualTo(expectedDc1);

        Map<Range<BigInteger>, List<String>> expectedDc2 = Map.of(
                Range.openClosed(new BigInteger("-5000"), new BigInteger("5000")), List.of("replica2-1"));
        Map<Range<BigInteger>, List<String>> actualDc2 = dcReplicasByRange(response, "dc2");

        assertThat(actualDc2).isEqualTo(expectedDc2);
    }
}
