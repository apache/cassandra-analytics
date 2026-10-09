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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.common.response.TableStatsResponse;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import org.apache.cassandra.clients.Sidecar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CassandraDataLayerTests
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS = ImmutableMap.of(
    "keyspace", "big-data",
    "table", "customers",
    "sidecar_contact_points", "localhost");

    @Test
    void testDynamicSizingUsesQuotedIdentifiers()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("keyspace", "MyKeyspace");
        options.put("table", "MyTable");
        options.put(ClientConfig.SIZING_KEY, ClientConfig.SIZING_DYNAMIC);
        options.put(ClientConfig.NUM_CORES_KEY, "2");
        options.put("consistencylevel", "ONE");
        ClientConfig clientConfig = ClientConfig.create(options);
        CassandraDataLayer layer = new CassandraDataLayer(clientConfig, mock(Sidecar.ClientConfig.class), null);
        layer.maybeQuotedKeyspace = "\"MyKeyspace\"";
        layer.maybeQuotedTable = "\"MyTable\"";
        layer.sidecar = mock(SidecarClient.class);
        RingEntry entry = mock(RingEntry.class);
        when(entry.fqdn()).thenReturn("localhost");
        RingResponse ring = new RingResponse();
        ring.add(entry);
        TableStatsResponse stats = mock(TableStatsResponse.class);
        when(stats.totalDiskSpaceUsedBytes()).thenReturn(1024L);
        when(layer.sidecar.tableStats(any(SidecarInstance.class), eq("\"MyKeyspace\""), eq("\"MyTable\"")))
            .thenReturn(CompletableFuture.completedFuture(stats));

        assertThat(layer.getSizing(CompletableFuture.completedFuture(ring),
                                   ReplicationFactor.simpleStrategy(1), clientConfig).getEffectiveNumberOfCores())
        .isEqualTo(1);
        verify(layer.sidecar).tableStats(any(SidecarInstance.class), eq("\"MyKeyspace\""), eq("\"MyTable\""));
    }

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
}
