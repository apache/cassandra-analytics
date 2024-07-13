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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CassandraDataLayerTests
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS = ImmutableMap.of(
    "keyspace", "big-data",
    "table", "customers",
    "sidecar_instances", "localhost");

    @Test
    void testDefaultClearSnapshotStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        ClientConfig clientConfig = ClientConfig.create(options);
        assertEquals("big-data", clientConfig.keyspace());
        assertEquals("customers", clientConfig.table());
        assertEquals("localhost", clientConfig.sidecarContactPoints());
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = clientConfig.clearSnapshotStrategy();
        assertTrue(clearSnapshotStrategy.shouldClearOnCompletion());
        assertEquals("2d", clearSnapshotStrategy.ttl());
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
