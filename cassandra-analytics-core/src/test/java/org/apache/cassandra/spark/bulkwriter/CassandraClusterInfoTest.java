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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.apache.cassandra.spark.utils.CqlUtils;

import static org.apache.cassandra.spark.TestUtils.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraClusterInfoTest
{
    @Test
    void testTimeSkewAcceptable()
    {
        Instant localNow = Instant.now();
        int allowanceMinutes = 10;
        Instant remoteNow = localNow.plus(Duration.ofMinutes(1));
        CassandraClusterInfo ci = mockClusterInfoForTimeSkewTest(allowanceMinutes, remoteNow);
        assertThatNoException()
        .describedAs("Acceptable time skew should validate without exception")
        .isThrownBy(() -> ci.validateTimeSkewWithLocalNow(range(10, 20), localNow));
    }

    @Test
    void testTimeSkewTooLarge()
    {
        Instant localNow = Instant.ofEpochMilli(1726604289530L);
        int allowanceMinutes = 10;
        Instant remoteNow = localNow.plus(Duration.ofMinutes(11)); // 11 > allowanceMinutes
        CassandraClusterInfo ci = mockClusterInfoForTimeSkewTest(allowanceMinutes, remoteNow);
        assertThatThrownBy(() -> ci.validateTimeSkewWithLocalNow(range(10, 20), localNow))
        .describedAs("Time skew with too large a value should throw TimeSkewTooLargeException")
        .isExactlyInstanceOf(TimeSkewTooLargeException.class)
        .hasMessage("Time skew between Spark and Cassandra is too large. " +
                    "allowableSkewInMinutes=10, " +
                    "localTime=2024-09-17T20:18:09.530Z, " +
                    "remoteCassandraTime=2024-09-17T20:29:09.530Z, " +
                    "clusterId=null");
    }

    public static CassandraClusterInfo mockClusterInfoForTimeSkewTest(int allowanceMinutes, Instant remoteNow)
    {
        return new MockClusterInfoForTimeSkew(allowanceMinutes, remoteNow);
    }

    @Test
    void testGetTrackedReplicationType()
    {
        String schema = "CREATE KEYSPACE mykeyspace "
                        + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}"
                        + "  AND durable_writes = true"
                        + "  AND replication_type = 'tracked';";
        CassandraClusterInfo ci = mockClusterInfoWithSchema("mykeyspace", schema);
        assertThat(ci.getReplicationType())
        .describedAs("Keyspace with replication_type = 'tracked' should return 'tracked'")
        .isEqualTo("tracked");
    }

    @Test
    void testGetUntrackedReplicationType()
    {
        String schema = "CREATE KEYSPACE k "
                        + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}"
                        + "  AND durable_writes = true"
                        + "  AND replication_type = 'untracked';";
        CassandraClusterInfo ci = mockClusterInfoWithSchema("k", schema);
        assertThat(ci.getReplicationType())
        .describedAs("Keyspace with replication_type = 'untracked' should return 'untracked'")
        .isEqualTo("untracked");
    }

    @Test
    void testGetReplicationTypeReturnsNullWhenPropertyAbsent()
    {
        // Schema without replication_type — pre-mutation-tracking clusters
        String schema = "CREATE KEYSPACE k "
                        + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}"
                        + "  AND durable_writes = true;";
        CassandraClusterInfo ci = mockClusterInfoWithSchema("k", schema);
        assertThat(ci.getReplicationType())
        .describedAs("Keyspace without replication_type property should return null")
        .isNull();
    }

    private static CassandraClusterInfo mockClusterInfoWithSchema(String keyspace, String schemaStr)
    {
        return new MockClusterInfoForSchema(keyspace, schemaStr);
    }

    private static class MockClusterInfoForTimeSkew extends CassandraClusterInfo
    {
        private CassandraContext cassandraContext;

        MockClusterInfoForTimeSkew(int allowanceMinutes, Instant remoteNow)
        {
            super((BulkSparkConf) null);
            mockCassandraContext(allowanceMinutes, remoteNow);
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            this.cassandraContext = mock(CassandraContext.class, RETURNS_DEEP_STUBS);
            return cassandraContext;
        }

        @Override
        public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
        {
            return TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("dc1", 3), 5);
        }

        private void mockCassandraContext(int allowanceMinutes, Instant remoteNow)
        {
            when(cassandraContext.getCluster()).thenReturn(Collections.emptySet());
            TimeSkewResponse tsr = new TimeSkewResponse(remoteNow.toEpochMilli(), allowanceMinutes);
            when(cassandraContext.getSidecarClient().timeSkew(any()))
            .thenReturn(CompletableFuture.completedFuture(tsr));
            when(cassandraContext.sidecarPort()).thenReturn(9043);
        }
    }

    /**
     * Minimal ClusterInfo stub that returns a fixed keyspace schema string, used to test
     * {@link CassandraClusterInfo#getReplicationType()}.
     */
    private static class MockClusterInfoForSchema extends CassandraClusterInfo
    {
        private final String keyspaceName;
        private final String schemaStr;

        MockClusterInfoForSchema(String keyspace, String schemaStr)
        {
            super((BulkSparkConf) null);
            this.keyspaceName = keyspace;
            this.schemaStr = schemaStr;
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            return mock(CassandraContext.class, RETURNS_DEEP_STUBS);
        }

        @Override
        public String getKeyspaceSchema(boolean cached)
        {
            return schemaStr;
        }

        @Override
        public String getReplicationType()
        {
            return CqlUtils.extractReplicationType(schemaStr, keyspaceName);
        }
    }
}
