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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.SidecarInstanceFactory;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.Nullable;

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
    void testValidateSidecarInstanceIdCoverageThrowsWhenPartiallyConfigured()
    {
        CassandraClusterInfo ci = noOpClusterInfoWithGlobalInstanceId(1);
        Set<RingInstance> instances = new HashSet<>();
        instances.add(ringInstance("dc1-i0", 1));
        instances.add(ringInstance("dc1-i1", null));
        instances.add(ringInstance("dc1-i2", null));

        assertThatThrownBy(() -> ci.validateSidecarInstanceIdCoverage(instances))
        .describedAs("2/3 instances would fall back to the global id and collide on whichever instance it identifies")
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining(BulkSparkConf.SIDECAR_INSTANCE_ID + "=1")
        .hasMessageContaining("dc1-i1")
        .hasMessageContaining("dc1-i2");
    }

    @Test
    void testValidateSidecarInstanceIdCoverageAllowsFullCoverage()
    {
        CassandraClusterInfo ci = noOpClusterInfoWithGlobalInstanceId(1);
        Set<RingInstance> instances = new HashSet<>();
        instances.add(ringInstance("dc1-i0", 1));
        instances.add(ringInstance("dc1-i1", 2));
        instances.add(ringInstance("dc1-i2", 3));

        assertThatNoException()
        .describedAs("every instance resolves its own id, so the global id is never actually used")
        .isThrownBy(() -> ci.validateSidecarInstanceIdCoverage(instances));
    }

    @Test
    void testValidateSidecarInstanceIdCoverageNoopForSingleInstance()
    {
        CassandraClusterInfo ci = noOpClusterInfoWithGlobalInstanceId(1);
        Set<RingInstance> instances = Collections.singleton(ringInstance("dc1-i0", null));

        assertThatNoException()
        .describedAs("a single instance is unambiguous: the global id can only apply to it")
        .isThrownBy(() -> ci.validateSidecarInstanceIdCoverage(instances));
    }

    @Test
    void testValidateSidecarInstanceIdCoverageNoopWhenGlobalIdUnset()
    {
        CassandraClusterInfo ci = noOpClusterInfoWithGlobalInstanceId(null);
        Set<RingInstance> instances = new HashSet<>();
        instances.add(ringInstance("dc1-i0", null));
        instances.add(ringInstance("dc1-i1", null));

        assertThatNoException().isThrownBy(() -> ci.validateSidecarInstanceIdCoverage(instances));
    }

    @Test
    void testSidecarInstanceIdsByHostnameFromPlainContactPoints()
    {
        Set<SidecarInstance> contactPoints = new HashSet<>(Arrays.asList(
        SidecarInstanceFactory.createFromString("cassandra1:9043=1", 9043),
        SidecarInstanceFactory.createFromString("cassandra2:9043=2", 9043),
        SidecarInstanceFactory.createFromString("cassandra3:9043", 9043)));

        Map<String, Integer> byHostname = CassandraClusterInfo.sidecarInstanceIdsByHostname(contactPoints);

        assertThat(byHostname).containsEntry("cassandra1", 1).containsEntry("cassandra2", 2);
        assertThat(byHostname)
        .describedAs("contact point with no '=<id>' suffix has no entry")
        .doesNotContainKey("cassandra3");
    }

    @Test
    void testSidecarInstanceIdsByHostnameFromCoordinatedWriteContactPoints()
    {
        // Coordinated-write contact points are parsed the same way (SimpleClusterConf.buildSidecarContactPoints
        // also delegates to SidecarInstanceFactory.createFromString), so per-instance ids must resolve here too -
        // this is the exact path that regressed when id lookup was previously sourced from
        // conf.sidecarContactPoints() instead of the cluster's actually-resolved contact points.
        Set<SidecarInstance> coordinatedContactPoints = new HashSet<>(Arrays.asList(
        SidecarInstanceFactory.createFromString("172.20.39.166:9043=1"),
        SidecarInstanceFactory.createFromString("172.20.39.97:9043=2"),
        SidecarInstanceFactory.createFromString("172.20.39.216:9043=3")));

        Map<String, Integer> byHostname = CassandraClusterInfo.sidecarInstanceIdsByHostname(coordinatedContactPoints);

        assertThat(byHostname).containsEntry("172.20.39.166", 1)
                              .containsEntry("172.20.39.97", 2)
                              .containsEntry("172.20.39.216", 3);
    }

    @Test
    void testSidecarInstanceIdsByHostnameEmptyWhenNoneConfigured()
    {
        Set<SidecarInstance> contactPoints = new HashSet<>(Arrays.asList(
        SidecarInstanceFactory.createFromString("cassandra1:9043", 9043),
        SidecarInstanceFactory.createFromString("cassandra2:9043", 9043)));

        assertThat(CassandraClusterInfo.sidecarInstanceIdsByHostname(contactPoints)).isEmpty();
    }

    private static RingInstance ringInstance(String fqdn, @Nullable Integer sidecarInstanceId)
    {
        return new RingInstance(new RingEntry.Builder()
                                .datacenter("dc1")
                                .address(fqdn)
                                .port(7000)
                                .status("UP")
                                .state("NORMAL")
                                .token("0")
                                .fqdn(fqdn)
                                .rack("rack")
                                .owns("")
                                .load("")
                                .hostId("")
                                .build(), null, sidecarInstanceId);
    }

    private static CassandraClusterInfo noOpClusterInfoWithGlobalInstanceId(@Nullable Integer globalInstanceId)
    {
        Map<String, String> options = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        options.put(WriterOptions.SIDECAR_CONTACT_POINTS.name(), "127.0.0.1");
        options.put(WriterOptions.KEYSPACE.name(), "ks");
        options.put(WriterOptions.TABLE.name(), "table");
        options.put(WriterOptions.KEYSTORE_PASSWORD.name(), "dummy_password");
        options.put(WriterOptions.KEYSTORE_BASE64_ENCODED.name(), "ZHVtbXk=");

        SparkConf sparkConf = new SparkConf();
        if (globalInstanceId != null)
        {
            sparkConf.set(BulkSparkConf.SIDECAR_INSTANCE_ID, globalInstanceId.toString());
        }
        return new NoOpClusterInfo(new BulkSparkConf(sparkConf, options));
    }

    private static class NoOpClusterInfo extends CassandraClusterInfo
    {
        NoOpClusterInfo(BulkSparkConf conf)
        {
            super(conf);
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            CassandraContext context = mock(CassandraContext.class, RETURNS_DEEP_STUBS);
            when(context.getCluster()).thenReturn(Collections.emptySet());
            return context;
        }
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
}
