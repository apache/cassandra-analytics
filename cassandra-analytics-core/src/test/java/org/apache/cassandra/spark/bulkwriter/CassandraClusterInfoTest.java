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
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
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

    static Stream<Arguments> sidecarResponseDelays()
    {
        return Stream.of(
        Arguments.of((Object) new int[] {100, 200, 300}), // all responses within deadline
        Arguments.of((Object) new int[] {500, 3000}) // single timeout
        );
    }

    @ParameterizedTest
    @MethodSource("sidecarResponseDelays")
    @Timeout(value = 2300, unit = TimeUnit.MILLISECONDS) // set timeout slightly higher than deadline of (1000 + 100) * 2
    void testSuccessfulGetAllNodeSettings(int[] responseDelayMillis)
    {
        BulkSparkConf conf = mockBulkSparkWithSidecarConf(1, 100, 2);
        try (CassandraClusterInfo ci = new MockClusterInfoForNodeSettings(conf, responseDelayMillis))
        {
            assertThatNoException()
            .describedAs("Accept when at least one node responds within total timeout")
            .isThrownBy(ci::getAllNodeSettings);
        }
    }

    @Test
    void testTimeoutGetAllNodeSettings()
    {
        BulkSparkConf conf = mockBulkSparkWithSidecarConf(1, 100, 2);
        try (CassandraClusterInfo ci = new MockClusterInfoForNodeSettings(conf, 3000, 3300))
        {
            assertThatThrownBy(ci::getAllNodeSettings)
            .describedAs("Raise error when no responses received within timeout")
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("Unable to determine the node settings. 0/2 instances available.");
        }
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
    void testSidecarInstanceIdsByHostnameEmptyWhenNoneConfigured()
    {
        Set<SidecarInstance> contactPoints = new HashSet<>(Arrays.asList(
        SidecarInstanceFactory.createFromString("cassandra1:9043", 9043),
        SidecarInstanceFactory.createFromString("cassandra2:9043", 9043)));

        assertThat(CassandraClusterInfo.sidecarInstanceIdsByHostname(contactPoints)).isEmpty();
    }

    @Test
    void testSidecarInstanceIdsByHostnameThrowsWhenSharedHostnameHasDifferentIds()
    {
        // Reproduces a Sidecar deployment shared/load-balanced across multiple Cassandra instances: they are
        // only reachable through the same address (e.g. one LB VIP), each meant to carry its own id. The
        // current hostname-keyed lookup cannot represent this - it can only associate a single id per hostname -
        // so instead of silently picking one id, it fails fast while building the lookup.
        Set<SidecarInstance> contactPoints = new HashSet<>(Arrays.asList(
        SidecarInstanceFactory.createFromString("sidecar-lb:9043=1"),
        SidecarInstanceFactory.createFromString("sidecar-lb:9043=2"),
        SidecarInstanceFactory.createFromString("sidecar-lb:9043=3")));

        assertThatThrownBy(() -> CassandraClusterInfo.sidecarInstanceIdsByHostname(contactPoints))
        .describedAs("a single shared Sidecar endpoint fronting multiple instances (e.g. behind a load balancer) "
                    + "cannot be expressed by a hostname->id map keyed on hostname alone")
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate key");
    }

    @Test
    void testSidecarInstanceIdsByHostnameMissesWhenAddressFormatDiffersFromRingFqdn()
    {
        // The lookup is keyed on the literal contact-point address string. If the ring later reports this same
        // physical instance under a different string (e.g. its fqdn, when the contact point was configured by
        // IP), the two never match: getTokenRangeReplicasFromSidecar's instanceIdsByHostname.get(metadata.fqdn())
        // misses, and the configured id is silently dropped for that instance - not an exception, just a null.
        Set<SidecarInstance> contactPoints = Collections.singleton(
        SidecarInstanceFactory.createFromString("10.0.0.5:9043=1"));

        Map<String, Integer> byHostname = CassandraClusterInfo.sidecarInstanceIdsByHostname(contactPoints);

        assertThat(byHostname).containsEntry("10.0.0.5", 1);
        assertThat(byHostname)
        .describedAs("the ring would report this same instance by its fqdn, not its IP - the lookup key must "
                    + "match exactly, so the configured id is invisible under the fqdn key")
        .doesNotContainKey("node1.example.com");
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

    private BulkSparkConf mockBulkSparkWithSidecarConf(int requestTimeoutSeconds, long maxRetryDelayMillis, int retryCount)
    {
        BulkSparkConf conf = mock(BulkSparkConf.class);
        when(conf.getSidecarRequestTimeoutSeconds()).thenReturn(requestTimeoutSeconds);
        when(conf.getSidecarRequestMaxRetryDelayMillis()).thenReturn(maxRetryDelayMillis);
        when(conf.getSidecarRequestRetries()).thenReturn(retryCount);
        return conf;
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

    private static class MockClusterInfoForNodeSettings extends CassandraClusterInfo
    {
        MockClusterInfoForNodeSettings(BulkSparkConf conf, int... responseDelayMillis)
        {
            super(conf);

            allNodeSettingFutures.clear();
            List<CompletableFuture<NodeSettings>> futures = new ArrayList<>(responseDelayMillis.length);
            for (int delay : responseDelayMillis)
            {
                CompletableFuture<NodeSettings> future = CompletableFuture.supplyAsync(() -> {
                    Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
                    return mock(NodeSettings.class);
                });
                futures.add(future);
            }
            allNodeSettingFutures.addAll(futures);
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            return mock(CassandraContext.class);
        }
    }
}
