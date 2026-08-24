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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCassandraClusterInfo;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;

import static org.apache.cassandra.spark.TestUtils.range;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
    void testCoordinatedClusterInfoUsesSidecarClientContactPointsForTimeSkew()
    {
        Instant localNow = Instant.now();
        int allowanceMinutes = 10;

        SidecarClient sidecarClient = mock(SidecarClient.class);
        CassandraContext ctx = mock(CassandraContext.class);
        when(ctx.getSidecarClient()).thenReturn(sidecarClient);
        TimeSkewResponse tsr = new TimeSkewResponse(localNow.toEpochMilli(), allowanceMinutes);
        when(sidecarClient.timeSkew()).thenReturn(CompletableFuture.completedFuture(tsr));

        CassandraClusterInfo ci = new CoordinatedCassandraClusterInfo((BulkSparkConf) null, null)
        {
            @Override
            protected CassandraContext buildCassandraContext()
            {
                return ctx;
            }

            @Override
            public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
            {
                return TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("dc1", 3), 5);
            }
        };

        assertThatNoException()
        .describedAs("Load-balanced cluster info must use timeSkew() so load balancer contact points stay reachable")
        .isThrownBy(() -> ci.validateTimeSkewWithLocalNow(range(10, 20), localNow));
        verify(sidecarClient).timeSkew();
        verify(sidecarClient, never()).timeSkew(anyList());
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
