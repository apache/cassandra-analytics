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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.CassandraClusterInfo;
import org.apache.cassandra.spark.bulkwriter.CassandraClusterInfoTest;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.bulkwriter.WriteAvailability;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;

import static org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup.fromBulkSparkConf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class CassandraClusterInfoGroupTest
{
    @Test
    void testLookupCluster()
    {
        CassandraClusterInfoGroup group = mockClusterGroup(2, index -> mockClusterInfo("cluster" + index));
        assertThat(group.size()).isEqualTo(2);
        group.forEach((clusterId, clusterInfo) -> {
            assertThat(group.getValueOrNull(clusterId)).isSameAs(clusterInfo).isNotNull();
        });
        assertThat(group.getValueOrNull("cluster2")).isNull();
    }

    @Test
    void testClusterId()
    {
        CassandraClusterInfoGroup group = mockClusterGroup(2, index -> mockClusterInfo("cluster" + index));
        assertThat(group.clusterId()).isEqualTo("ClusterInfoGroup: [cluster0, cluster1]");

        group = mockClusterGroup(1, index -> mockClusterInfo("cluster" + index));
        assertThat(group.clusterId()).isEqualTo("ClusterInfoGroup: [cluster0]");
    }

    @Test
    void testDelegationOfSingleCluster()
    {
        CassandraClusterInfo clusterInfo = mockClusterInfo("cluster0");
        TokenRangeReplicasResponse response = TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(10, 3);
        TokenRangeMapping<RingInstance> expectedTokenRangeMapping = TokenRangeMapping.create(() -> response,
                                                                                             () -> Partitioner.Murmur3Partitioner,
                                                                                             RingInstance::new);
        when(clusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(expectedTokenRangeMapping);
        when(clusterInfo.getLowestCassandraVersion()).thenReturn("lowestCassandraVersion");
        when(clusterInfo.clusterWriteAvailability()).thenReturn(Collections.emptyMap());
        CassandraClusterInfoGroup group = mockClusterGroup(1, index -> clusterInfo);
        // Since there is a single clusterInfo in the group. It behaves as a simple delegation to the sole clusterInfo
        assertThat(group.clusterWriteAvailability()).isSameAs(clusterInfo.clusterWriteAvailability());
        assertThat(group.getLowestCassandraVersion()).isSameAs(clusterInfo.getLowestCassandraVersion());
        assertThat(group.getTokenRangeMapping(true)).isSameAs(clusterInfo.getTokenRangeMapping(true));
    }

    @Test
    void testAggregatePartitioner()
    {
        CassandraClusterInfoGroup invalidGroup = mockClusterGroup(2, index -> {
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            Partitioner partitioner = (index % 2 == 0) ? Partitioner.Murmur3Partitioner : Partitioner.RandomPartitioner;
            when(clusterInfo.getPartitioner()).thenReturn(partitioner);
            return clusterInfo;
        });
        assertThatThrownBy(invalidGroup::getPartitioner)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Clusters are not running with the same partitioner kind. Found partitioners: " +
                    "{cluster0=org.apache.cassandra.dht.Murmur3Partitioner, cluster1=org.apache.cassandra.dht.RandomPartitioner}");

        CassandraClusterInfoGroup goodGroup = mockClusterGroup(2, index -> {
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            when(clusterInfo.getPartitioner()).thenReturn(Partitioner.Murmur3Partitioner);
            return clusterInfo;
        });
        assertThat(goodGroup.getPartitioner()).isEqualTo(Partitioner.Murmur3Partitioner);
    }

    @Test
    void testAggregateWriteAvailability()
    {
        Map<RingInstance, WriteAvailability> cluster1Availability = ImmutableMap.of(mock(RingInstance.class), WriteAvailability.AVAILABLE);
        Map<RingInstance, WriteAvailability> cluster2Availability = ImmutableMap.of(mock(RingInstance.class), WriteAvailability.UNAVAILABLE_DOWN);
        CassandraClusterInfoGroup group = mockClusterGroup(2, index -> {
            Map<RingInstance, WriteAvailability> availability = (index % 2 == 0) ? cluster1Availability : cluster2Availability;
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            when(clusterInfo.clusterWriteAvailability()).thenReturn(availability);
            return clusterInfo;
        });
        assertThat(group.clusterWriteAvailability())
        .describedAs("clusterWriteAvailability retrieved from group contains entries from both clusters")
        .hasSize(2)
        .containsValues(WriteAvailability.AVAILABLE, WriteAvailability.UNAVAILABLE_DOWN);
    }

    @Test
    void testAggregateLowestCassandraVersion()
    {
        CassandraClusterInfoGroup goodGroup = mockClusterGroup(2, index -> {
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            when(clusterInfo.getLowestCassandraVersion()).thenReturn("4.0." + index);
            return clusterInfo;
        });
        assertThat(goodGroup.getLowestCassandraVersion()).isEqualTo("4.0.0");
    }

    @Test
    void testAggregateLowestCassandraVersionFailDueToDifference()
    {
        CassandraClusterInfoGroup badGroup = mockClusterGroup(2, index -> {
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            when(clusterInfo.getLowestCassandraVersion()).thenReturn((4 + index) + ".0.0");
            return clusterInfo;
        });
        assertThatThrownBy(badGroup::getLowestCassandraVersion)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cluster versions are not compatible. lowest=4.0.0 and highest=5.0.0");
    }

    @Test
    void testCheckBulkWriterIsEnabledOrThrow()
    {
        for (int i = 0; i < 2; i++)
        {
            int notEnabledClusterIndex = i;
            CassandraClusterInfoGroup badGroup = mockClusterGroup(2, index -> {
                CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
                if (index == notEnabledClusterIndex)
                {
                    doThrow(new RuntimeException("not enabled")).when(clusterInfo).checkBulkWriterIsEnabledOrThrow();
                }
                return clusterInfo;
            });

            assertThatThrownBy(badGroup::checkBulkWriterIsEnabledOrThrow)
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("Failed to perform action on cluster: cluster" + notEnabledClusterIndex)
            .hasRootCauseMessage("not enabled");
        }
    }

    @Test
    void testAggregateTokenRangeMapping()
    {
        TokenRangeMapping<RingInstance> topology1 = TokenRangeMappingUtils.buildTokenRangeMapping(0,
                                                                                                  ImmutableMap.of("dc1", 3),
                                                                                                  5);
        TokenRangeMapping<RingInstance> topology2 = TokenRangeMappingUtils.buildTokenRangeMapping(10,
                                                                                                  ImmutableMap.of("dc1", 3),
                                                                                                  8);
        CassandraClusterInfoGroup group = mockClusterGroup(2, index -> {
            TokenRangeMapping<RingInstance> topology = index == 0 ? topology1 : topology2;
            CassandraClusterInfo clusterInfo = mockClusterInfo("cluster" + index);
            when(clusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(topology);
            return clusterInfo;
        });
        TokenRangeMapping<RingInstance> topology = group.getTokenRangeMapping(false);
        TokenRangeMapping<RingInstance> expected = TokenRangeMapping.consolidate(Arrays.asList(topology1, topology2));
        assertThat(topology).isEqualTo(expected);
        TokenRangeMapping<RingInstance> cachedTopology = group.getTokenRangeMapping(true);
        assertThat(cachedTopology).isSameAs(topology);
    }

    @Test
    void testTimeSkewTooLarge()
    {
        for (int i = 0; i < 2; i++)
        {
            int clusterIndexWithLargeTimeSkew = i;
            Instant remoteNow = Instant.ofEpochMilli(1726604289530L); // time in the past that guarantees to exceed the skew allowance
            CassandraClusterInfoGroup group = mockClusterGroup(2, index -> {
                if (index == clusterIndexWithLargeTimeSkew)
                {
                    CassandraClusterInfo ci = spy(CassandraClusterInfoTest.mockClusterInfoForTimeSkewTest(10, remoteNow));
                    when(ci.clusterId()).thenReturn("cluster" + index);
                    return ci;
                }
                else
                {
                    return mockClusterInfo("cluster" + index);
                }
            });

            assertThatThrownBy(() -> group.validateTimeSkew(Range.openClosed(BigInteger.valueOf(10), BigInteger.valueOf(20))))
            .isExactlyInstanceOf(TimeSkewTooLargeException.class)
            .hasMessageContaining("Time skew between Spark and Cassandra is too large. ")
            .hasMessageContaining("allowableSkewInMinutes=10, ")
            .hasMessageContaining("remoteCassandraTime=2024-09-17T20:18:09.530Z, ")
            .hasMessageContaining("clusterId=cluster" + clusterIndexWithLargeTimeSkew);
        }

    }

    @Test
    void testCreateClusterInfoListFailsDueToAbsentConfiguration()
    {
        BulkSparkConf conf = mock(BulkSparkConf.class);
        assertThatThrownBy(() -> fromBulkSparkConf(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("In order to create an instance of CassandraCoordinatedBulkWriterContext, " +
                    "you must provide the appropriate coordinated write configuration by " +
                    "setting the `COORDINATED_WRITE_CONFIG` writer option.");
    }

    @Test
    void testCreateClusterInfoListFailsDueToEmptyConfiguration()
    {
        BulkSparkConf conf = mock(BulkSparkConf.class, RETURNS_DEEP_STUBS);
        when(conf.coordinatedWriteConf().clusters()).thenReturn(Collections.emptyMap());
        assertThatThrownBy(() -> fromBulkSparkConf(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No cluster info is built from");
    }

    @Test
    void testCreateClusterInfoListFailsDueToEmptyClusterId()
    {
        BulkSparkConf conf = mock(BulkSparkConf.class);
        CoordinatedWriteConf.ClusterConf clusterConf = new CoordinatedWriteConf.SimpleClusterConf(Collections.singletonList("localhost:9043"), "localDc");
        when(conf.coordinatedWriteConf()).thenReturn(new CoordinatedWriteConf(Collections.singletonMap("", clusterConf)));
        assertThatThrownBy(() -> fromBulkSparkConf(conf))
        .isInstanceOf(IllegalStateException.class)
        .describedAs("The exception message should include the original json to help spot the wrong configuration (empty clusterId)")
        .hasMessage("Found coordinatedWriteConf with empty or null clusterId. " +
                    "CoordinatedWriteConf{json={\"\":{\"sidecarContactPoints\":[\"localhost:9043\"],\"localDc\":\"localDc\"}}}");
    }

    private CassandraClusterInfoGroup mockClusterGroup(int size,
                                                       Function<Integer, CassandraClusterInfo> clusterInfoCreator)
    {
        List<ClusterInfo> clusterInfos = IntStream.range(0, size).boxed().map(clusterInfoCreator).collect(Collectors.toList());
        return CassandraClusterInfoGroup.createFrom(clusterInfos);
    }

    private CassandraClusterInfo mockClusterInfo(String clusterId)
    {
        CassandraClusterInfo clusterInfo = mock(CassandraClusterInfo.class);
        when(clusterInfo.clusterId()).thenReturn(clusterId);
        return clusterInfo;
    }
}
