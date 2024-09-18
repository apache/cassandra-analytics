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

package org.apache.cassandra.spark.bulkwriter.token;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.common.model.NodeState;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenRangeMappingTest
{
    @Test
    void testCreateTokenRangeMapping()
    {
        TokenRangeMapping<RingInstance> topology = createTestMapping(10);
        assertThat(topology.partitioner()).isEqualTo(Partitioner.Murmur3Partitioner);
    }

    @Test
    void testTokenRangeMappingEqualsAndHashcode()
    {
        TokenRangeMapping<RingInstance> topology1 = createTestMapping(10);
        TokenRangeMapping<RingInstance> topology2 = createTestMapping(10);
        TokenRangeMapping<RingInstance> topology3 = createTestMapping(5);

        assertThat(topology1).isEqualTo(topology2);
        assertThat(topology1.hashCode()).isEqualTo(topology2.hashCode());
        assertThat(topology1).isNotEqualTo(topology3);
        assertThat(topology1.hashCode()).isNotEqualTo(topology3.hashCode());
    }

    @Test
    void testCreateTokenRangeMappingFailDueToIllegalTokenRangeReplicasResponse()
    {
        TokenRangeReplicasResponse invalidResponse = TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(10, 3);
        String invalidInstance = "127.0.3.1:9042";
        invalidResponse.writeReplicas().forEach(replicaInfo -> {
            // add instance (ip:port) that is not part of the cluster
            replicaInfo.replicasByDatacenter().put("dc3", Collections.singletonList(invalidInstance));
        });
        assertThatThrownBy(() -> TokenRangeMapping.create(() -> invalidResponse,
                                                          () -> Partitioner.Murmur3Partitioner,
                                                          RingInstance::new))
        .isExactlyInstanceOf(RuntimeException.class)
        .hasMessage("No metadata found for instance: " + invalidInstance);
    }

    @Test
    void testCreateTokenRangeMappingWithPending()
    {
        TokenRangeReplicasResponse response = TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(10, 3);
        int i = 0;
        int pendingCount = 0;
        for (NodeState state : NodeState.values())
        {
            String key = "localhost" + i + ":9042";
            i++;
            ReplicaMetadata metadata = response.replicaMetadata().get(key);
            ReplicaMetadata updatedMetadata = new ReplicaMetadata(state.name(),
                                                                  metadata.status(),
                                                                  metadata.fqdn(),
                                                                  metadata.address(),
                                                                  metadata.port(),
                                                                  metadata.datacenter());
            response.replicaMetadata().put(key, updatedMetadata);
            if (state.isPending)
            {
                pendingCount++;
            }
        }
        TokenRangeMapping<RingInstance> topology = TokenRangeMapping.create(() -> response,
                                                                            () -> Partitioner.Murmur3Partitioner,
                                                                            RingInstance::new);
        assertThat(topology.pendingInstances()).hasSize(pendingCount);
    }

    @Test
    void testConsolidateListOfTokenRangeMappingFails()
    {
        assertThatThrownBy(() -> TokenRangeMapping.consolidate(Collections.emptyList()))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot consolidate TokenRangeMapping from none");

        TokenRangeMapping<RingInstance> topologyWithMurmur3 = createTestMapping(5, Partitioner.Murmur3Partitioner);
        TokenRangeMapping<RingInstance> topologyWithRandom = createTestMapping(5, Partitioner.RandomPartitioner);
        assertThatThrownBy(() -> TokenRangeMapping.consolidate(Arrays.asList(topologyWithMurmur3, topologyWithRandom)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Multiple Partitioners found: ")
        .hasMessageContaining(Partitioner.Murmur3Partitioner.toString())
        .hasMessageContaining(Partitioner.RandomPartitioner.toString());
    }

    @Test
    void testConsolidateSingleTokenRangeMapping()
    {
        // Consolidate a single TokenRangeMapping
        TokenRangeMapping<RingInstance> topology = createTestMapping(5);
        TokenRangeMapping<RingInstance> consolidated = TokenRangeMapping.consolidate(Collections.singletonList(topology));
        assertThat(consolidated).isSameAs(topology);
    }

    @Test
    void testConsolidateFromTokenRangeMappingsWithSameRingLayout()
    {
        TokenRangeMapping<RingInstance> topology1 = createTestMapping(0, 5, Partitioner.Murmur3Partitioner, "cluster1");
        TokenRangeMapping<RingInstance> topology2 = createTestMapping(0, 5, Partitioner.Murmur3Partitioner, "cluster2");
        TokenRangeMapping<RingInstance> consolidated = TokenRangeMapping.consolidate(Arrays.asList(topology1, topology2));
        assertThat(consolidated.allInstances())
        .describedAs("Consolidated allInstances contains instances from both topologies")
        .hasSize(10)
        .containsAll(topology1.allInstances())
        .containsAll(topology2.allInstances());
        assertThat(consolidated.pendingInstances()).isEmpty();
        assertThat(consolidated.getTokenRanges().asMap())
        .describedAs("Consolidated TokenRanges contains instances and ranges from both topologies")
        .hasSize(10)
        .containsAllEntriesOf(topology1.getTokenRanges().asMap())
        .containsAllEntriesOf(topology2.getTokenRanges().asMap());
        assertThat(consolidated.getRangeMap().asMapOfRanges())
        .describedAs("Consolidated RangeMap has the same key set (ranges) as both topologies because the range layouts are the same")
        .hasSize(7)
        .containsOnlyKeys(topology1.getRangeMap().asMapOfRanges().keySet())
        .containsOnlyKeys(topology2.getRangeMap().asMapOfRanges().keySet());
        // the instances per range in the consolidated should contain the instances of the same range from both topologies
        consolidated.getRangeMap().asMapOfRanges().forEach((range, instances) -> {
            if (instances.isEmpty())
            {
                return;
            }
            assertThat(instances)
            .containsAll(topology1.getRangeMap().asMapOfRanges().get(range))
            .containsAll(topology2.getRangeMap().asMapOfRanges().get(range));
        });
    }

    @Test
    void testConsolidateFromTokenRangeMappingsWithDistinctRingLayout()
    {
        TokenRangeMapping<RingInstance> topology1 = createTestMapping(0, 5, Partitioner.Murmur3Partitioner, "cluster1");
        TokenRangeMapping<RingInstance> topology2 = createTestMapping(1, 5, Partitioner.Murmur3Partitioner, "cluster2");
        TokenRangeMapping<RingInstance> consolidated = TokenRangeMapping.consolidate(Arrays.asList(topology1, topology2));
        assertThat(consolidated.allInstances())
        .describedAs("Consolidated allInstances contains instances from both topologies")
        .hasSize(10)
        .containsAll(topology1.allInstances())
        .containsAll(topology2.allInstances());
        assertThat(consolidated.pendingInstances()).isEmpty();
        assertThat(consolidated.getTokenRanges().asMap())
        .describedAs("Consolidated TokenRanges contains instances and ranges from both topologies")
        .hasSize(10)
        .containsAllEntriesOf(topology1.getTokenRanges().asMap())
        .containsAllEntriesOf(topology2.getTokenRanges().asMap());
        List<Range<BigInteger>> expectedConsolidatedRanges = Arrays.asList(
        range(Long.MIN_VALUE, 0),
        range(0, 1), range(1, 100),
        range(100, 101), range(101, 200),
        range(200, 201), range(201, 300),
        range(300, 301), range(301, 400),
        range(400, 401), range(401, 500),
        range(500, 501), range(501, Long.MAX_VALUE));
        assertThat(consolidated.getRangeMap().asMapOfRanges())
        .describedAs("Consolidated RangeMap has different key set (ranges) from both topologies because the range layouts are different")
        .hasSize(topology1.getRangeMap().asMapOfRanges().size() + topology2.getRangeMap().asMapOfRanges().size() - 1)
        .containsOnlyKeys(expectedConsolidatedRanges);
        // the instances per range in the consolidated should contain the instances of the same range from both topologies
        expectedConsolidatedRanges.forEach(range -> {
            List<RingInstance> instances = consolidated.getRangeMap().asMapOfRanges().get(range);
            if (instances.isEmpty())
            {
                return;
            }
            List<RingInstance> instancesOfCluster1 = getSoleValue(topology1.getRangeMap().subRangeMap(range).asMapOfRanges());
            List<RingInstance> instancesOfCluster2 = getSoleValue(topology2.getRangeMap().subRangeMap(range).asMapOfRanges());
            assertThat(instances)
            .hasSize(instancesOfCluster1.size() + instancesOfCluster2.size())
            .containsAll(instancesOfCluster1)
            .containsAll(instancesOfCluster2);
        });
    }

    private TokenRangeMapping<RingInstance> createTestMapping(int instanceCount)
    {
        return createTestMapping(instanceCount, Partitioner.Murmur3Partitioner);
    }

    private TokenRangeMapping<RingInstance> createTestMapping(int instanceCount, Partitioner partitioner)
    {
        return createTestMapping(0L, instanceCount, partitioner, null);
    }

    private TokenRangeMapping<RingInstance> createTestMapping(long startToken, int instanceCount, Partitioner partitioner, String clusterId)
    {
        return TokenRangeMapping.create(
        () -> TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(startToken, instanceCount, 3),
        () -> partitioner,
        metadata -> new RingInstance(metadata, clusterId));
    }

    private Range<BigInteger> range(long start, long end)
    {
        return Range.openClosed(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    private List<RingInstance> getSoleValue(Map<?, List<RingInstance>> map)
    {
        if (map.isEmpty())
        {
            return Collections.emptyList();
        }

        assertThat(map).hasSize(1);
        return map.values().iterator().next();
    }
}
