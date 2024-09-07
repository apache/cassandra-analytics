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

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.common.model.NodeState;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenRangeMappingTest
{
    private final ReplicationFactor rf = new ReplicationFactor(new HashMap<String, String>()
    {{
        put("class", "SimpleStrategy");
        put("replication_factor", "3");
    }});

    @Test
    void testCreateTokenRangeMapping()
    {
        TokenRangeMapping<RingInstance> topology = createTestMapping(10);
        assertThat(topology.partitioner()).isEqualTo(Partitioner.Murmur3Partitioner);
        assertThat(topology.replicationFactor()).isEqualTo(rf);
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
                                                          () -> rf, RingInstance::new))
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
                                                                            () -> rf, RingInstance::new);
        assertThat(topology.pendingInstances()).hasSize(pendingCount);
    }

    private TokenRangeMapping<RingInstance> createTestMapping(int instanceCount)
    {
        return TokenRangeMapping.create(
        () -> TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(instanceCount, 3),
        () -> Partitioner.Murmur3Partitioner,
        () -> rf,
        RingInstance::new);
    }
}
