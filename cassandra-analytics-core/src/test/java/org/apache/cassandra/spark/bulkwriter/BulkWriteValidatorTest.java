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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkWriteValidatorTest
{
    @Test
    void testConsistencyCheckFailureWhenBlockedInstancesFailQuorum()
    {
        BulkWriterContext mockWriterContext = mock(BulkWriterContext.class);
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockWriterContext.cluster()).thenReturn(mockClusterInfo);

        CassandraContext mockCassandraContext = mock(CassandraContext.class);
        when(mockClusterInfo.getCassandraContext()).thenReturn(mockCassandraContext);
        Map<String, String> replicationOptions = new HashMap<>();
        replicationOptions.put("class", "SimpleStrategy");
        replicationOptions.put("replication_factor", "3");
        TokenRangeMapping<RingInstance> topology = CassandraClusterInfo.getTokenRangeReplicas(
        () -> mockSimpleTokenRangeReplicasResponse(10, 3),
        () -> Partitioner.Murmur3Partitioner,
        () -> new ReplicationFactor(replicationOptions),
        ringInstance -> {
            int nodeId = Integer.parseInt(ringInstance.ipAddress().replace("localhost", ""));
            return nodeId <= 2; // block nodes 0, 1, 2
        });
        when(mockClusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(topology);
        Map<RingInstance, InstanceAvailability> instanceAvailabilityMap = new HashMap<>(10);
        Set<String> blocked = topology.getBlockedInstances();
        for (RingInstance instance : topology.getTokenRanges().keySet())
        {
            String ip = instance.ringInstance().address();
            instanceAvailabilityMap.put(instance, blocked.contains(ip) ? InstanceAvailability.UNAVAILABLE_BLOCKED : InstanceAvailability.AVAILABLE);
        }
        when(mockClusterInfo.getInstanceAvailability()).thenReturn(instanceAvailabilityMap);

        JobInfo mockJobInfo = mock(JobInfo.class);
        UUID jobId = UUID.randomUUID();
        when(mockJobInfo.getId()).thenReturn(jobId.toString());
        when(mockJobInfo.getRestoreJobId()).thenReturn(jobId);
        when(mockJobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("testkeyspace", "testtable"));
        when(mockJobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.QUORUM);
        when(mockJobInfo.effectiveSidecarPort()).thenReturn(9043);
        when(mockJobInfo.jobKeepAliveMinutes()).thenReturn(-1);
        when(mockWriterContext.job()).thenReturn(mockJobInfo);

        BulkWriteValidator writerValidator = new BulkWriteValidator(mockWriterContext, new ReplicaAwareFailureHandler<>(Partitioner.Murmur3Partitioner));
        assertThatThrownBy(() -> writerValidator.validateClOrFail(topology))
        .isExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
        .hasMessageContaining("Failed to write");
    }

    private TokenRangeReplicasResponse mockSimpleTokenRangeReplicasResponse(int instancesCount, int replicationFactor)
    {
        long startToken = 0;
        long rangeLength = 100;
        List<ReplicaInfo> replicaInfoList = new ArrayList<>(instancesCount);
        Map<String, ReplicaMetadata> replicaMetadata = new HashMap<>(instancesCount);
        for (int i = 0; i < instancesCount; i++)
        {
            long endToken = startToken + rangeLength;
            List<String> replicas = new ArrayList<>(replicationFactor);
            for (int r = 0; r < replicationFactor; r++)
            {
                replicas.add("localhost" + (i + r) % instancesCount);
            }
            Map<String, List<String>> replicasPerDc = new HashMap<>();
            replicasPerDc.put("ignored", replicas);
            ReplicaInfo ri = new ReplicaInfo(String.valueOf(startToken), String.valueOf(endToken), replicasPerDc);
            replicaInfoList.add(ri);
            String address = "localhost" + i;
            ReplicaMetadata rm = new ReplicaMetadata("NORMAL", "UP", address, address, 9042, "ignored");
            replicaMetadata.put(address, rm);
            startToken = endToken;
        }
        return new TokenRangeReplicasResponse(replicaInfoList, replicaInfoList, replicaMetadata);
    }
}
