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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.exception.CoordinatorNotAvailableException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkWriteValidatorTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkWriteValidatorTest.class);

    @Test
    void testConsistencyCheckFailureWhenDownInstancesFailQuorum()
    {
        BulkWriterContext mockWriterContext = mock(BulkWriterContext.class);
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockWriterContext.cluster()).thenReturn(mockClusterInfo);

        CassandraContext mockCassandraContext = mock(CassandraContext.class);
        when(mockClusterInfo.getCassandraContext()).thenReturn(mockCassandraContext);
        Map<String, String> replicationOptions = new HashMap<>();
        replicationOptions.put("class", "SimpleStrategy");
        replicationOptions.put("replication_factor", "3");
        when(mockClusterInfo.replicationFactor()).thenReturn(new ReplicationFactor(replicationOptions));
        TokenRangeMapping<RingInstance> topology = TokenRangeMapping.create(
        () -> TokenRangeMappingUtils.mockSimpleTokenRangeReplicasResponse(10, 3),
        () -> Partitioner.Murmur3Partitioner,
        metadata -> new RingInstance(metadata, null));
        when(mockClusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(topology);
        Map<RingInstance, WriteAvailability> instanceAvailabilityMap = new HashMap<>(10);
        for (RingInstance instance : topology.getTokenRanges().keySet())
        {
            // Mark nodes 0, 1, 2 as DOWN
            int nodeId = Integer.parseInt(instance.ipAddress().replace("localhost", ""));
            instanceAvailabilityMap.put(instance, (nodeId <= 2) ? WriteAvailability.UNAVAILABLE_DOWN : WriteAvailability.AVAILABLE);
        }
        when(mockClusterInfo.clusterWriteAvailability()).thenReturn(instanceAvailabilityMap);

        JobInfo mockJobInfo = mock(JobInfo.class);
        UUID jobId = UUID.randomUUID();
        when(mockJobInfo.getId()).thenReturn(jobId.toString());
        when(mockJobInfo.getRestoreJobId()).thenReturn(jobId);
        when(mockJobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("testkeyspace", "testtable"));
        when(mockJobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.QUORUM);
        when(mockJobInfo.effectiveSidecarPort()).thenReturn(9043);
        when(mockJobInfo.jobKeepAliveMinutes()).thenReturn(-1);
        when(mockWriterContext.job()).thenReturn(mockJobInfo);

        ReplicaAwareFailureHandler<RingInstance> failureHandler = new MultiClusterReplicaAwareFailureHandler<>(Partitioner.Murmur3Partitioner);
        BulkWriteValidator writerValidator = new BulkWriteValidator(mockWriterContext, failureHandler);
        assertThatThrownBy(() -> writerValidator.validateClOrFail(topology))
        .isExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
        .hasMessageContaining("Failed to write");
    }

    @Test
    void testCoordinatorAvailabilityWithSingleCoordinator()
    {
        ImmutableMap<String, Integer> rfByDc = ImmutableMap.of("DC1", 3);
        TokenRangeMapping<RingInstance> topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfByDc, 6);
        ReplicaAwareFailureHandler<RingInstance> failureHandler = failureHandlerWithFailures(topology, "DC1-i1", "DC1-i2");

        BulkWriteValidator.validateCoordinatorAvailableOrFail(topology, failureHandler, LOGGER, "test", jobInfo());
    }

    @Test
    void testCoordinatorAvailabilityFailsWhenEveryReplicaIsDown()
    {
        ImmutableMap<String, Integer> rfByDc = ImmutableMap.of("DC1", 3);
        TokenRangeMapping<RingInstance> topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfByDc, 3);
        ReplicaAwareFailureHandler<RingInstance> failureHandler =
        failureHandlerWithFailures(topology, "DC1-i1", "DC1-i2", "DC1-i3");

        assertThatThrownBy(() -> BulkWriteValidator.validateCoordinatorAvailableOrFail(topology, failureHandler, LOGGER,
                                                                                       "test", jobInfo()))
        .isExactlyInstanceOf(CoordinatorNotAvailableException.class)
        .hasMessageContaining("No available coordinator candidate");
    }

    @Test
    void testCoordinatorAvailabilityFailsWhenEveryReplicaIsPending()
    {
        ImmutableMap<String, Integer> rfByDc = ImmutableMap.of("DC1", 3);
        TokenRangeMapping<RingInstance> topology =
        TokenRangeMappingUtils.buildTokenRangeMappingWithPendingInstances(0, rfByDc, 3, "DC1-i1", "DC1-i2", "DC1-i3");

        assertThatThrownBy(() -> BulkWriteValidator.validateCoordinatorAvailableOrFail(topology, noFailures(), LOGGER,
                                                                                       "test", jobInfo()))
        .isExactlyInstanceOf(CoordinatorNotAvailableException.class)
        .hasMessageContaining("No available coordinator candidate");
    }

    @Test
    void testCoordinatorAvailabilityPassesWhenOnlyRemoteDatacenterIsUp()
    {
        ImmutableMap<String, Integer> rfByDc = ImmutableMap.of("DC1", 3, "DC2", 3);
        TokenRangeMapping<RingInstance> topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfByDc, 3);
        ReplicaAwareFailureHandler<RingInstance> failureHandler =
        failureHandlerWithFailures(topology, "DC2-i1", "DC2-i2", "DC2-i3");

        BulkWriteValidator.validateCoordinatorAvailableOrFail(topology, failureHandler, LOGGER, "test", jobInfo());
    }

    @Test
    void testCoordinatorAvailabilityWithNoReplicaForRange()
    {
        ImmutableMap<String, Integer> rfByDc = ImmutableMap.of("DC1", 3);
        List<RingInstance> instances = TokenRangeMappingUtils.getInstances(0, rfByDc, 3).subList(0, 1);
        Multimap<RingInstance, Range<BigInteger>> partialRing = ArrayListMultimap.create();
        partialRing.put(instances.get(0), Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)));
        TokenRangeMapping<RingInstance> topology = new TokenRangeMapping<>(Partitioner.Murmur3Partitioner,
                                                                           partialRing,
                                                                           new HashSet<>(instances));

        assertThatThrownBy(() -> BulkWriteValidator.validateCoordinatorAvailableOrFail(topology, noFailures(), LOGGER,
                                                                                       "test", jobInfo()))
        .isExactlyInstanceOf(CoordinatorNotAvailableException.class)
        .hasMessageContaining("the token ring is incomplete");
    }

    private static JobInfo jobInfo()
    {
        JobInfo mockJobInfo = mock(JobInfo.class);
        when(mockJobInfo.getId()).thenReturn(UUID.randomUUID().toString());
        return mockJobInfo;
    }

    private static ReplicaAwareFailureHandler<RingInstance> noFailures()
    {
        return new MultiClusterReplicaAwareFailureHandler<>(Partitioner.Murmur3Partitioner);
    }

    private static ReplicaAwareFailureHandler<RingInstance> failureHandlerWithFailures(TokenRangeMapping<RingInstance> topology,
                                                                                       String... failedNodeNames)
    {
        Set<String> failed = new HashSet<>(Arrays.asList(failedNodeNames));
        ReplicaAwareFailureHandler<RingInstance> failureHandler = noFailures();
        topology.getTokenRanges().asMap().forEach((instance, ranges) -> {
            if (failed.contains(instance.nodeName()))
            {
                ranges.forEach(range -> failureHandler.addFailure(range, instance, "simulated failure"));
            }
        });
        return failureHandler;
    }
}
