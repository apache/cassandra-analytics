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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.apache.cassandra.spark.TestUtils.range;
import static org.apache.cassandra.spark.bulkwriter.RingInstanceTest.instance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SingleClusterReplicaAwareFailureHandlerTest
{
    private static final String DATACENTER_1 = "dc1";
    private final Partitioner partitioner = Partitioner.Murmur3Partitioner;
    private final SingleClusterReplicaAwareFailureHandler<RingInstance> handler = new SingleClusterReplicaAwareFailureHandler<>(partitioner, null);

    @Test
    void testGetFailedInstances()
    {
        RingInstance instance1 = instance(BigInteger.valueOf(10), "instance1", DATACENTER_1);
        RingInstance instance2 = instance(BigInteger.valueOf(20), "instance2", DATACENTER_1);
        assertThat(handler.isEmpty()).isTrue();
        handler.addFailure(range(0, 10), instance1, "instance 1 fails");
        assertThat(handler.isEmpty()).isFalse();
        handler.addFailure(range(0, 10), instance2, "instance 2 fails");
        handler.addFailure(range(10, 20), instance2, "instance 2 fails");
        assertThat(handler.getFailedInstances())
        .hasSize(2)
        .containsExactlyInAnyOrder(instance1, instance2);
    }

    @Test
    public void testMinorityFailuresProduceNoFailedRanges()
    {
        testFailedRangeCheck(ctx -> {
            Range<BigInteger> range = range(0, 10);
            Map<Range<BigInteger>, Set<RingInstance>> writeReplicasOfRange = ctx.topology.getWriteReplicasOfRange(range, DATACENTER_1);
            assertThat(writeReplicasOfRange).hasSize(1);
            List<RingInstance> writeReplicas = new ArrayList<>(writeReplicasOfRange.values().iterator().next());
            assertThat(writeReplicas).hasSize(3);
            RingInstance instance = writeReplicas.get(1);
            // one failure per each distinct range; it should not fail as CL is LOCAL_QUORUM
            Range<BigInteger> range1 = range(0, 3);
            Range<BigInteger> range2 = range(3, 4);
            Range<BigInteger> range3 = range(5, 6);
            handler.addFailure(range1, instance, "Failure 1");
            handler.addFailure(range2, instance, "Failure 2");
            handler.addFailure(range3, instance, "Failure 3");
            assertThat(handler.getFailedRanges(ctx.topology, ctx.jobInfo, ctx.clusterInfo)).isEmpty();
        });
    }

    @Test
    public void testMajorityFailuresProduceFailedRanges()
    {
        testFailedRangeCheck(ctx -> {
            Range<BigInteger> range = range(0, 10);
            Map<Range<BigInteger>, Set<RingInstance>> writeReplicasOfRange = ctx.topology.getWriteReplicasOfRange(range, DATACENTER_1);
            assertThat(writeReplicasOfRange).hasSize(1);
            List<RingInstance> writeReplicas = new ArrayList<>(writeReplicasOfRange.values().iterator().next());
            assertThat(writeReplicas).hasSize(3);
            // the majority of the replicas of range
            RingInstance instance1 = writeReplicas.get(1);
            RingInstance instance2 = writeReplicas.get(2);

            handler.addFailure(range, instance1, "fails on instance 1");
            handler.addFailure(range, instance2, "fails on instance 2");
            List<ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange>
            failedRanges = handler.getFailedRanges(ctx.topology, ctx.jobInfo, ctx.clusterInfo);
            assertThat(failedRanges).isNotEmpty();
        });
    }

    private void testFailedRangeCheck(Consumer<FailureHandlerTextContext> test)
    {

        TokenRangeMapping<RingInstance> topology = TokenRangeMappingTest.createTestMapping(0, 5, partitioner, null);
        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.LOCAL_QUORUM);
        when(jobInfo.getLocalDC()).thenReturn(DATACENTER_1);
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                     ImmutableMap.of(DATACENTER_1, 3));
        when(clusterInfo.replicationFactor()).thenReturn(rf);
        test.accept(new FailureHandlerTextContext(topology, jobInfo, clusterInfo));
    }
}
