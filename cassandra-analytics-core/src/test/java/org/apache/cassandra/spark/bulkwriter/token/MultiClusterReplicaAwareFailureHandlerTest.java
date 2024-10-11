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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.apache.cassandra.spark.TestUtils.range;
import static org.apache.cassandra.spark.bulkwriter.RingInstanceTest.instance;
import static org.apache.cassandra.spark.bulkwriter.token.TokenRangeMappingTest.createTestMapping;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MultiClusterReplicaAwareFailureHandlerTest
{
    private static final String DATACENTER_1 = "dc1";
    private final Partitioner partitioner = Partitioner.Murmur3Partitioner;
    private MultiClusterReplicaAwareFailureHandler<RingInstance> handler = new MultiClusterReplicaAwareFailureHandler<>(partitioner);

    @Test
    void testAddFailuresFromBothInstancesWithAndWithoutClusterIdFails()
    {
        RingInstance instance = instance(BigInteger.valueOf(10), "node1", DATACENTER_1);
        RingInstance instanceWithClusterId = new RingInstance(instance.ringEntry(), "testCluster");
        assertThatNoException().isThrownBy(() -> handler.addFailure(range(0, 10), instance, "failure"));
        assertThatThrownBy(() -> handler.addFailure(range(0, 10), instanceWithClusterId, "failure"))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set value for non-null cluster when the container is used for non-coordinated-write");

        // create a new handler and add failures in the other order
        handler = new MultiClusterReplicaAwareFailureHandler<>(partitioner);
        assertThatNoException().isThrownBy(() -> handler.addFailure(range(0, 10), instanceWithClusterId, "failure"));
        assertThatThrownBy(() -> handler.addFailure(range(0, 10), instance, "failure"))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set value for null cluster when the container is used for coordinated-write");
    }

    @Test
    void testGetFailedInstanceFromMultipleClusters()
    {
        RingInstance instanceC1 = instance(BigInteger.ZERO, "node1", DATACENTER_1, "cluster1");
        RingInstance instanceC2 = instance(BigInteger.ZERO, "node1", DATACENTER_1, "cluster2");
        handler.addFailure(range(-10, 0), instanceC1, "failure in cluster1 instance");
        handler.addFailure(range(-10, 0), instanceC2, "failure in cluster2 instance");
        Set<RingInstance> failedInstances = handler.getFailedInstances();
        assertThat(failedInstances)
        .hasSize(2)
        .containsExactlyInAnyOrder(instanceC1, instanceC2);
    }

    @Test
    void testGetFailedRangesOfClusters()
    {
        testFailedRangeCheckWithTwoClusters(ctx -> {
            Range<BigInteger> range = range(1, 10); // range value that does not overlap with the ranges in topology
            Map<Range<BigInteger>, Set<RingInstance>> writeReplicasOfRangeAcrossClusters = ctx.topology.getWriteReplicasOfRange(range, DATACENTER_1);
            assertThat(writeReplicasOfRangeAcrossClusters).hasSize(1);
            Map<String, List<RingInstance>> writeReplicasPerCluster = writeReplicasOfRangeAcrossClusters
                                                                      .values()
                                                                      .stream()
                                                                      .flatMap(Set::stream)
                                                                      .collect(Collectors.groupingBy(RingInstance::clusterId));
            assertThat(writeReplicasPerCluster).hasSize(2).containsKeys("cluster1", "cluster2");
            handler.addFailure(range, writeReplicasPerCluster.get("cluster1").get(0), "failure in cluster1");
            handler.addFailure(range, writeReplicasPerCluster.get("cluster2").get(0), "failure in cluster2");
            assertThat(handler.getFailedRanges(ctx.topology, ctx.jobInfo, ctx.clusterInfo))
            .describedAs("Each cluster should have only 1 failure of the range, which is acceptable for LOCAL_QUORUM.")
            .isEmpty();

            // now cluster1 has 2 failures
            handler.addFailure(range, writeReplicasPerCluster.get("cluster1").get(1), "another failure in cluster1");
            List<ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange>
            failedRanges = handler.getFailedRanges(ctx.topology, ctx.jobInfo, ctx.clusterInfo);
            assertThat(failedRanges)
            .describedAs("Cluster1 should have failed range")
            .hasSize(1);
            Set<RingInstance> failedInstances = failedRanges.get(0).failuresPerInstance.instances();
            assertThat(failedInstances)
            .hasSize(2)
            .containsExactlyInAnyOrder(writeReplicasPerCluster.get("cluster1").get(0),
                                       writeReplicasPerCluster.get("cluster1").get(1));

            // now cluster2 has 2 failures too
            handler.addFailure(range, writeReplicasPerCluster.get("cluster2").get(1), "another failure in cluster2");
            failedRanges = handler.getFailedRanges(ctx.topology, ctx.jobInfo, ctx.clusterInfo);
            assertThat(failedRanges)
            .describedAs("Both clusters should have failed ranges")
            .hasSize(2);
            Set<RingInstance> failedInstancesInCluster2 = failedRanges.get(1).failuresPerInstance.instances();
            assertThat(failedInstancesInCluster2)
            .hasSize(2)
            .containsExactlyInAnyOrder(writeReplicasPerCluster.get("cluster2").get(0),
                                       writeReplicasPerCluster.get("cluster2").get(1));
        });
    }

    private void testFailedRangeCheckWithTwoClusters(Consumer<FailureHandlerTextContext> test)
    {
        TokenRangeMapping<RingInstance> cluster1Topology = createTestMapping(0, 5, partitioner, "cluster1");
        TokenRangeMapping<RingInstance> cluster2Topology = createTestMapping(1, 5, partitioner, "cluster2");
        TokenRangeMapping<RingInstance> consolidatedTopology = TokenRangeMapping.consolidate(Arrays.asList(cluster1Topology, cluster2Topology));
        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.LOCAL_QUORUM);
        CoordinatedWriteConf cwc = mock(CoordinatedWriteConf.class);
        CoordinatedWriteConf.ClusterConf cc = mock(CoordinatedWriteConf.ClusterConf.class);
        when(cc.localDc()).thenReturn(DATACENTER_1);
        when(cwc.cluster(any())).thenReturn(cc);
        when(jobInfo.coordinatedWriteConf()).thenReturn(cwc);
        when(jobInfo.isCoordinatedWriteEnabled()).thenReturn(true);
        CassandraClusterInfoGroup group = mock(CassandraClusterInfoGroup.class);
        ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                     ImmutableMap.of(DATACENTER_1, 3));
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.replicationFactor()).thenReturn(rf);
        when(group.getValueOrThrow(any())).thenReturn(clusterInfo); // return the same clusterinfo for both clusters (for test simplicity)
        test.accept(new FailureHandlerTextContext(consolidatedTopology, jobInfo, group));
    }
}
