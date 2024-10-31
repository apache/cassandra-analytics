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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.data.ConsistencyVerificationResult;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.spark.bulkwriter.CassandraContext;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageDataTransferApiImpl;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.SimpleClusterConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.exception.ImportFailedException;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.mockito.ArgumentCaptor;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class CoordinatedImportCoordinatorTest
{
    CoordinatedImportCoordinator coordinator;
    UUID jobId;
    JobInfo mockJobInfo;
    CassandraCoordinatedBulkWriterContext mockWriterContext;
    StorageTransportExtension mockExtension;
    CoordinatedCloudStorageDataTransferApi dataTransferApi;
    Map<String, CloudStorageDataTransferApiImpl> apiPerCluster;
    String clusterId1 = "cluster1";
    String clusterId2 = "cluster2";

    // extension call captors
    ArgumentCaptor<String> stagedClusters;
    ArgumentCaptor<String> appliedClusters;

    @BeforeEach
    public void setup() throws Exception
    {
        mockJobInfo = mock(JobInfo.class);
        jobId = UUID.randomUUID();
        when(mockJobInfo.getId()).thenReturn(jobId.toString());
        when(mockJobInfo.getRestoreJobId()).thenReturn(jobId);
        when(mockJobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("testkeyspace", "testtable"));
        when(mockJobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.LOCAL_QUORUM);
        when(mockJobInfo.jobKeepAliveMinutes()).thenReturn(-1);

        Map<String, SimpleClusterConf> clusters = new HashMap<>();
        clusters.put(clusterId1, new SimpleClusterConf(Arrays.asList("instance-1:9043", "instance-2:9043", "instance-3:9043"), "dc1"));
        clusters.put(clusterId2, new SimpleClusterConf(Arrays.asList("instance-4:9043", "instance-5:9043", "instance-6:9043"), "dc1"));
        CoordinatedWriteConf coordinatedWriteConf = new CoordinatedWriteConf(clusters);
        when(mockJobInfo.coordinatedWriteConf()).thenReturn(coordinatedWriteConf);

        mockWriterContext = mock(CassandraCoordinatedBulkWriterContext.class);

        CassandraClusterInfoGroup clusterInfoGroup = CassandraClusterInfoGroup.createFrom(Arrays.asList(mockCluster(clusterId1), mockCluster(clusterId2)));
        when(mockWriterContext.cluster()).thenReturn(clusterInfoGroup);
        when(mockWriterContext.job()).thenReturn(mockJobInfo);

        dataTransferApi = mockDataTransferApi(Arrays.asList(clusterId1, clusterId2));

        mockExtension = mock(StorageTransportExtension.class);
        stagedClusters = ArgumentCaptor.forClass(String.class);
        appliedClusters = ArgumentCaptor.forClass(String.class);
        doNothing().when(mockExtension).onStageSucceeded(stagedClusters.capture(), anyLong());
        doNothing().when(mockExtension).onImportSucceeded(appliedClusters.capture(), anyLong());

        coordinator = CoordinatedImportCoordinator.of(0, mockJobInfo, dataTransferApi, mockExtension);
    }

    @Test
    void testHappyPath() throws Exception
    {
        // setup for happy path, i.e. all API calls are successful
        for (CloudStorageDataTransferApiImpl api : apiPerCluster.values())
        {
            doNothing().when(api).updateRestoreJob(any());

            doReturn(completeJobProgress())
            .when(dataTransferApi).restoreJobProgress(same(api), any());
        }

        // await in a separate thread
        CompletableFuture<Void> fut = CompletableFuture.runAsync(coordinator::await);

        assertThat(coordinator.isStageReady()).isFalse();
        assertThat(coordinator.isImportReady()).isFalse();
        // signal stage ready
        coordinator.onStageReady(jobId.toString());
        assertThat(coordinator.isStageReady()).isTrue();

        loopAssert(() -> stagedClusters.getAllValues().size() == 2, "waiting for all cluster to stage successfully");
        assertThat(stagedClusters.getAllValues()).containsExactlyInAnyOrder(clusterId1, clusterId2);

        // signal apply read
        coordinator.onImportReady(jobId.toString());

        loopAssert(() -> appliedClusters.getAllValues().size() == 2, "waiting for all cluster to import successfully");
        assertThat(appliedClusters.getAllValues()).containsExactlyInAnyOrder(clusterId1, clusterId2);

        fut.get();
        assertThat(coordinator.succeeded()).isTrue();
    }

    @Test
    void testFailToSendCoordinationSignal()
    {
        for (CloudStorageDataTransferApiImpl api : apiPerCluster.values())
        {
            doThrow(new SidecarApiCallException("failed to send signal")).when(api).updateRestoreJob(any());
        }

        // await in a separate thread
        CompletableFuture<Void> fut = CompletableFuture.runAsync(coordinator::await);

        // signal stage ready
        coordinator.onStageReady(jobId.toString());

        loopAssert(() -> coordinator.failure() != null, "waiting for coordinator to fail");
        assertThat(coordinator.succeeded()).isFalse();
        assertThat(coordinator.failure())
        .isExactlyInstanceOf(ImportFailedException.class)
        .hasRootCauseExactlyInstanceOf(SidecarApiCallException.class)
        .hasRootCauseMessage("failed to send signal");

        assertThatThrownBy(fut::get)
        .isExactlyInstanceOf(ExecutionException.class)
        .hasCauseExactlyInstanceOf(ImportFailedException.class)
        .hasRootCauseExactlyInstanceOf(SidecarApiCallException.class)
        .hasRootCauseMessage("failed to send signal");
    }

    @Test
    void testFailToStage()
    {
        for (CloudStorageDataTransferApiImpl api : apiPerCluster.values())
        {
            doNothing().when(api).updateRestoreJob(any());

            doReturn(failedJobProgress())
            .when(dataTransferApi).restoreJobProgress(same(api), any());
        }

        // await in a separate thread
        CompletableFuture.runAsync(coordinator::await);

        // signal stage ready
        coordinator.onStageReady(jobId.toString());

        loopAssert(() -> coordinator.failure() != null, "waiting for coordinator to fail");
        assertThat(coordinator.succeeded()).isFalse();
        assertThat(coordinator.failure())
        .isExactlyInstanceOf(ImportFailedException.class)
        .hasRootCauseExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
        .hasRootCauseMessage("Some of the token ranges cannot satisfy with consistency level. " +
                             "job=" + jobId +
                             " phase=STAGE_READY consistencyLevel=LOCAL_QUORUM clusterId=cluster1 ranges=null");
    }

    @Test
    void testFailToImport()
    {
        for (CloudStorageDataTransferApiImpl api : apiPerCluster.values())
        {
            doNothing().when(api).updateRestoreJob(any());

            doReturn(completeJobProgress(), // stage is successful
                     failedJobProgress()) // import fails
            .when(dataTransferApi).restoreJobProgress(same(api), any());
        }

        // await in a separate thread
        CompletableFuture.runAsync(coordinator::await);

        // signal stage ready
        coordinator.onStageReady(jobId.toString());

        loopAssert(() -> stagedClusters.getAllValues().size() == 2, "waiting for all cluster to stage successfully");
        assertThat(stagedClusters.getAllValues()).containsExactlyInAnyOrder(clusterId1, clusterId2);

        // signal apply read
        coordinator.onImportReady(jobId.toString());

        loopAssert(() -> coordinator.failure() != null, "waiting for coordinator to fail");
        assertThat(coordinator.succeeded()).isFalse();
        assertThat(coordinator.failure())
        .isExactlyInstanceOf(ImportFailedException.class)
        .hasRootCauseExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
        .hasRootCauseMessage("Some of the token ranges cannot satisfy with consistency level. " +
                             "job=" + jobId +
                             " phase=IMPORT_READY consistencyLevel=LOCAL_QUORUM clusterId=cluster1 ranges=null");
    }

    private ClusterInfo mockCluster(String clusterId)
    {
        ClusterInfo cluster = mock(ClusterInfo.class);
        when(cluster.clusterId()).thenReturn(clusterId);

        ImmutableMap<String, Integer> rfOptions = ImmutableMap.of("dc1", 3);
        ReplicationFactor rf = new ReplicationFactor(NetworkTopologyStrategy, rfOptions);
        when(cluster.replicationFactor()).thenReturn(rf);

        CassandraContext mockCassandraContext = mock(CassandraContext.class);
        when(cluster.getCassandraContext()).thenReturn(mockCassandraContext);

        TokenRangeMapping<RingInstance> topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfOptions, 10);
        when(cluster.getTokenRangeMapping(anyBoolean())).thenReturn(topology);

        return cluster;
    }

    private CoordinatedCloudStorageDataTransferApi mockDataTransferApi(List<String> clusters)
    {
        apiPerCluster = new HashMap<>(clusters.size());
        for (String clusterId : clusters)
        {
            CloudStorageDataTransferApiImpl api = mock(CloudStorageDataTransferApiImpl.class);
            when(api.jobInfo()).thenReturn(mockJobInfo);
            apiPerCluster.put(clusterId, api);
        }
        CoordinatedCloudStorageDataTransferApi coordinatedApi = new CoordinatedCloudStorageDataTransferApi(RateLimiter.create(1000), apiPerCluster);
        return spy(coordinatedApi);
    }

    private RestoreJobProgressResponsePayload completeJobProgress()
    {
        return RestoreJobProgressResponsePayload.builder()
                                                .withStatus(ConsistencyVerificationResult.SATISFIED)
                                                .withMessage("All ranges have succeeded.")
                                                .build();
    }

    private RestoreJobProgressResponsePayload failedJobProgress()
    {
        return RestoreJobProgressResponsePayload.builder()
                                                .withStatus(ConsistencyVerificationResult.FAILED)
                                                .withMessage("One or more ranges have failed.")
                                                .build();
    }

    // loop at most 10 times until the condition is evaluated to true
    private void loopAssert(Supplier<Boolean> condition, String desc)
    {
        int attempts = 10;
        while (!condition.get() && attempts-- > 1)
        {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }

        if (attempts == 0)
        {
            fail("loop assert times out for " + desc);
        }
    }
}
