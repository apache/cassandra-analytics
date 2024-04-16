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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.data.QualifiedTableName;
import o.a.c.sidecar.client.shaded.common.data.RingEntry;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.request.Request;
import org.apache.cassandra.spark.bulkwriter.ImportCompletionCoordinator.RequestAndInstance;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice;
import org.apache.cassandra.spark.bulkwriter.blobupload.StorageClient;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ImportCompletionCoordinatorTest
{
    private static final int TOTAL_INSTANCES = 10;

    BulkWriterContext mockWriterContext;
    BulkWriteValidator writerValidator;
    TokenRangeMapping<RingInstance> topology;
    JobInfo mockJobInfo;
    BulkSparkConf mockConf;
    BlobDataTransferApi dataTransferApi;
    UUID jobId;
    StorageTransportExtension mockExtension;
    ArgumentCaptor<String> appliedObjectKeys;
    Consumer<CancelJobEvent> onCancelJob;

    @BeforeEach
    public void setup() throws Exception
    {
        mockJobInfo = mock(JobInfo.class);
        jobId = UUID.randomUUID();
        when(mockJobInfo.getId()).thenReturn(jobId.toString());
        when(mockJobInfo.getRestoreJobId()).thenReturn(jobId);
        when(mockJobInfo.getQualifiedTableName()).thenReturn(new QualifiedTableName("testkeyspace", "testtable"));
        when(mockJobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.QUORUM);
        mockConf = mock(BulkSparkConf.class);
        when(mockConf.getEffectiveSidecarPort()).thenReturn(9043);

        mockWriterContext = mock(BulkWriterContext.class);
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockWriterContext.cluster()).thenReturn(mockClusterInfo);

        CassandraContext mockCassandraContext = mock(CassandraContext.class);
        when(mockClusterInfo.getCassandraContext()).thenReturn(mockCassandraContext);
        topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), TOTAL_INSTANCES);
        when(mockClusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(topology);
        when(mockWriterContext.job()).thenReturn(mockJobInfo);
        when(mockWriterContext.conf()).thenReturn(mockConf);
        when(mockConf.getJobKeepAliveMinutes()).thenReturn(-1);

        writerValidator = new BulkWriteValidator(mockWriterContext, new ReplicaAwareFailureHandler<>(Partitioner.Murmur3Partitioner));

        // clients will not be used in this test class; mock is at the API method level
        BlobDataTransferApi api = new BlobDataTransferApi(mockJobInfo, mock(SidecarClient.class), mock(StorageClient.class));
        dataTransferApi = spy(api);

        mockExtension = mock(StorageTransportExtension.class);
        appliedObjectKeys = ArgumentCaptor.forClass(String.class);
        doNothing().when(mockExtension).onObjectApplied(any(), appliedObjectKeys.capture(), anyLong(), anyLong());

        onCancelJob = event -> {
            throw new RuntimeException("It should not be called");
        };
    }

    @Test
    void testAwaitForCompletionWithNoErrors()
    {
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, false, 0);
        ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                       writerValidator, resultList, mockExtension, onCancelJob)
                                   .waitForCompletion();
        validateAllSlicesWereCalledAtMostOnce(resultList);
        assertEquals(resultList.size(), appliedObjectKeys.getAllValues().size(),
                     "All objects should be applied and reported for exactly once");
        assertEquals(allTestObjectKeys(), new HashSet<>(appliedObjectKeys.getAllValues()));
    }

    @Test
    void testAwaitForCompletionWithNoErrorsAndSlowImport()
    {
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, true, 0);
        ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                       writerValidator, resultList, mockExtension, onCancelJob)
                                   .waitForCompletion();
        validateAllSlicesWereCalledAtMostOnce(resultList);
        assertEquals(resultList.size(), appliedObjectKeys.getAllValues().size(),
                     "All objects should be applied and reported for exactly once");
        assertEquals(allTestObjectKeys(), new HashSet<>(appliedObjectKeys.getAllValues()));
    }

    @Test // the test scenario has error when checking, but CL passes overall and the import is successful
    void testAwaitForCompletionWithErrorsAndCLPasses()
    {
        // There is 1 failure in each replica set. 2 out of 3 replicas succeeds.
        List<BlobStreamResult> resultList = buildBlobStreamResult(1, false, 0);
        ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                       writerValidator, resultList, mockExtension, onCancelJob)
                                   .waitForCompletion();
        validateAllSlicesWereCalledAtMostOnce(resultList);
        assertEquals(resultList.size(), appliedObjectKeys.getAllValues().size(),
                     "All objects should be applied and reported for exactly once");
        assertEquals(allTestObjectKeys(), new HashSet<>(appliedObjectKeys.getAllValues()));
    }

    @Test // the test scenario has errors that fails CL, the import fails
    void testAwaitForCompletionWithErrorsAndCLFails()
    {
        // There is 2 failure in each replica set. Only 1 out of 3 replicas succeeds.
        // All replica sets fail, the number of ranges is not deterministic.
        // Therefore, the assertion omits the number of ranges in the message
        String errorMessage = "ranges with QUORUM for job " + jobId + " in phase WaitForCommitCompletion";
        List<BlobStreamResult> resultList = buildBlobStreamResult(2, false, 0);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                           writerValidator, resultList, mockExtension, onCancelJob)
                                       .waitForCompletion();
        });
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains("Failed to load"));
        assertTrue(exception.getMessage().contains(errorMessage));
        assertNotNull(exception.getCause());
        validateAllSlicesWereCalledAtMostOnce(resultList);
        assertEquals(0, appliedObjectKeys.getAllValues().size(),
                     "No object should be applied and reported");
    }

    @Test
    void testCLUnsatisfiedRanges()
    {
        String errorMessage = "Some of the token ranges cannot satisfy with consistency level. job=" + jobId + " phase=WaitForCommitCompletion";
        // CL check won't fail as there is no failed instances.
        // The check won't be satisfied too since there is not enough available instances.
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, false, 2);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                           writerValidator, resultList, mockExtension, onCancelJob)
                                       .waitForCompletion();
        });
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains(errorMessage));
        assertNull(exception.getCause());
        validateAllSlicesWereCalledAtMostOnce(resultList);
        assertEquals(0, appliedObjectKeys.getAllValues().size(),
                     "No object should be applied and reported");
    }

    @Test
    void testAwaitShouldPassWithStuckSliceWhenClSatisfied()
    {
        /*
         * When slice import is stuck on server side, i.e. import request never indicate the slice is complete.
         * If the consistency level has been satisfied for all ranges, it is safe to ignore the abnormal status
         * of the stuck slices.
         * The test verifies that in such scenario, ImportCompletionCoordinator does not block forever,
         * and it can conclude success result
         */
        List<BlobStreamResult> resultList = buildBlobStreamResultWithNoProgressImports(1);
        ImportCompletionCoordinator coordinator = ImportCompletionCoordinator.of(0, mockWriterContext, dataTransferApi,
                                                                                 writerValidator, resultList, mockExtension, onCancelJob);
        coordinator.waitForCompletion();
        assertEquals(resultList.size(), appliedObjectKeys.getAllValues().size(),
                     "All objects should be applied and reported for exactly once");
        assertEquals(allTestObjectKeys(), new HashSet<>(appliedObjectKeys.getAllValues()));
        Map<CompletableFuture<Void>, RequestAndInstance> importFutures = coordinator.importFutures();
        int cancelledImports = importFutures.keySet().stream().mapToInt(f -> f.isCancelled() ? 1 : 0).sum();
        assertEquals(TOTAL_INSTANCES, cancelledImports,
                     "Each replica set should have a slice gets cancelled due to making no progress");
    }

    @Test
    void testJobCancelOnTopologyChanged()
    {
        AtomicBoolean isCancelled = new AtomicBoolean(false);
        Consumer<CancelJobEvent> onCancel = event -> {
            isCancelled.set(true);
        };
        BulkWriterContext mockWriterContext = mock(BulkWriterContext.class);
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockWriterContext.cluster()).thenReturn(mockClusterInfo);
        when(mockClusterInfo.getTokenRangeMapping(false))
        .thenReturn(TokenRangeMappingUtils.buildTokenRangeMapping(0,
                                                                  ImmutableMap.of("DC1", 3),
                                                                  TOTAL_INSTANCES))
        .thenReturn(TokenRangeMappingUtils.buildTokenRangeMapping(0,
                                                                  ImmutableMap.of("DC1", 3),
                                                                  TOTAL_INSTANCES + 1)); // adding a new instance; expansion
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, false, 0);
        AtomicReference<CassandraTopologyMonitor> monitorRef = new AtomicReference<>(null);
        ImportCompletionCoordinator coordinator = new ImportCompletionCoordinator(0, mockWriterContext, dataTransferApi,
                                                                                  writerValidator, resultList, mockExtension, onCancel,
                                                                                  (clusterInfo, onCancelJob) -> {
                                                                                      monitorRef.set(new CassandraTopologyMonitor(clusterInfo, onCancelJob));
                                                                                      return monitorRef.get();
                                                                                  });
        monitorRef.get().checkTopologyOnDemand();
        CompletionException coordinatorEx = assertThrows(CompletionException.class, coordinator::waitForCompletion);
        assertEquals("Topology changed during bulk write", coordinatorEx.getCause().getMessage());
        assertTrue(isCancelled.get());
        CompletableFuture<Void> firstFailure = coordinator.firstFailure();
        assertTrue(firstFailure.isCompletedExceptionally());
        ExecutionException firstFailureEx = assertThrows(ExecutionException.class, firstFailure::get);
        assertEquals(coordinatorEx.getCause(), firstFailureEx.getCause());
    }

    private Set<String> allTestObjectKeys()
    {
        return IntStream.range(0, 10).boxed().map(i -> "key_for_instance_" + i).collect(Collectors.toSet());
    }

    private List<BlobStreamResult> buildBlobStreamResultWithNoProgressImports(int noProgressInstanceCount)
    {
        return buildBlobStreamResult(0, false, 0, noProgressInstanceCount);
    }

    private List<BlobStreamResult> buildBlobStreamResult(int failedInstanceCount, boolean simulateSlowImport, int unavailableInstanceCount)
    {
        return buildBlobStreamResult(failedInstanceCount, simulateSlowImport, unavailableInstanceCount, 0);
    }

    /**
     * @param failedInstanceCount number of instances in each replica set that fail the http request
     * @param simulateSlowImport slow import with artificial delay
     * @param unavailableInstanceCount number of instances in each replica set that is not included in the BlobStreamResult
     * @param noProgressInstanceCount number of instances in each replica set that make no progress, i.e. future never complete
     * @return a list of blob stream result
     */
    private List<BlobStreamResult> buildBlobStreamResult(int failedInstanceCount,
                                                         boolean simulateSlowImport,
                                                         int unavailableInstanceCount,
                                                         int noProgressInstanceCount)
    {
        List<BlobStreamResult> resultList = new ArrayList<>();
        int totalInstances = 10;

        for (int i = 0; i < totalInstances; i++)
        {
            List<RingInstance> replicaSet = Arrays.asList(ringInstance(i, totalInstances),
                                                          ringInstance(i + 1, totalInstances),
                                                          ringInstance(i + 2, totalInstances));
            Set<CreatedRestoreSlice> createdRestoreSlices = new HashSet<>();
            int failedPerReplica = failedInstanceCount;
            int unavailablePerReplica = unavailableInstanceCount;
            int noProgressPerReplicaSet = noProgressInstanceCount;
            // create one distinct slice per instance
            CreateSliceRequestPayload mockCreateSliceRequestPayload = mock(CreateSliceRequestPayload.class);
            when(mockCreateSliceRequestPayload.startToken()).thenReturn(BigInteger.valueOf(100 * i));
            when(mockCreateSliceRequestPayload.endToken()).thenReturn(BigInteger.valueOf(100 * (1 + i)));
            when(mockCreateSliceRequestPayload.sliceId()).thenReturn(UUID.randomUUID().toString());
            when(mockCreateSliceRequestPayload.key()).thenReturn("key_for_instance_" + i); // to be captured by extension mock
            when(mockCreateSliceRequestPayload.bucket()).thenReturn("bucket"); // to be captured by extension mock
            when(mockCreateSliceRequestPayload.compressedSize()).thenReturn(1L); // to be captured by extension mock
            when(mockCreateSliceRequestPayload.compressedSizeOrZero()).thenReturn(1L);
            List<RingInstance> passedReplicaSet = new ArrayList<>();
            for (RingInstance instance : replicaSet)
            {
                if (unavailablePerReplica-- > 0)
                {
                    continue; // do not include this instance
                }
                passedReplicaSet.add(instance);
                createdRestoreSlices.add(new CreatedRestoreSlice(mockCreateSliceRequestPayload));
                if (simulateSlowImport && i == totalInstances - 1)
                {
                    // only add slowness for the last import
                    doAnswer((Answer<CompletableFuture<Void>>) invocation -> {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(2000));
                        return CompletableFuture.completedFuture(null);
                    })
                    .when(dataTransferApi)
                    .createRestoreSliceFromDriver(eq(new SidecarInstanceImpl(instance.nodeName(), 9043)),
                                                  eq(mockCreateSliceRequestPayload));
                }
                else if (noProgressPerReplicaSet-- > 0)
                {
                    // return a future that does complete
                    doReturn(new CompletableFuture<>())
                    .when(dataTransferApi)
                    .createRestoreSliceFromDriver(eq(new SidecarInstanceImpl(instance.nodeName(), 9043)),
                                                  eq(mockCreateSliceRequestPayload));
                }
                else if (failedPerReplica-- > 0)
                {
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.completeExceptionally(RetriesExhaustedException.of(10, mock(Request.class), null));
                    doReturn(future)
                    .when(dataTransferApi)
                    .createRestoreSliceFromDriver(eq(new SidecarInstanceImpl(instance.nodeName(), 9043)),
                                                  eq(mockCreateSliceRequestPayload));
                }
                else
                {
                    doReturn(CompletableFuture.completedFuture(null))
                    .when(dataTransferApi)
                    .createRestoreSliceFromDriver(eq(new SidecarInstanceImpl(instance.nodeName(), 9043)),
                                                  eq(mockCreateSliceRequestPayload));
                }
            }
            BlobStreamResult result = new BlobStreamResult("", mock(Range.class), Collections.emptyList(),
                                                           passedReplicaSet, createdRestoreSlices, 0, 0);
            resultList.add(result);
        }
        return resultList;
    }

    // Some slice might not be called due to short circuit, hence at most once
    private void validateAllSlicesWereCalledAtMostOnce(List<BlobStreamResult> resultList)
    {
        for (BlobStreamResult blobStreamResult : resultList)
        {
            for (RingInstance instance : blobStreamResult.passed)
            {
                for (CreatedRestoreSlice createdRestoreSlice : blobStreamResult.createdRestoreSlices)
                {
                    verify(dataTransferApi, atMostOnce())
                    .createRestoreSliceFromDriver(eq(new SidecarInstanceImpl(instance.nodeName(), 9043)),
                                                  eq(createdRestoreSlice.sliceRequestPayload()));
                }
            }
        }
    }

    private RingInstance ringInstance(int i, int totalInstances)
    {
        int instanceInRing = i % totalInstances + 1;
        return new RingInstance(new RingEntry.Builder()
                                .datacenter("DC1")
                                .address("127.0.0." + instanceInRing)
                                .token(String.valueOf(i * 100))
                                .fqdn("instance-" + instanceInRing)
                                .build());
    }
}
