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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.ConsistencyVerificationResult;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobProgressFetchPolicy;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobStatus;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.ImportCoordinator;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.exception.ImportFailedException;
import org.apache.cassandra.spark.transports.storage.extensions.CoordinationSignalListener;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;

import static org.apache.cassandra.spark.transports.storage.extensions.TransportExtensionUtils.validateReceivedJobId;

/**
 * Import coordinator that implements the two phase import for coordinated write.
 * 1. It waits for the stageReady signal from StorageTransportExtension and update the restore job status on all clusters to STAGE_READY
 * 2. It, then, polls the stage progress of each cluster and notifies StorageTransportExtension on completion
 * 3. It waits for the applyReady signal from StorageTransportExtension and update the restore job status on all clusters to APPLY_READY
 * 4. It polls the apply/import progress of each cluster
 * 5. Finally, the two phase import completes.
 * The procedure is programed in {@link #awaitInternal()}
 */
public class CoordinatedImportCoordinator implements ImportCoordinator, CoordinationSignalListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedImportCoordinator.class);

    private final long startTimeNanos;
    private final CoordinatedCloudStorageDataTransferApi dataTransferApi;
    private final JobInfo job;
    private final StorageTransportExtension extension;
    private final CompletableFuture<RestoreJobStatus> stageReady = new CompletableFuture<>();
    private final CompletableFuture<RestoreJobStatus> importReady = new CompletableFuture<>();
    private final Set<String> stageCompletedClusters;
    private final Set<String> importCompletedClusters;

    private volatile boolean succeeded = false;
    private volatile ImportFailedException importFailedException = null;

    @VisibleForTesting
    CoordinatedImportCoordinator(long startTimeNanos,
                                 JobInfo job,
                                 CoordinatedCloudStorageDataTransferApi dataTransferApi,
                                 StorageTransportExtension extension)
    {
        this.startTimeNanos = startTimeNanos;
        this.job = job;
        this.dataTransferApi = dataTransferApi;
        this.stageCompletedClusters = ConcurrentHashMap.newKeySet(dataTransferApi.size());
        this.importCompletedClusters = ConcurrentHashMap.newKeySet(dataTransferApi.size());
        this.extension = extension;
    }

    public static CoordinatedImportCoordinator of(long startTimeNanos,
                                                  JobInfo job,
                                                  CoordinatedCloudStorageDataTransferApi dataTransferApi,
                                                  StorageTransportExtension extension)
    {
        return new CoordinatedImportCoordinator(startTimeNanos,
                                                job,
                                                dataTransferApi,
                                                extension);
    }

    @Override
    public boolean succeeded()
    {
        return succeeded;
    }

    @Override
    public ImportFailedException failure()
    {
        return importFailedException;
    }

    @Override
    public void await() throws ImportFailedException
    {
        try
        {
            awaitInternal();
            succeeded = true;
        }
        catch (Exception cause)
        {
            importFailedException = new ImportFailedException(cause);
            throw importFailedException;
        }
    }

    @Override
    public void onStageReady(String jobId)
    {
        validateReceivedJobId(jobId, job);
        LOGGER.info("Received StageReady signal for coordinated write. Notifying Sidecars. jobId={}", jobId);
        stageReady.complete(RestoreJobStatus.STAGE_READY);
    }

    @Override
    public void onImportReady(String jobId)
    {
        validateReceivedJobId(jobId, job);
        LOGGER.info("Received ImportReady signal for coordinated write. Notifying Sidecars. jobId={}", jobId);
        importReady.complete(RestoreJobStatus.IMPORT_READY);
    }

    private void awaitInternal()
    {
        // wait until coordination extension signals that it is ready to stage
        waitForStageReady();
        // driver notifies sidecars to stage
        sendCoordinationSignal(RestoreJobStatus.STAGE_READY);
        // driver polls for all clusters to finish staging
        pollForPhaseCompletion(stageCompletedClusters,
                               extension::onStageSucceeded,
                               extension::onStageFailed);
        // once complete, advance to staged in all clusters
        sendCoordinationSignal(RestoreJobStatus.STAGED);

        // wait until coordination extension signals that
        waitForImportReady();
        // driver notifies sidecars to import
        sendCoordinationSignal(RestoreJobStatus.IMPORT_READY);
        // driver polls for all clusters to finish importing
        pollForPhaseCompletion(importCompletedClusters,
                               extension::onImportSucceeded,
                               extension::onImportFailed);
    }

    private void waitForStageReady()
    {
        Preconditions.checkState(RestoreJobStatus.STAGE_READY == stageReady.join());
    }

    private void waitForImportReady()
    {
        Preconditions.checkState(RestoreJobStatus.IMPORT_READY == importReady.join());
    }

    // When completedClusters contains the current clusterId, we skip checking the progress on the cluster
    // Otherwise, it retrieves progress from each cluster and invoke the callbacks accordingly
    // When any cluster fails, it throws ImportFailedException and stop
    private void pollForPhaseCompletion(Set<String> completedClusters,
                                        BiConsumer<String, Long> onClusterCompletion,
                                        BiConsumer<String, Throwable> failureHandler) throws ConsistencyNotSatisfiedException
    {
        // all clusters should complete before exiting the loop
        while (completedClusters.size() < dataTransferApi.size())
        {
            dataTransferApi.restoreJobProgress(RestoreJobProgressFetchPolicy.FIRST_FAILED, completedClusters::contains, (clusterId, progress) -> {
                if (checkProgressOfCluster(clusterId, progress, completedClusters, failureHandler))
                {
                    onClusterCompletion.accept(clusterId, elapsedMillis());
                }
            });
        }
    }

    private long elapsedMillis()
    {
        long nowNanos = System.nanoTime();
        long elapsedNanos = nowNanos - startTimeNanos;
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
    }

    private void sendCoordinationSignal(RestoreJobStatus status)
    {
        UpdateRestoreJobRequestPayload requestPayload = UpdateRestoreJobRequestPayload.builder().withStatus(status).build();
        dataTransferApi.updateRestoreJob(requestPayload);
    }

    // Returns true if succeeded, false if pending.
    // If it has failed, an ImportFailedException is thrown
    private boolean checkProgressOfCluster(String clusterId,
                                           RestoreJobProgressResponsePayload progress,
                                           Set<String> completedClusters,
                                           BiConsumer<String, Throwable> failureHandler) throws ConsistencyNotSatisfiedException
    {
        if (progress.status() == ConsistencyVerificationResult.SATISFIED)
        {
            completedClusters.add(clusterId);
            return true;
        }

        if (progress.status() == ConsistencyVerificationResult.PENDING)
        {
            return false;
        }

        completedClusters.add(clusterId);
        String error = String.format("Some of the token ranges cannot satisfy with consistency level. " +
                                     "job=%s phase=%s consistencyLevel=%s clusterId=%s ranges=%s",
                                     job.getRestoreJobId(), determineReadyPhase(), job.getConsistencyLevel(), clusterId, progress.failedRanges());
        LOGGER.error(error);
        ConsistencyNotSatisfiedException exception = new ConsistencyNotSatisfiedException(error);
        failureHandler.accept(clusterId, exception);
        throw exception;
    }

    private RestoreJobStatus determineReadyPhase()
    {
        if (importReady.isDone())
        {
            return RestoreJobStatus.IMPORT_READY;
        }
        else if (stageReady.isDone())
        {
            return RestoreJobStatus.STAGE_READY;
        }

        return RestoreJobStatus.CREATED;
    }


    @VisibleForTesting
    boolean isStageReady()
    {
        if (stageReady.isDone())
        {
            return RestoreJobStatus.STAGE_READY == stageReady.join();
        }
        return false;
    }

    @VisibleForTesting
    boolean isImportReady()
    {
        if (importReady.isDone())
        {
            return RestoreJobStatus.IMPORT_READY == importReady.join();
        }
        return false;
    }
}
