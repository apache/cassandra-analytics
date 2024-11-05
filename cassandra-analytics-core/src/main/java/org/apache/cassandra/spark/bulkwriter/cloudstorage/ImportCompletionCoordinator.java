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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.BulkWriteValidator;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.CancelJobEvent;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.exception.ImportFailedException;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.cassandra.util.ThreadUtil;

import static org.apache.cassandra.clients.Sidecar.toSidecarInstance;
import static org.apache.cassandra.spark.bulkwriter.cloudstorage.CreatedRestoreSlice.ConsistencyLevelCheckResult.NOT_SATISFIED;
import static org.apache.cassandra.spark.bulkwriter.cloudstorage.CreatedRestoreSlice.ConsistencyLevelCheckResult.SATISFIED;

/**
 * Import coordinator that wait for import of all slices to complete.
 * 1. It queries all relevant sidecar instance to learned whether slices are imported or not
 * 2. It aggregates the results locally to determine whether consistency level has been satisfied or not.
 * The procedure is programed in {@link #await()}
 */
public final class ImportCompletionCoordinator implements ImportCoordinator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCompletionCoordinator.class);

    private final long startTimeNanos;
    private final CloudStorageDataTransferApi dataTransferApi;
    private final BulkWriteValidator writeValidator;
    private final List<CloudStorageStreamResult> cloudStorageStreamResultList;
    private final JobInfo job;
    private final ScheduledExecutorService scheduler;
    private final CassandraTopologyMonitor cassandraTopologyMonitor;
    private final ReplicationFactor replicationFactor;
    private final StorageTransportExtension extension;
    private final CompletableFuture<Void> firstFailure = new CompletableFuture<>();
    private final AtomicReference<ImportFailedException> importFailedException = new AtomicReference<>(null);
    private final CompletableFuture<Void> terminal = new CompletableFuture<>();
    private final Map<CompletableFuture<Void>, RequestAndInstance> importFutures = new HashMap<>();
    private final AtomicBoolean consistencyLevelReached = new AtomicBoolean(false);
    private final AtomicInteger completedSlices = new AtomicInteger(0);

    private long waitStartNanos;
    private long minSliceSize = Long.MAX_VALUE;
    private long maxSliceSize = Long.MIN_VALUE;
    private int totalSlices;
    private AtomicInteger satisfiedSlices;

    private ImportCompletionCoordinator(long startTimeNanos,
                                        BulkWriterContext writerContext,
                                        CloudStorageDataTransferApi dataTransferApi,
                                        BulkWriteValidator writeValidator,
                                        List<CloudStorageStreamResult> cloudStorageStreamResultList,
                                        StorageTransportExtension extension,
                                        Consumer<CancelJobEvent> onCancelJob)
    {
        this(startTimeNanos, writerContext, dataTransferApi, writeValidator,
             cloudStorageStreamResultList, extension, onCancelJob, CassandraTopologyMonitor::new);
    }

    @VisibleForTesting
    ImportCompletionCoordinator(long startTimeNanos,
                                BulkWriterContext writerContext,
                                CloudStorageDataTransferApi dataTransferApi,
                                BulkWriteValidator writeValidator,
                                List<CloudStorageStreamResult> cloudStorageStreamResultList,
                                StorageTransportExtension extension,
                                Consumer<CancelJobEvent> onCancelJob,
                                BiFunction<ClusterInfo, Consumer<CancelJobEvent>, CassandraTopologyMonitor> monitorCreator)
    {
        this.startTimeNanos = startTimeNanos;
        this.job = writerContext.job();
        this.dataTransferApi = dataTransferApi;
        this.writeValidator = writeValidator;
        this.cloudStorageStreamResultList = cloudStorageStreamResultList;
        this.extension = extension;
        ThreadFactory tf = ThreadUtil.threadFactory("Import completion timeout");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(tf);
        Consumer<CancelJobEvent> wrapped = cancelJobEvent -> {
            // try to complete the firstFailure, in order to exit coordinator ASAP
            setImportFailure(new ImportFailedException(cancelJobEvent.reason, cancelJobEvent.exception));
            onCancelJob.accept(cancelJobEvent);
        };
        this.cassandraTopologyMonitor = monitorCreator.apply(writerContext.cluster(), wrapped);
        this.replicationFactor = writerContext.cluster().replicationFactor();
    }


    public static ImportCompletionCoordinator of(long startTimeNanos,
                                                 BulkWriterContext writerContext,
                                                 CloudStorageDataTransferApi dataTransferApi,
                                                 BulkWriteValidator writeValidator,
                                                 List<CloudStorageStreamResult> resultsAsCloudStorageStreamResults,
                                                 StorageTransportExtension extension,
                                                 Consumer<CancelJobEvent> onCancelJob)
    {
        return new ImportCompletionCoordinator(startTimeNanos,
                                               writerContext, dataTransferApi,
                                               writeValidator, resultsAsCloudStorageStreamResults,
                                               extension, onCancelJob);
    }

    @Override
    public boolean succeeded()
    {
        return consistencyLevelReached.get();
    }

    @Override
    public ImportFailedException failure()
    {
        return importFailedException.get();
    }

    /**
     * Block for the imports to complete by invoking the CreateRestoreJobSlice call to the server.
     * The method passes when the successful import can satisfy the configured consistency level;
     * otherwise, the method fails.
     * The wait is indefinite until one of the following conditions is met,
     * 1) _all_ slices have been checked, or
     * 2) the spark job reaches to its completion timeout
     * 3) At least one slice fails CL validation, as the job will eventually fail in this case.
     *    this means that some slices may never be processed by this loop
     * <p>
     * When there is a slice failed on CL validation and there are remaining slices to check, the wait continues.
     */
    @Override
    public void await() throws ImportFailedException
    {
        writeValidator.setPhase("WaitForImportCompletion");

        try
        {
            awaitInternal();
        }
        catch (Exception ex)
        {
            // rethrow as ImportFailedException, unwrap if any of the cause is ImportFailedException
            throw ImportFailedException.propagate(ex);
        }
        finally
        {
            if (terminal.isDone())
            {
                LOGGER.info("Concluded the safe termination, given the specified consistency level is satisfied " +
                            "and enough time has been blocked for importing slices.");
            }
            cassandraTopologyMonitor.shutdownNow();
            importFutures.keySet().forEach(f -> f.cancel(true));
            terminal.complete(null);
            scheduler.shutdownNow(); // shutdown and do not wait for the termination; the job is completing
        }
    }

    private void awaitInternal()
    {
        prepareToPoll();

        startPolling();

        waitForCompletion();
    }

    private void prepareToPoll()
    {
        totalSlices = cloudStorageStreamResultList.stream().mapToInt(res -> res.createdRestoreSlices.size()).sum();
        cloudStorageStreamResultList
        .stream()
        .flatMap(res -> res.createdRestoreSlices
                        .stream()
                        .map(CreatedRestoreSlice::sliceRequestPayload))
        .mapToLong(slice -> {
            // individual task should never return slice with 0-size bundle
            long size = slice.compressedSizeOrZero();
            if (size == 0)
            {
                throw new IllegalStateException("Found invalid slice with 0 compressed size. " +
                                                "slice: " + slice);
            }
            return size;
        })
        .forEach(size -> {
            minSliceSize = Math.min(minSliceSize, size);
            maxSliceSize = Math.max(maxSliceSize, size);
        });
        satisfiedSlices = new AtomicInteger(0);
        waitStartNanos = System.nanoTime();
    }

    private void startPolling()
    {
        for (CloudStorageStreamResult cloudStorageStreamResult : cloudStorageStreamResultList)
        {
            for (CreatedRestoreSlice createdRestoreSlice : cloudStorageStreamResult.createdRestoreSlices)
            {
                for (RingInstance instance : cloudStorageStreamResult.passed)
                {
                    createSliceInstanceFuture(createdRestoreSlice, instance);
                }
            }
        }
    }

    private void addCompletionMonitor(CompletableFuture<?> future)
    {
        // whenComplete callback will still be invoked when the future is cancelled.
        // In such case, expect CancellationException
        future.whenComplete((v, t) -> {
            if (t instanceof CancellationException)
            {
                RequestAndInstance rai = importFutures.get(future);
                LOGGER.info("Cancelled import. instance={} slice={}", rai.nodeFqdn, rai.requestPayload);
                return;
            }

            LOGGER.info("Completed slice requests {}/{}", completedSlices.incrementAndGet(), importFutures.keySet().size());

            // only enter the block once
            if (satisfiedSlices.get() == totalSlices
                && consistencyLevelReached.compareAndSet(false, true))
            {
                LOGGER.info("The specified consistency level of the job has been satisfied. consistencyLevel={}", job.getConsistencyLevel());

                long nowNanos = System.nanoTime();
                long timeToAllSatisfiedNanos = nowNanos - waitStartNanos;
                long elapsedNanos = nowNanos - startTimeNanos;
                long timeoutNanos = estimateTimeoutNanos(timeToAllSatisfiedNanos, elapsedNanos,
                                                         job.importCoordinatorTimeoutMultiplier(),
                                                         minSliceSize, maxSliceSize,
                                                         job.jobTimeoutSeconds());
                if (timeoutNanos > 0)
                {
                    LOGGER.info("Continuing to waiting on slices completion in order to prevent Cassandra side " +
                                "streaming as much as possible. The estimated additional wait time is {} seconds.",
                                TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
                    // schedule to complete the terminal
                    scheduler.schedule(() -> terminal.complete(null),
                                       timeoutNanos, TimeUnit.NANOSECONDS);
                }
                else
                {
                    // complete immediately since there is no additional time to wait
                    terminal.complete(null);
                }
            }
        });
    }

    private void waitForCompletion()
    {
        // the result either fail early once firstFailure future completes exceptionally, reached timeout (while CL is satisfied),
        // or the results list completes
        CompletableFuture.anyOf(firstFailure, terminal,
                                CompletableFuture.allOf(importFutures.keySet().toArray(new CompletableFuture[0])))
                         .join();

        // double check to make sure all slices are satisfied
        // Because at this point all ranges have been either satisfied or the job has already failed,
        // this is really just a sanity check for things like lost futures/future-introduced bugs
        validateAllRangesAreSatisfied();
    }

    // Calculate the timeout based on the 1) time taken to have all slices satisfied, 2) use import rate and 3) jobTimeoutSeconds
    // The effective timeout is the min of the estimate and the jobTimeoutSeconds (when specified)
    static long estimateTimeoutNanos(long timeToAllSatisfiedNanos,
                                     long elapsedNanos,
                                     double importCoordinatorTimeoutMultiplier,
                                     double minSliceSize,
                                     double maxSliceSize,
                                     long jobTimeoutSeconds)
    {
        long timeoutNanos = timeToAllSatisfiedNanos;
        // use the minSliceSize to get the slowest import rate. R = minSliceSize / T
        // use the maxSliceSize to get the highest amount of time needed for import. D = maxSliceSize / R
        // Please do not combine the two statements below for readability purpose
        double estimatedRateFloor = minSliceSize / timeToAllSatisfiedNanos;
        double timeEstimateBasedOnRate = maxSliceSize / estimatedRateFloor;
        double estimate = Math.max(timeEstimateBasedOnRate, (double) timeoutNanos);
        timeoutNanos = (long) Math.ceil(importCoordinatorTimeoutMultiplier * estimate);
        // consider the jobTimeoutSeconds only if it is specified
        if (jobTimeoutSeconds != -1)
        {
            long remainingTimeoutNanos = TimeUnit.SECONDS.toNanos(jobTimeoutSeconds) - elapsedNanos;
            if (remainingTimeoutNanos <= 0)
            {
                // Timeout has passed, and we have already achieved the desired consistency level.
                // Do not wait any longer
                return 0;
            }
            timeoutNanos = Math.min(timeoutNanos, remainingTimeoutNanos);
        }
        if (TimeUnit.NANOSECONDS.toHours(timeoutNanos) > 1)
        {
            LOGGER.warn("The additional time to wait is more than 1 hour. timeout={} seconds",
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        return timeoutNanos;
    }

    private void createSliceInstanceFuture(CreatedRestoreSlice createdRestoreSlice,
                                           RingInstance instance)
    {
        if (firstFailure.isCompletedExceptionally())
        {
            LOGGER.warn("The job has failed already. Skip sending import request. instance={} slice={}",
                        instance.nodeName(), createdRestoreSlice.sliceRequestPayload());
            return;
        }
        SidecarInstance sidecarInstance = toSidecarInstance(instance, job.effectiveSidecarPort());
        CreateSliceRequestPayload createSliceRequestPayload = createdRestoreSlice.sliceRequestPayload();
        CompletableFuture<Void> fut = dataTransferApi.createRestoreSliceFromDriver(sidecarInstance,
                                                                                   createSliceRequestPayload);
        fut = fut.handleAsync((ignored, throwable) -> {
            if (throwable == null)
            {
                handleSuccessfulSliceInstance(createdRestoreSlice, instance, createSliceRequestPayload);
            }
            else
            {
                // use handle API to swallow the throwable on purpose; the throwable is set to `firstFailure`
                handleFailedSliceInstance(instance, createSliceRequestPayload, throwable);
            }
            return null;
        });
        addCompletionMonitor(fut);
        // Use the fut variable (, instead of the new future object from whenComplete) for key on purpose.
        // So that whenComplete callback can receive CancellationException
        importFutures.put(fut, new RequestAndInstance(createSliceRequestPayload, instance.nodeName()));
    }

    private void handleFailedSliceInstance(RingInstance instance,
                                           CreateSliceRequestPayload createSliceRequestPayload,
                                           Throwable throwable)
    {
        LOGGER.warn("Import failed. instance={} slice={}", instance.nodeName(), createSliceRequestPayload, throwable);

        Range<BigInteger> range = Range.openClosed(createSliceRequestPayload.startToken(),
                                                   createSliceRequestPayload.endToken());
        writeValidator.updateFailureHandler(range, instance, "Failed to import slice. " + throwable.getMessage());
        // it either passes or throw if consistency level cannot be satisfied
        try
        {
            writeValidator.validateClOrFail(cassandraTopologyMonitor.initialTopology(), false);
        }
        catch (RuntimeException rte)
        {
            // record the first failure and cancel queued futures.
            setImportFailure(ImportFailedException.propagate(rte));
        }
    }

    private void handleSuccessfulSliceInstance(CreatedRestoreSlice createdRestoreSlice,
                                               RingInstance instance,
                                               CreateSliceRequestPayload createSliceRequestPayload)
    {
        LOGGER.info("Import succeeded. instance={} slice={}", instance.nodeName(), createSliceRequestPayload);
        createdRestoreSlice.addSucceededInstance(instance);
        if (SATISFIED ==
            createdRestoreSlice.checkForConsistencyLevel(job.getConsistencyLevel(),
                                                         replicationFactor,
                                                         job.getLocalDC()))
        {
            satisfiedSlices.incrementAndGet();
            try
            {
                extension.onObjectApplied(createSliceRequestPayload.bucket(),
                                          createSliceRequestPayload.key(),
                                          createSliceRequestPayload.compressedSizeOrZero(),
                                          System.nanoTime() - startTimeNanos);
            }
            catch (Throwable t)
            {
                // log a warning message and carry on
                LOGGER.warn("StorageTransportExtension fails to process ObjectApplied notification", t);
            }
        }
    }

    /**
     * Validate that all ranges should collect enough write acknowledges to satisfy the consistency level
     * It throws when there is any range w/o enough write acknowledges
     */
    private void validateAllRangesAreSatisfied()
    {
        List<CreatedRestoreSlice> unsatisfiedSlices = new ArrayList<>();
        for (CloudStorageStreamResult cloudStorageStreamResult : cloudStorageStreamResultList)
        {
            for (CreatedRestoreSlice createdRestoreSlice : cloudStorageStreamResult.createdRestoreSlices)
            {
                if (NOT_SATISFIED == createdRestoreSlice.checkForConsistencyLevel(job.getConsistencyLevel(),
                                                                                  replicationFactor,
                                                                                  job.getLocalDC()))
                {
                    unsatisfiedSlices.add(createdRestoreSlice);
                }
            }
        }
        if (unsatisfiedSlices.isEmpty())
        {
            LOGGER.info("All token ranges have satisfied with consistency level. consistencyLevel={} phase={}",
                        job.getConsistencyLevel(), writeValidator.getPhase());
        }
        else
        {
            String message = String.format("Some of the token ranges cannot satisfy with consistency level. " +
                                           "job=%s phase=%s consistencyLevel=%s ranges=%s",
                                           job.getRestoreJobId(), writeValidator.getPhase(), job.getConsistencyLevel(), unsatisfiedSlices);
            LOGGER.error(message);
            throw new ImportFailedException(message);
        }
    }

    private void setImportFailure(ImportFailedException failure)
    {
        if (importFailedException.compareAndSet(null, failure))
        {
            firstFailure.completeExceptionally(failure);
        }
    }

    @VisibleForTesting
    Map<CompletableFuture<Void>, RequestAndInstance> importFutures()
    {
        return importFutures;
    }

    // simple data class to group the request and the node fqdn
    static class RequestAndInstance
    {
        final String nodeFqdn;
        final CreateSliceRequestPayload requestPayload;

        RequestAndInstance(CreateSliceRequestPayload requestPayload, String nodeFqdn)
        {
            this.nodeFqdn = nodeFqdn;
            this.requestPayload = requestPayload;
        }
    }
}
