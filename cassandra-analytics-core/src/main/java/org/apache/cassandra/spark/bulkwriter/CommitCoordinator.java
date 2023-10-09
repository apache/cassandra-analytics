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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.util.ThreadUtil;
import org.jetbrains.annotations.Nullable;

public final class CommitCoordinator extends AbstractFuture<List<CommitResult>> implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitCoordinator.class);

    private final HashMap<RingInstance, ListeningExecutorService> executors = new HashMap<>();
    private final List<StreamResult> successfulUploads;
    private final DataTransferApi transferApi;
    private final ClusterInfo cluster;
    private final JobInfo job;
    private ListenableFuture<List<CommitResult>> allCommits;
    private final String jobSufix;

    public static CommitCoordinator commit(BulkWriterContext bulkWriterContext, StreamResult[] uploadResults)
    {
        CommitCoordinator coordinator = new CommitCoordinator(bulkWriterContext, uploadResults);
        coordinator.commit();
        return coordinator;
    }

    private CommitCoordinator(BulkWriterContext writerContext, StreamResult[] uploadResults)
    {
        this.transferApi = writerContext.transfer();
        this.cluster = writerContext.cluster();
        this.job = writerContext.job();
        this.jobSufix = "-" + job.getId();
        successfulUploads = Arrays.stream(uploadResults)
                                  .filter(result -> !result.passed.isEmpty())
                                  .collect(Collectors.toList());
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        close();
        return allCommits == null || allCommits.cancel(mayInterruptIfRunning);
    }

    void commit()
    {
        // We may have already committed - we should never get here if we did, but if somehow we do we should
        // simply return the commit results we already collected
        if (successfulUploads.size() > 0 && successfulUploads.stream()
                                                             .allMatch(result -> result.commitResults != null
                                                                                 && result.commitResults.size() > 0))
        {
            List<CommitResult> collect = successfulUploads.stream()
                                                          .flatMap(streamResult -> streamResult.commitResults.stream())
                                                          .collect(Collectors.toList());
            set(collect);
            return;
        }
        // First, group commits by instance so we can multi-commit
        Map<RingInstance, Map<String, Range<BigInteger>>> resultsByInstance = getResultsByInstance(successfulUploads);
        List<ListenableFuture<CommitResult>> commitFutures = resultsByInstance.entrySet().stream()
                                                                              .flatMap(entry -> commit(executors, entry.getKey(), entry.getValue()))
                                                                              .collect(Collectors.toList());
        // Create an aggregate ListenableFuture around the list of futures containing the results of the commit calls.
        // We'll fail fast if any of those errMsg (note that an errMsg here means an unexpected exception,
        // not a failure response from CassandraManager).
        // The callback on the aggregate listener sets the return value for this AbstractFuture
        // so callers can make blocking calls to CommitCoordinator::get.
        allCommits = Futures.allAsList(commitFutures);
        Futures.addCallback(allCommits,
                            new FutureCallback<List<CommitResult>>()
                            {
                                public void onSuccess(@Nullable List<CommitResult> result)
                                {
                                    set(result);
                                }

                                public void onFailure(Throwable throwable)
                                {
                                    setException(throwable);
                                }
                            },
                            Runnable::run);
    }

    private Stream<ListenableFuture<CommitResult>> commit(Map<RingInstance, ListeningExecutorService> executors,
                                                          RingInstance instance,
                                                          Map<String, Range<BigInteger>> uploadRanges)
    {
        ListeningExecutorService executorService =
        executors.computeIfAbsent(instance,
                                  inst -> MoreExecutors.listeningDecorator(
                                  Executors.newFixedThreadPool(job.getCommitThreadsPerInstance(),
                                                               ThreadUtil.threadFactory("commit-sstable-" + inst.getNodeName()))));
        List<String> allUuids = new ArrayList<>(uploadRanges.keySet());
        LOGGER.info("Committing UUIDs={}, Ranges={}, instance={}", allUuids, uploadRanges.values(), instance.getNodeName());
        List<List<String>> batches = Lists.partition(allUuids, job.getCommitBatchSize());
        return batches.stream().map(uuids -> {
            String migrationId = UUID.randomUUID().toString();
            return executorService.submit(() -> {
                CommitResult commitResult = new CommitResult(migrationId, instance, uploadRanges);
                try
                {
                    DataTransferApi.RemoteCommitResult result = transferApi.commitSSTables(instance, migrationId, uuids);
                    if (result.isSuccess)
                    {
                        LOGGER.info("[{}]: Commit succeeded on {} for {}", migrationId, instance.getNodeName(), uploadRanges);
                    }
                    else
                    {
                        LOGGER.error("[{}]: Commit failed: uploadRanges: {}, failedUuids: {}, stdErr: {}",
                                     migrationId,
                                     uploadRanges.entrySet(),
                                     result.failedUuids,
                                     result.stdErr);
                        if (result.failedUuids.size() > 0)
                        {
                            addFailures(result.failedUuids, uploadRanges, commitResult, result.stdErr);
                        }
                        else
                        {
                            addFailures(uploadRanges, commitResult, result.stdErr);
                        }
                    }
                }
                catch (Throwable throwable)
                {
                    addFailures(uploadRanges, commitResult, throwable.toString());
                    // On errMsg, refresh cluster information so we get the latest block list and status information to react accordingly
                    cluster.refreshClusterInfo();
                }
                return commitResult;
            });
        });
    }

    private void addFailures(List<String> failedRanges,
                             Map<String, Range<BigInteger>> uploadRanges,
                             CommitResult commitResult,
                             String error)
    {
        failedRanges.forEach(uuid -> {
            String shortUuid = uuid.replace(jobSufix, "");
            commitResult.addFailedCommit(shortUuid, uploadRanges.get(shortUuid), error != null ? error : "Unknown Commit Failure");
        });
    }

    private void addFailures(Map<String, Range<BigInteger>> failedRanges, CommitResult commitResult, String message)
    {
        failedRanges.forEach((key, value) -> commitResult.addFailedCommit(key, value, message));
        LOGGER.debug("Added failures to commitResult by Range: {}", commitResult);
    }

    private Map<RingInstance, Map<String, Range<BigInteger>>> getResultsByInstance(List<StreamResult> successfulUploads)
    {
        return successfulUploads
               .stream()
               .flatMap(upload -> upload.passed
                                  .stream()
                                  .map(instance -> new AbstractMap.SimpleEntry<>(instance,
                                                                                 new AbstractMap.SimpleEntry<>(upload.sessionID, upload.tokenRange))))
               .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey,
                                              Collectors.toMap(instance -> instance.getValue().getKey(),
                                                               instance -> instance.getValue().getValue())));
    }

    @Override
    public void close()
    {
        executors.values().forEach(ExecutorService::shutdownNow);
    }
}
