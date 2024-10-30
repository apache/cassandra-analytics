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

package org.apache.cassandra.cdc.scanner;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLogReader;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.cassandra.util.StatsUtil.reportTimeTaken;

/**
 * The core CDC logic used to execute a single microbatch that reads a set of SSTables, de-duplicates mutations
 * across replicas and builds into a `org.apache.cassandra.spark.reader.StreamScanner` for consumption.
 */
public class CdcScannerBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcScannerBuilder.class);

    protected final CdcBridge cdcBridge;
    protected final CassandraSource cassandraSource;
    protected final CdcOptions cdcOptions;
    final ICdcStats stats;
    final Map<CassandraInstance, CompletableFuture<List<CommitLogReader.Result>>> futures;
    @Nullable
    private final TokenRange tokenRange;
    @NotNull
    final CdcState startState;
    protected final int partitionId;
    private final long startTimeNanos;
    @NotNull
    private final AsyncExecutor executor;
    private final boolean readCommitLogHeader;
    private final long startTimestampMicroseconds;

    public CdcScannerBuilder(CdcBridge cdcBridge,
                             int partitionId,
                             CdcOptions cdcOptions,
                             ICdcStats stats,
                             @Nullable TokenRange tokenRange,
                             @NotNull CdcState startState,
                             @NotNull AsyncExecutor executor,
                             boolean readCommitLogHeader,
                             @NotNull Map<CassandraInstance, List<CommitLog>> logs,
                             CassandraSource cassandraSource)
    {
        this.cdcBridge = cdcBridge;
        this.cdcOptions = cdcOptions;
        this.stats = stats;
        this.tokenRange = tokenRange;
        this.startState = startState;
        this.executor = executor;
        this.readCommitLogHeader = readCommitLogHeader;
        this.startTimeNanos = System.nanoTime();
        this.cassandraSource = cassandraSource;
        this.partitionId = partitionId;
        this.startTimestampMicroseconds = cdcOptions.minimumTimestampMicros();

        LOGGER.debug("Opening CdcScanner " +
                     "numInstances={} startTimestampMicroseconds={} maxCommitLogsPerInstance={} partitionId={} samplingRate={} maxCdcState={}",
                     logs.size(),
                     startTimestampMicroseconds,
                     cdcOptions.maxCommitLogsPerInstance(),
                     partitionId,
                     cdcOptions.samplingRate(),
                     cdcOptions.maxCdcStateSize()
        );

        if (LOGGER.isTraceEnabled())
        {
            logs.values()
                .stream()
                .flatMap(Collection::stream)
                .forEach(log -> LOGGER.trace("Opening CdcScanner to read log instance={} log={} len={} partitionId={} maxOffset={}",
                                             log.instance().nodeName(), log.name(), log.length(), partitionId, log.maxOffset()));
        }

        this.futures = logs.entrySet().stream()
                           .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> openInstance(entry.getValue(),
                                                          cdcOptions.maxCommitLogsPerInstance(), startState.markers,
                                                          executor))
                           );
    }

    private static boolean greaterThanOrEqualToStartMarker(@NotNull CommitLog log,
                                                           @NotNull CommitLogMarkers markers,
                                                           @NotNull ICdcStats stats,
                                                           int partitionId)
    {
        Marker startMarker = markers.startMarker(log);
        Long segmentId = CommitLog.extractVersionAndSegmentId(log).map(Pair::getRight).orElse(null);
        Preconditions.checkArgument(startMarker.instance().equals(log.instance()), "Start marker should be on the same instance as commit log: "
                                                                                   + startMarker.instance().nodeName() + " vs. " + log.instance().nodeName());

        // only read CommitLog if greater than or equal to previously read CommitLog segmentId
        if (segmentId != null && segmentId >= startMarker.segmentId())
        {
            LOGGER.trace("Commit log greater than or equal to startMarker log={} segmentId={} instance={} startMarker={} partitionId={}",
                         log.name(), segmentId, log.instance().nodeName(), startMarker.segmentId(), partitionId);
            return true;
        }

        LOGGER.debug("Commit log before startMarker log={} segmentId={} instance={} startMarker={} partitionId={}",
                     log.name(), log.segmentId(), log.instance().nodeName(), startMarker.segmentId(), partitionId);
        stats.skippedCommitLogsCount(1);
        return false;
    }

    public static Stream<CommitLog> sortAndLimit(int partitionId,
                                                 Collection<CommitLog> logs,
                                                 int maxCommitLogsPerInstance,
                                                 @NotNull CommitLogMarkers markers, ICdcStats stats)
    {
        if (maxCommitLogsPerInstance > 0)
        {
            LOGGER.debug("Sorting and limiting results numLogs={} partitionId={} maxCommitLogsPerInstance={}",
                         logs.size(), partitionId, maxCommitLogsPerInstance);
        }
        return logs.stream()
                   .sorted(CommitLog::compareTo)
                   .filter(log -> greaterThanOrEqualToStartMarker(log, markers, stats, partitionId))
                   .limit(maxCommitLogsPerInstance <= 0 ? Long.MAX_VALUE : maxCommitLogsPerInstance);
    }

    private CompletableFuture<List<CommitLogReader.Result>> openInstance(@NotNull List<CommitLog> logs,
                                                                         int maxCommitLogsPerInstance,
                                                                         @NotNull CommitLogMarkers markers,
                                                                         @NotNull AsyncExecutor executor)
    {
        // read all commit logs on instance in ascending segmentId order and combine results into single future
        // if we fail to read any commit log on the instance we fail this instance
        logs = sortAndLimit(partitionId, logs, maxCommitLogsPerInstance, markers, stats)
               .collect(Collectors.toList());
        return openLogs(0, logs, markers, executor, ImmutableList.of());
    }

    private CompletableFuture<List<CommitLogReader.Result>> openLogs(int index,
                                                                     @NotNull List<CommitLog> logs,
                                                                     @NotNull CommitLogMarkers markers,
                                                                     @NotNull AsyncExecutor executor,
                                                                     @NotNull ImmutableList<CommitLogReader.Result> previous)
    {
        if (index >= logs.size())
        {
            return completedFuture(ImmutableList.of());
        }

        CommitLog log = logs.get(index);
        return executor
               .submit(() -> openReader(log, markers))
               .thenCompose(result -> {
                   // merge latest result with previously read logs
                   ImmutableList.Builder<CommitLogReader.Result> builder = ImmutableList.builder();
                   builder.addAll(previous);
                   if (!result.wasSkipped())
                   {
                       builder.add(result);
                   }
                   ImmutableList<CommitLogReader.Result> merged = builder.build();

                   if (result.isFullyRead() && index + 1 < logs.size())
                   {
                       // fully read previous commit log so move to next log segment
                       return openLogs(index + 1, logs, markers, executor, merged);
                   }

                   // we can't proceed to next commit log until we fully read this log segment
                   // so complete here and retry on next microbatch
                   return completedFuture(merged);
               })
               .handle((list, throwable) -> {
                   if (throwable != null)
                   {
                       // failed to read this commit log so return previous successful results
                       LOGGER.warn("Failed to open CommitLog instance={} log={} high={} partitionId={}",
                                   log.instance().nodeName(), log.name(), markers.startMarker(log), partitionId);
                       return previous;
                   }
                   return list;
               });
    }

    @NotNull
    private CommitLogReader.Result openReader(@NotNull CommitLog log,
                                              @NotNull CommitLogMarkers markers)
    {
        LOGGER.debug("Opening BufferingCommitLogReader instance={} log={} high={} partitionId={}",
                     log.instance().nodeName(), log.name(), markers.startMarker(log), partitionId);
        return reportTimeTaken(() -> cdcBridge.readLog(log,
                                                       tokenRange,
                                                       markers,
                                                       partitionId,
                                                       stats,
                                                       executor,
                                                       null,
                                                       cdcOptions.discardOldMutations() ? startTimestampMicroseconds : null,
                                                       readCommitLogHeader), commitLogReadTime -> {
            LOGGER.debug("Finished reading log on instance instance={} log={} partitionId={} timeNanos={}",
                         log.instance().nodeName(), log.name(), partitionId, commitLogReadTime);
            stats.commitLogReadTime(commitLogReadTime);
            stats.commitLogBytesFetched(log.length());
        });
    }

    public CdcStreamScanner build()
    {
        // block on futures to read all CommitLog mutations and collect CDC updates
        List<PartitionUpdateWrapper> updates =
        futures.values()
               .stream()
               .map(future -> FutureUtils.await(future, throwable -> LOGGER.warn("Failed to read instance with error", throwable)))
               .filter(FutureUtils.FutureResult::isSuccess)
               .map(FutureUtils.FutureResult::value)
               .filter(Objects::nonNull)
               .flatMap(Collection::stream)
               .flatMap(f -> f.updates().stream())
               .collect(Collectors.toList());
        stats.mutationsReadPerBatch(updates.size());

        // begin mutate the start state
        CdcState.Mutator stateMutator = startState.mutate();
        Collection<PartitionUpdateWrapper> filteredUpdates = reportTimeTaken(() -> filterValidUpdates(updates, stateMutator),
                                                                             stats::mutationsFilterTime);

        long now = System.currentTimeMillis();
        filteredUpdates.forEach(update -> stats.changeReceived(update.keyspace(),
                                                               update.table(),
                                                               now - TimeUnit.MICROSECONDS.toMillis(update.maxTimestampMicros()))
        );

        if (stateMutator.isFull(cdcOptions))
        {
            int cdcStateSize = stateMutator.size();
            // we don't want the CDC state to grow indefinately, it indicates something is wrong so fail
            futures.clear();
            LOGGER.error("Watermarker has exceeded max permitted size watermarkerSize={} maxCdcStateSize={}", cdcStateSize, cdcOptions.maxCdcStateSize());
            stats.watermarkerExceededSize(cdcStateSize);
            throw new RuntimeException("Watermark state has exceeded max permitted size: " + cdcStateSize);
        }

        // update CDC state with new marker positions
        // only update marker if we fully read all logs on an instance without errors
        futures.forEach((instance, future) -> {
            if (!future.isCompletedExceptionally())
            {
                future.join()
                      .stream()
                      .map(CommitLogReader.Result::marker)
                      .max(Marker::compareTo)
                      .ifPresent(marker -> stateMutator.advanceMarker(instance, marker));
            }
        });
        futures.clear();

        long timeTakenToReadBatch = System.nanoTime() - startTimeNanos;
        LOGGER.debug("Processed CdcScanner startTimestampMicroseconds={} partitionId={} timeNanos={} updates={}",
                     startTimestampMicroseconds,
                     partitionId,
                     timeTakenToReadBatch,
                     updates.size()
        );
        stats.mutationsBatchReadTime(timeTakenToReadBatch);

        // hand updates over to CdcSortedStreamScanner
        // build new end state, purging expired mutations to prevent the state growing indefinately
        CdcState endState = stateMutator
                            .nextEpoch()
                            .withRange(tokenRange)
                            .purge(stats, startTimestampMicroseconds)
                            .build();
        return buildStreamScanner(filteredUpdates, endState);
    }

    /**
     * Return a CdcSortedStreamScanner to iterate over a collection of Cdc updates
     *
     * @param updates  collection of cdc updates read from the commit logs in this micro-batch
     * @param endState resulting the cdc state to be persisted once this micro-batch completes successfully.
     * @return a CdcSortedStreamScanner
     */
    public CdcStreamScanner buildStreamScanner(Collection<PartitionUpdateWrapper> updates, @NotNull CdcState endState)
    {
        return cdcBridge.openCdcStreamScanner(updates, endState, ThreadLocalRandom.current(), cassandraSource, cdcOptions.samplingRate());
    }

    /**
     * Get rid of invalid updates from the updates
     *
     * @param updates, a collection of CdcUpdates
     * @return a new updates without invalid updates
     */
    private Collection<PartitionUpdateWrapper> filterValidUpdates(Collection<PartitionUpdateWrapper> updates, CdcState.Mutator stateMutator)
    {
        if (updates.isEmpty())
        {
            return updates;
        }

        Map<PartitionUpdateWrapper, List<PartitionUpdateWrapper>> replicaCopies = updates
                                                                                  .stream()
                                                                                  .collect(Collectors.groupingBy(update -> update, Collectors.toList()));

        return replicaCopies.values()
                            .stream()
                            // discard PartitionUpdate w/o enough replicas
                            .filter(update -> filter(update, stateMutator))
                            .map(update -> update.get(0)) // Dedup the valid updates to just 1 copy
                            .collect(Collectors.toList());
    }

    private boolean filter(List<PartitionUpdateWrapper> updates, CdcState.Mutator stateMutator)
    {
        return filter(updates, cdcOptions::minimumReplicas, stateMutator, stats);
    }

    static boolean filter(List<PartitionUpdateWrapper> updates,
                          Function<String, Integer> minimumReplicas,
                          CdcState.Mutator stateMutator,
                          ICdcStats stats)
    {
        if (updates.isEmpty())
        {
            throw new IllegalStateException("Should not receive empty list of updates");
        }

        PartitionUpdateWrapper update = updates.get(0);
        int numReplicas = updates.size() + stateMutator.replicaCount(update);
        int minimumReplicasPerMutation = minimumReplicas.apply(update.keyspace());

        if (numReplicas < minimumReplicasPerMutation)
        {
            // insufficient replica copies to publish
            // so record replica count and handle on subsequent round
            LOGGER.warn("Ignore the partition update due to insufficient replicas received. required={} received={} keyspace={} table={} watermarkerSize={}",
                        minimumReplicasPerMutation, numReplicas, update.keyspace(), update.table(), stateMutator.size());
            stateMutator.recordReplicaCount(update, numReplicas);
            stats.insufficientReplicas(update.keyspace(), update.table());
            return false;
        }

        // sufficient replica copies to publish

        if (updates.stream().anyMatch(stateMutator::seenBefore))
        {
            // mutation previously marked as late
            // now we have sufficient replica copies to publish
            // so clear watermark and publish now
            LOGGER.info("Achieved consistency level for late partition update. required={} received={} keyspace={} table={} watermarkerSize={}",
                        minimumReplicasPerMutation, numReplicas, update.keyspace(), update.table(), stateMutator.size());
            stateMutator.untrackReplicaCount(update);
            stats.lateChangePublished(update.keyspace(), update.table());
            return true;
        }

        // we haven't seen this mutation before and achieved CL, so publish
        stats.changePublished(update.keyspace(), update.table());
        return true;
    }
}
