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

package org.apache.cassandra.spark.reader;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.RangeTombstoneMarkerImplementation;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CdcScannerBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcScannerBuilder.class);

    // Match both legacy and new version of CommitLogs, for example: CommitLog-12345.log and CommitLog-4-12345.log
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog(-\\d+)*-(\\d+).log");
    private static final CompletableFuture<BufferingCommitLogReader.Result> NO_OP_FUTURE =
            CompletableFuture.completedFuture(null);

    final TableMetadata table;
    final Partitioner partitioner;
    final Stats stats;
    final Map<CassandraInstance, CompletableFuture<List<PartitionUpdateWrapper>>> futures;
    final int minimumReplicasPerMutation;
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    @Nullable
    private final CdcOffsetFilter offsetFilter;
    @NotNull
    final Watermarker watermarker;
    private final int partitionId;
    private final long startTimeNanos;
    @NotNull
    private final TimeProvider timeProvider;

    // CHECKSTYLE IGNORE: Constructor with many parameters
    public CdcScannerBuilder(int partitionId,
                             TableMetadata table,
                             Partitioner partitioner,
                             CommitLogProvider commitLogs,
                             Stats stats,
                             @Nullable SparkRangeFilter sparkRangeFilter,
                             @Nullable CdcOffsetFilter offsetFilter,
                             int minimumReplicasPerMutation,
                             @NotNull Watermarker jobWatermarker,
                             @NotNull String jobId,
                             @NotNull ExecutorService executorService,
                             @NotNull TimeProvider timeProvider)
    {
        this.table = table;
        this.partitioner = partitioner;
        this.stats = stats;
        this.sparkRangeFilter = sparkRangeFilter;
        this.offsetFilter = offsetFilter;
        this.watermarker = jobWatermarker.instance(jobId);
        Preconditions.checkArgument(minimumReplicasPerMutation >= 1,
                                    "minimumReplicasPerMutation should be at least 1");
        this.minimumReplicasPerMutation = minimumReplicasPerMutation;
        this.startTimeNanos = System.nanoTime();
        this.timeProvider = timeProvider;

        Map<CassandraInstance, List<CommitLog>> logs = commitLogs
                .logs()
                .collect(Collectors.groupingBy(CommitLog::instance, Collectors.toList()));
        Map<CassandraInstance, CommitLog.Marker> markers = logs.keySet().stream()
                .map(watermarker::highWaterMark)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(CommitLog.Marker::instance, Function.identity()));

        this.partitionId = partitionId;
        LOGGER.info("Opening CdcScanner numInstances={} start={} maxAgeMicros={} partitionId={} listLogsTimeNanos={}",
                    logs.size(),
                    offsetFilter != null ? offsetFilter.start().getTimestampMicros() : null,
                    offsetFilter != null ? offsetFilter.maxAgeMicros() : null,
                    partitionId,
                    System.nanoTime() - startTimeNanos);

        this.futures = logs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> openInstanceAsync(entry.getValue(),
                                                                                        markers.get(entry.getKey()),
                                                                                        executorService)));
    }

    private static boolean skipCommitLog(@NotNull CommitLog log, @Nullable CommitLog.Marker highwaterMark)
    {
        if (highwaterMark == null)
        {
            return false;
        }
        Long segmentId = extractSegmentId(log);
        if (segmentId != null)
        {
            // Only read CommitLog if greater than or equal to previously read CommitLog segmentId
            return segmentId < highwaterMark.segmentId();
        }
        return true;
    }

    @Nullable
    public static Long extractSegmentId(@NotNull CommitLog log)
    {
        return extractSegmentId(log.name());
    }

    @Nullable
    public static Long extractSegmentId(@NotNull String filename)
    {
        Matcher matcher = CdcScannerBuilder.COMMIT_LOG_FILE_PATTERN.matcher(filename);
        if (matcher.matches())
        {
            try
            {
                return Long.parseLong(matcher.group(2));
            }
            catch (NumberFormatException exception)
            {
                LOGGER.error("Could not parse commit log segmentId name={}", filename, exception);
                return null;
            }
        }
        LOGGER.error("Could not parse commit log filename name={}", filename);
        return null;  // Cannot extract segment id
    }

    private CompletableFuture<List<PartitionUpdateWrapper>> openInstanceAsync(@NotNull List<CommitLog> logs,
                                                                              @Nullable CommitLog.Marker highWaterMark,
                                                                              @NotNull ExecutorService executorService)
    {
        // Read all CommitLogs on instance async and combine into single future,
        // if we fail to read any CommitLog on the instance we fail this instance
        List<CompletableFuture<BufferingCommitLogReader.Result>> futures = logs.stream()
                .map(log -> openReaderAsync(log, highWaterMark, executorService))
                .collect(Collectors.toList());
        return FutureUtils.combine(futures)
                          .thenApply(result -> {
                              // Update highwater mark on success, if instance fails we don't update
                              // highwater mark so resume from original position on next attempt
                              result.stream()
                                    .map(BufferingCommitLogReader.Result::marker)
                                    .max(CommitLog.Marker::compareTo)
                                    .ifPresent(watermarker::updateHighWaterMark);

                              // Combine all updates into single list
                              return result.stream()
                                           .map(BufferingCommitLogReader.Result::updates)
                                           .flatMap(Collection::stream)
                                           .collect(Collectors.toList());
                          });
    }

    private CompletableFuture<BufferingCommitLogReader.Result> openReaderAsync(@NotNull CommitLog log,
                                                                               @Nullable CommitLog.Marker highWaterMark,
                                                                               @NotNull ExecutorService executorService)
    {
        if (skipCommitLog(log, highWaterMark))
        {
            return NO_OP_FUTURE;
        }
        return CompletableFuture.supplyAsync(() -> openReader(log, highWaterMark), executorService);
    }

    @Nullable
    private BufferingCommitLogReader.Result openReader(@NotNull CommitLog log, @Nullable CommitLog.Marker highWaterMark)
    {
        long startTimeNanos = System.nanoTime();
        LOGGER.info("Opening BufferingCommitLogReader instance={} log={} high='{}' partitionId={}",
                    log.instance().nodeName(), log.name(), highWaterMark, partitionId);
        try (BufferingCommitLogReader reader =
                new BufferingCommitLogReader(table, offsetFilter, log, sparkRangeFilter, highWaterMark, partitionId))
        {
            if (reader.isReadable())
            {
                return reader.result();
            }
        }
        finally
        {
            LOGGER.info("Finished reading log on instance instance={} log={} partitionId={} timeNanos={}",
                        log.instance().nodeName(), log.name(), partitionId, System.nanoTime() - startTimeNanos);
        }
        return null;
    }

    public StreamScanner<Rid> build()
    {
        // Block on futures to read all CommitLog mutations and pass over to SortedStreamScanner
        List<PartitionUpdateWrapper> updates = futures.values().stream()
                .map(future -> FutureUtils.await(future, throwable -> LOGGER.warn("Failed to read instance with error",
                                                                                  throwable)))
                .filter(FutureUtils.FutureResult::isSuccess)
                .map(FutureUtils.FutureResult::value)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        futures.clear();

        schedulePersist();

        Collection<PartitionUpdateWrapper> filtered = filterValidUpdates(updates);

        LOGGER.info("Opened CdcScanner start={} maxAgeMicros={} partitionId={} timeNanos={}",
                    offsetFilter != null ? offsetFilter.start().getTimestampMicros() : null,
                    offsetFilter != null ? offsetFilter.maxAgeMicros() : null,
                    partitionId,
                    System.nanoTime() - startTimeNanos);
        return new SortedStreamScanner(table, partitioner, filtered, timeProvider);
    }

    /**
     * A stream scanner that is backed by a sorted collection of {@link PartitionUpdateWrapper}
     */
    private static class SortedStreamScanner extends AbstractStreamScanner
    {
        private final Queue<PartitionUpdateWrapper> updates;

        SortedStreamScanner(@NotNull TableMetadata metadata,
                            @NotNull Partitioner partitionerType,
                            @NotNull Collection<PartitionUpdateWrapper> updates,
                            @NotNull TimeProvider timeProvider)
        {
            super(metadata, partitionerType, timeProvider);
            this.updates = new PriorityQueue<>(PartitionUpdateWrapper::compareTo);
            this.updates.addAll(updates);
        }

        @Override
        UnfilteredPartitionIterator initializePartitions()
        {
            return new UnfilteredPartitionIterator()
            {
                private PartitionUpdateWrapper next;

                @Override
                public TableMetadata metadata()
                {
                    return metadata;
                }

                @Override
                public void close()
                {
                    // Do nothing
                }

                @Override
                public boolean hasNext()
                {
                    if (next == null)
                    {
                        next = updates.poll();
                    }
                    return next != null;
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    PartitionUpdate update = next.partitionUpdate();
                    next = null;
                    return update.unfilteredIterator();
                }
            };
        }

        @Override
        public void close()
        {
            updates.clear();
        }

        @Override
        protected void handleRowTombstone(Row row)
        {
            // Prepare clustering data to be consumed the next
            columnData = new ClusteringColumnDataState(row.clustering());
            rid.setTimestamp(row.deletion().time().markedForDeleteAt());
            // Flag was reset at org.apache.cassandra.spark.sparksql.SparkCellIterator.getNext
            rid.setRowDeletion(true);
        }

        @Override
        protected void handlePartitionTombstone(UnfilteredRowIterator partition)
        {
            rid.setPartitionKeyCopy(partition.partitionKey().getKey(),
                                    ReaderUtils.tokenToBigInteger(partition.partitionKey().getToken()));
            rid.setTimestamp(partition.partitionLevelDeletion().markedForDeleteAt());
            // Flag was reset at org.apache.cassandra.spark.sparksql.SparkCellIterator.getNext
            rid.setPartitionDeletion(true);
        }

        @Override
        protected void handleCellTombstone()
        {
            rid.setValueCopy(null);
        }

        @Override
        protected void handleCellTombstoneInComplex(Cell<?> cell)
        {
            if (cell.column().type instanceof ListType)
            {
                LOGGER.warn("Unable to process element deletions inside a List type. Skipping...");
                return;
            }

            CellPath path = cell.path();
            if (0 < path.size())  // Size can either be 0 (EmptyCellPath) or 1 (SingleItemCellPath)
            {
                rid.addCellTombstoneInComplex(path.get(0));
            }
        }

        @Override
        protected void handleRangeTombstone(RangeTombstoneMarker marker)
        {
            rid.addRangeTombstoneMarker(new RangeTombstoneMarkerImplementation(marker));
        }
    }

    private void schedulePersist()
    {
        // Add task listener to persist Watermark on task success
        TaskContext.get().addTaskCompletionListener(context -> {
            if (context.isCompleted() && context.fetchFailed().isEmpty())
            {
                LOGGER.info("Persisting Watermark on task completion partitionId={}", partitionId);
                // Once we have read all CommitLogs we can persist the watermark state
                watermarker.persist(offsetFilter != null ? offsetFilter.maxAgeMicros() : null);
            }
            else
            {
                LOGGER.warn("Not persisting Watermark due to task failure partitionId={}",
                            partitionId, context.fetchFailed().get());
            }
        });
    }

    /**
     * Get rid of invalid updates from the updates
     *
     * @param updates, a collection of PartitionUpdateWrappers
     * @return a new updates without invalid updates
     */
    private Collection<PartitionUpdateWrapper> filterValidUpdates(Collection<PartitionUpdateWrapper> updates)
    {
        // Only filter if it demands more than 1 replicas to compact
        if (minimumReplicasPerMutation == 1 || updates.isEmpty())
        {
            return updates;
        }

        Map<PartitionUpdateWrapper, List<PartitionUpdateWrapper>> replicaCopies = updates.stream()
                .collect(Collectors.groupingBy(update -> update, Collectors.toList()));

        return replicaCopies.values().stream()
                .filter(this::filter)          // Discard PartitionUpdate without enough replicas
                .map(update -> update.get(0))  // Deduplicate the valid updates to just singe copy
                .collect(Collectors.toList());
    }

    private boolean filter(List<PartitionUpdateWrapper> updates)
    {
        return filter(updates, minimumReplicasPerMutation, watermarker, stats);
    }

    static boolean filter(List<PartitionUpdateWrapper> updates,
                          int minimumReplicasPerMutation,
                          Watermarker watermarker,
                          Stats stats)
    {
        if (updates.isEmpty())
        {
            throw new IllegalStateException("Should not received empty list of updates");
        }

        PartitionUpdateWrapper update = updates.get(0);
        PartitionUpdate partitionUpdate = update.partitionUpdate();
        int numReplicas = updates.size() + watermarker.replicaCount(update);

        if (numReplicas < minimumReplicasPerMutation)
        {
            // Insufficient replica copies to publish, so record replica count and handle on subsequent round
            LOGGER.warn("Ignore the partition update (partition key: '{}') for this batch "
                      + "due to insufficient replicas received. {} required {} received.",
                        partitionUpdate != null ? partitionUpdate.partitionKey() : "unknown",
                        minimumReplicasPerMutation, numReplicas);
            watermarker.recordReplicaCount(update, numReplicas);
            stats.insufficientReplicas(update, updates.size(), minimumReplicasPerMutation);
            return false;
        }

        // Sufficient Replica Copies to Publish

        if (updates.stream().anyMatch(watermarker::seenBefore))
        {
            // Mutation previously marked as late, now we have sufficient replica copies to publish,
            // so clear watermark and publish now
            LOGGER.info("Achieved consistency level for late partition update (partition key: '{}'). {} received.",
                        partitionUpdate != null ? partitionUpdate.partitionKey() : "unknown", numReplicas);
            watermarker.untrackReplicaCount(update);
            stats.lateMutationPublished(update);
            return true;
        }

        // We haven't seen this mutation before and achieved CL, so publish
        stats.publishedMutation(update);
        return true;
    }

    public static class CdcScanner implements ISSTableScanner
    {
        final TableMetadata tableMetadata;
        final PartitionUpdate update;
        UnfilteredRowIterator it;

        public CdcScanner(TableMetadata tableMetadata, PartitionUpdate update)
        {
            this.tableMetadata = tableMetadata;
            this.update = update;
            this.it = update.unfilteredIterator();
        }

        @Override
        public long getLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            return 0;
        }

        @Override
        public long getBytesScanned()
        {
            return 0;
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return it != null;
        }

        @Override
        public UnfilteredRowIterator next()
        {
            UnfilteredRowIterator result = it;
            it = null;
            return result;
        }
    }
}
