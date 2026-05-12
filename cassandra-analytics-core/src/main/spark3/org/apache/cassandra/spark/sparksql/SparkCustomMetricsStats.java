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

package org.apache.cassandra.spark.sparksql;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;

/**
 * Stats implementation that collects summary read duration timing for metrics reporting.
 * This extends the base Stats class and accumulates summary read timing data that can be
 * accessed by the Spark metrics system.
 * <p>
 * Uses LongAdder for optimal performance under concurrent access. LongAdder internally
 * implements thread-local accumulation cells that eliminate contention between executor
 * threads while maintaining perfect accuracy.
 */
public class SparkCustomMetricsStats extends Stats
{
    private final LongAdder totalSummaryReadDuration = new LongAdder();
    private final LongAdder totalOpenedSSTableDuration = new LongAdder();
    private final LongAdder totalCorruptSSTableCount = new LongAdder();
    private final LongAdder totalSkippedSSTableCount = new LongAdder();
    private final LongAdder totalS3HeadObjectDuration = new LongAdder();
    private final LongAdder totalS3GetObjectDuration = new LongAdder();
    private final LongAdder totalMutableMetadataDriftCount = new LongAdder();
    private final LongAdder totalMutableMetadataHeadFallbackCount = new LongAdder();

    /**
     * Get the total accumulated summary read duration in nanoseconds.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total summary read duration in nanoseconds
     */
    public long getTotalSummaryReadDurationNanos()
    {
        return totalSummaryReadDuration.sum();
    }

    /**
     * Get the total accumulated SSTable open duration in nanoseconds.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total SSTable open duration in nanoseconds
     */
    public long getTotalOpenedSSTableDurationNanos()
    {
        return totalOpenedSSTableDuration.sum();
    }

    /**
     * Get the total count of corrupt SSTables encountered.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total count of corrupt SSTables
     */
    public long getTotalCorruptSSTableCount()
    {
        return totalCorruptSSTableCount.sum();
    }

    /**
     * Get the total count of skipped SSTables.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total count of skipped SSTables
     */
    public long getTotalSkippedSSTableCount()
    {
        return totalSkippedSSTableCount.sum();
    }

    /**
     * Get the total accumulated S3 headObject duration in nanoseconds.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total S3 headObject duration in nanoseconds
     */
    public long getTotalS3HeadObjectDurationNanos()
    {
        return totalS3HeadObjectDuration.sum();
    }

    /**
     * Get the total accumulated S3 getObject duration in nanoseconds.
     * Aggregates all thread-local cells maintained by LongAdder.
     *
     * @return total S3 getObject duration in nanoseconds
     */
    public long getTotalS3GetObjectDurationNanos()
    {
        return totalS3GetObjectDuration.sum();
    }

    /**
     * Get the total count of mutable metadata size drift observations.
     *
     * @return total count of mutable metadata drift observations
     */
    public long getTotalMutableMetadataDriftCount()
    {
        return totalMutableMetadataDriftCount.sum();
    }

    /**
     * Get the total count of mutable metadata HEAD fallback reads.
     *
     * @return total count of mutable metadata HEAD fallback reads
     */
    public long getTotalMutableMetadataHeadFallbackCount()
    {
        return totalMutableMetadataHeadFallbackCount.sum();
    }

    /**
     * Accumulate summary read duration timing.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently opening SSTable readers.
     */
    @Override
    public void readSummaryDb(SSTable ssTable, long timeNanos)
    {
        totalSummaryReadDuration.add(timeNanos);
    }

    /**
     * Accumulate SSTable open duration timing.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently opening SSTables.
     */
    @Override
    public void openedSSTable(SSTable ssTable, long timeNanos)
    {
        totalOpenedSSTableDuration.add(timeNanos);
    }

    /**
     * Accumulate corrupt SSTable count.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently encountering corrupt SSTables.
     */
    @Override
    public void corruptSSTable(Throwable throwable, String keyspace, String table, SSTable ssTable)
    {
        totalCorruptSSTableCount.add(1);
    }

    /**
     * Accumulate skipped SSTable count.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently skipping SSTables.
     */
    @Override
    public void skippedSSTable(@Nullable SparkRangeFilter sparkRangeFilter,
                               @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                               @NotNull BigInteger firstToken,
                               @NotNull BigInteger lastToken)
    {
        totalSkippedSSTableCount.add(1);
    }

    /**
     * Accumulate S3 headObject call duration.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently performing S3 headObject operations.
     */
    @Override
    public void s3HeadObjectOperation(long timeNanos)
    {
        totalS3HeadObjectDuration.add(timeNanos);
    }

    /**
     * Accumulate S3 getObject call duration.
     * <p>
     * Uses LongAdder.add() which automatically distributes writes across thread-local
     * cells under contention, providing excellent performance when multiple executor
     * threads are concurrently performing S3 getObject operations.
     */
    @Override
    public void s3GetObjectOperation(long timeNanos)
    {
        totalS3GetObjectDuration.add(timeNanos);
    }

    @Override
    public void s3MutableMetadataDriftDetected(FileType fileType, long manifestSize, long actualSize)
    {
        totalMutableMetadataDriftCount.add(1);
    }

    @Override
    public void s3MutableMetadataHeadFallback(FileType fileType)
    {
        totalMutableMetadataHeadFallbackCount.add(1);
    }

    // All other Stats methods are no-ops, pending further implementations.
}
