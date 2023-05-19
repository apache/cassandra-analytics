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

package org.apache.cassandra.spark.stats;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class Stats
{

    public static class DoNothingStats extends Stats
    {
        public static final DoNothingStats INSTANCE = new DoNothingStats();
    }

    // Spark Row Iterator

    /**
     * On open SparkRowIterator
     */
    public void openedSparkRowIterator()
    {
    }

    /**
     * On iterate to next row
     */
    public void nextRow()
    {
    }

    /**
     * Open closed SparkRowIterator
     *
     * @param timeOpenNanos time SparkRowIterator was open in nanos
     */
    public void closedSparkRowIterator(long timeOpenNanos)
    {
    }

    // Spark Cell Iterator

    /**
     * On opened SparkCellIterator
     */
    public void openedSparkCellIterator()
    {
    }

    /**
     * On iterate to next cell
     *
     * @param timeNanos time since last cell
     */
    public void nextCell(long timeNanos)
    {
    }

    /**
     * How long it took to deserialize a particular field
     *
     * @param field     CQL field
     * @param timeNanos time to deserialize in nanoseconds
     */
    public void fieldDeserialization(CqlField field, long timeNanos)
    {
    }

    /**
     * SSTableReader skipped partition in SparkCellIterator e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skippedPartitionInIterator(ByteBuffer key, BigInteger token)
    {
    }

    /**
     * On closed SparkCellIterator
     *
     * @param timeOpenNanos time SparkCellIterator was open in nanos
     */
    public void closedSparkCellIterator(long timeOpenNanos)
    {
    }

    // Partitioned Data Layer

    /**
     * Failed to open SSTable reads for a replica
     *
     * @param replica   the replica
     * @param throwable the exception
     */
    public <T extends SSTablesSupplier> void failedToOpenReplica(T replica, Throwable throwable)
    {
    }

    /**
     * Failed to open SSTableReaders for enough replicas to satisfy the consistency level
     *
     * @param primaryReplicas primary replicas selected
     * @param backupReplicas  backup replicas selected
     */
    public <T extends SSTablesSupplier> void notEnoughReplicas(Set<T> primaryReplicas, Set<T> backupReplicas)
    {
    }

    /**
     * Open SSTableReaders for enough replicas to satisfy the consistency level
     *
     * @param primaryReplicas primary replicas selected
     * @param backupReplicas  backup replicas selected
     * @param timeNanos       time in nanoseconds
     */
    public <T extends SSTablesSupplier> void openedReplicas(Set<T> primaryReplicas,
                                                            Set<T> backupReplicas,
                                                            long timeNanos)
    {
    }

    // CDC

    public void insufficientReplicas(IPartitionUpdateWrapper update, int numCopies, int minimumReplicasPerMutation)
    {
    }

    public void lateMutationPublished(IPartitionUpdateWrapper update)
    {
    }

    public void publishedMutation(IPartitionUpdateWrapper update)
    {
    }

    /**
     * The time taken to list the snapshot
     *
     * @param replica   the replica
     * @param timeNanos time in nanoseconds to list the snapshot
     */
    public <T extends SSTablesSupplier> void timeToListSnapshot(T replica, long timeNanos)
    {
    }

    // CompactionScanner

    /**
     * On opened CompactionScanner
     *
     * @param timeToOpenNanos time to open the CompactionScanner in nanos
     */
    public void openedCompactionScanner(long timeToOpenNanos)
    {
    }

    // SSTable Data.db Input Stream

    /**
     * On open an input stream on a Data.db file
     */
    public void openedDataInputStream()
    {
    }

    /**
     * On skip bytes from an input stream on a Data.db file,
     * mostly from SSTableReader skipping out of range partition
     */
    public void skippedBytes(long length)
    {
    }

    /**
     * The SSTableReader used the Summary.db/Index.db offsets to skip to the first in-range partition
     * skipping 'length' bytes before reading the Data.db file
     */
    public void skippedDataDbStartOffset(long length)
    {
    }

    /**
     * The SSTableReader used the Summary.db/Index.db offsets to close after passing the last in-range partition
     * after reading 'length' bytes from the Data.db file
     */
    public void skippedDataDbEndOffset(long length)
    {
    }

    /**
     * On read bytes from an input stream on a Data.db file
     */
    public void readBytes(int length)
    {
    }

    /**
     * On decompress bytes from an input stream on a compressed Data.db file
     *
     * @param compressedLen   compressed length in bytes
     * @param decompressedLen compressed length in bytes
     */
    public void decompressedBytes(int compressedLen, int decompressedLen)
    {
    }

    /**
     * On an exception when decompressing an SSTable e.g. if corrupted
     *
     * @param ssTable   the SSTable being decompressed
     * @param throwable the exception thrown
     */
    public void decompressionException(SSTable ssTable, Throwable throwable)
    {
    }

    /**
     * On close an input stream on a Data.db file
     */
    public void closedDataInputStream()
    {
    }

    // Partition Push-Down Filters

    /**
     * Partition key push-down filter skipped SSTable because Filter.db did not contain partition
     */
    public void missingInBloomFilter()
    {
    }

    /**
     * Partition key push-down filter skipped SSTable because Index.db did not contain partition
     */
    public void missingInIndex()
    {
    }

    // SSTable Filters

    /**
     * SSTableReader skipped SSTable e.g. because not overlaps with Spark worker token range
     *
     * @param sparkRangeFilter    spark range filter used to filter SSTable
     * @param partitionKeyFilters list of partition key filters used to filter SSTable
     * @param firstToken          SSTable first token
     * @param lastToken           SSTable last token
     */
    public void skippedSSTable(@Nullable SparkRangeFilter sparkRangeFilter,
                               @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                               @NotNull BigInteger firstToken,
                               @NotNull BigInteger lastToken)
    {
    }

    /**
     * SSTableReader skipped an SSTable because it is repaired and the Spark worker is not the primary repair replica
     *
     * @param ssTable    the SSTable being skipped
     * @param repairedAt last repair timestamp for SSTable
     */
    public void skippedRepairedSSTable(SSTable ssTable, long repairedAt)
    {
    }

    /**
     * SSTableReader skipped partition e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skippedPartition(ByteBuffer key, BigInteger token)
    {
    }

    /**
     * SSTableReader opened an SSTable
     *
     * @param timeNanos total time to open in nanoseconds
     */
    public void openedSSTable(SSTable ssTable, long timeNanos)
    {
    }

    /**
     * SSTableReader opened and deserialized a Summary.db file
     *
     * @param timeNanos total time to read in nanoseconds
     */
    public void readSummaryDb(SSTable ssTable, long timeNanos)
    {
    }

    /**
     * SSTableReader opened and deserialized a Index.db file
     *
     * @param timeNanos total time to read in nanoseconds
     */
    public void readIndexDb(SSTable ssTable, long timeNanos)
    {
    }

    /**
     * Read a single partition in the Index.db file
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void readPartitionIndexDb(ByteBuffer key, BigInteger token)
    {
    }

    /**
     * SSTableReader read next partition
     *
     * @param timeOpenNanos time in nanoseconds since last partition was read
     */
    public void nextPartition(long timeOpenNanos)
    {
    }

    /**
     * Exception thrown when reading SSTable
     *
     * @param throwable exception thrown
     * @param keyspace  keyspace
     * @param table     table
     * @param ssTable   the SSTable being read
     */
    public void corruptSSTable(Throwable throwable, String keyspace, String table, SSTable ssTable)
    {
    }

    /**
     * SSTableReader closed an SSTable
     *
     * @param timeOpenNanos time in nanoseconds SSTable was open
     */
    public void closedSSTable(long timeOpenNanos)
    {
    }

    // SSTable Input Stream

    /**
     * When {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} queue is full, usually indicating
     * job is CPU-bound and blocked on the CompactionIterator
     *
     * @param ssTable the SSTable source for this input stream
     */
    public void inputStreamQueueFull(SSTableSource<? extends SSTable> ssTable)
    {
    }

    /**
     * Failure occurred in the {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}
     *
     * @param ssTable   the SSTable source for this input stream
     * @param throwable throwable
     */
    public void inputStreamFailure(SSTableSource<? extends SSTable> ssTable, Throwable throwable)
    {
    }

    /**
     * Time the {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} spent blocking on queue
     * waiting for bytes. High time spent blocking indicates the job is network-bound, or blocked on the
     * {@link org.apache.cassandra.spark.utils.streaming.SSTableSource} to supply the bytes.
     *
     * @param ssTable the SSTable source for this input stream
     * @param nanos   time in nanoseconds
     */
    public void inputStreamTimeBlocked(SSTableSource<? extends SSTable> ssTable, long nanos)
    {
    }

    /**
     * Bytes written to {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}
     * by the {@link org.apache.cassandra.spark.utils.streaming.SSTableSource}
     *
     * @param ssTable the SSTable source for this input stream
     * @param length  number of bytes written
     */
    public void inputStreamBytesWritten(SSTableSource<? extends SSTable> ssTable, int length)
    {
    }

    /**
     * Bytes read from {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}
     *
     * @param ssTable         the SSTable source for this input stream
     * @param length          number of bytes read
     * @param queueSize       current queue size
     * @param percentComplete % completion
     */
    public void inputStreamByteRead(SSTableSource<? extends SSTable> ssTable,
                                    int length,
                                    int queueSize,
                                    int percentComplete)
    {
    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.SSTableSource} has finished writing
     * to {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} after reaching expected file length
     *
     * @param ssTable the SSTable source for this input stream
     */
    public void inputStreamEndBuffer(SSTableSource<? extends SSTable> ssTable)
    {
    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} finished and closed
     *
     * @param ssTable           the SSTable source for this input stream
     * @param runTimeNanos      total time open in nanoseconds
     * @param totalNanosBlocked total time blocked on queue waiting for bytes in nanoseconds
     */
    public void inputStreamEnd(SSTableSource<? extends SSTable> ssTable, long runTimeNanos, long totalNanosBlocked)
    {
    }

    /**
     * Called when the InputStream skips bytes
     *
     * @param ssTable         the SSTable source for this input stream
     * @param bufferedSkipped the number of bytes already buffered in memory skipped
     * @param rangeSkipped    the number of bytes skipped
     *                        by efficiently incrementing the start range for the next request
     */
    public void inputStreamBytesSkipped(SSTableSource<? extends SSTable> ssTable,
                                        long bufferedSkipped,
                                        long rangeSkipped)
    {
    }

    /**
     * Number of successfully read mutations
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsReadCount(long incrCount)
    {
    }

    /**
     * Deserialized size of a successfully read mutation
     *
     * @param nBytes mutation size in bytes
     */
    public void mutationsReadBytes(long nBytes)
    {
    }

    /**
     * Called when received a mutation with unknown table
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsIgnoredUnknownTableCount(long incrCount)
    {
    }

    /**
     * Called when deserialization of a mutation fails
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsDeserializeFailedCount(long incrCount)
    {
    }

    /**
     * Called when a mutation's checksum calculation fails or doesn't match with expected checksum
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsChecksumMismatchCount(long incrCount)
    {
    }

    /**
     * Called when a mutation doesn't have expected table id, and ignored from processing
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsIgnoredUntrackedTableCount(long incrCount)
    {
    }

    /**
     * Called when a mutation doesn't have expected token range, and ignored from processing
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsIgnoredOutOfTokenRangeCount(long incrCount)
    {
    }

    /**
     * Time taken to read a CommitLog file
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogReadTime(long timeTaken)
    {
    }

    /**
     * Number of mutations read by a micro batch
     *
     * @param count mutations count
     */
    public void mutationsReadPerBatch(long count)
    {
    }

    /**
     * Time taken by a micro batch, i.e, to read CommitLog files of a batch
     *
     * @param timeTaken time taken, in nano secs
     */
    public void mutationsBatchReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to aggregate and filter mutations
     *
     * @param timeTakenNanos time taken in nanoseconds
     */
    public void mutationsFilterTime(long timeTakenNanos)
    {
    }

    /**
     * Difference between the time mutation was created and time the same was read by a spark worker
     *
     * @param latency time difference, in milli secs
     */
    public void mutationReceivedLatency(long latency)
    {
    }

    /**
     * Difference between the time mutation was created and time the same produced as a spark row
     *
     * @param latency time difference, in milli secs
     */
    public void mutationProducedLatency(long latency)
    {
    }

    /**
     * Number of unexpected CommitLog EOF occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogSegmentUnexpectedEndErrorCount(long incrCount)
    {
    }

    /**
     * Number of invalid mutation size occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogInvalidSizeMutationCount(long incrCount)
    {
    }

    /**
     * Number of IO exceptions seen while reading CommitLog header
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogHeaderReadFailureCount(long incrCount)
    {
    }

    /**
     * Time taken to read a CommitLog's header
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogHeaderReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to read a CommitLog's segment/section
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogSegmentReadTime(long timeTaken)
    {
    }

    /**
     * Number of CommitLogs skipped
     *
     * @param incrCount delta value to add to the count
     */
    public void skippedCommitLogsCount(long incrCount)
    {
    }

    /**
     * Number of bytes skipped/seeked when reading the CommitLog
     *
     * @param nBytes number of bytes
     */
    public void commitLogBytesSkippedOnRead(long nBytes)
    {
    }

    /**
     * Number of CommitLog bytes fetched
     *
     * @param nBytes number of bytes
     */
    public void commitLogBytesFetched(long nBytes)
    {
    }

    /**
     * The {@code org.apache.cassandra.db.commitlog.BufferingCommitLogReader} dropped a mutation because the client
     * write timestamp exceeded the watermarker timestamp window
     *
     * @param maxTimestampMicros mutation max timestamp in microseconds
     */
    public void droppedOldMutation(long maxTimestampMicros)
    {
    }
}
