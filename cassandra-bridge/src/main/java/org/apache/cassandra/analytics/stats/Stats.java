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

package org.apache.cassandra.analytics.stats;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class Stats
{

    public static class DoNothingStats extends Stats
    {
        public static final DoNothingStats INSTANCE = new DoNothingStats();
    }

    public <T extends CassandraFile> BufferingInputStreamStats<T> bufferingInputStreamStats()
    {
        return BufferingInputStreamStats.doNothingStats();
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

    // PartitionSizeIterator stats

    /**
     * @param timeToOpenNanos time taken to open PartitionSizeIterator in nanos
     */
    public void openedPartitionSizeIterator(long timeToOpenNanos)
    {

    }

    /**
     * @param entry emitted single IndexEntry.
     */
    public void emitIndexEntry(IndexEntry entry)
    {

    }

    /**
     * @param timeNanos the time in nanos spent blocking waiting for next IndexEntry.
     */
    public void indexIteratorTimeBlocked(long timeNanos)
    {

    }

    /**
     * @param timeNanos time taken to for PartitionSizeIterator to run in nanos.
     */
    public void closedPartitionSizeIterator(long timeNanos)
    {

    }

    /**
     * @param timeToOpenNanos time taken to open Index.db files in nanos
     */
    public void openedIndexFiles(long timeToOpenNanos)
    {

    }

    /**
     * @param timeToOpenNanos time in nanos the IndexIterator was open for.
     */
    public void closedIndexIterator(long timeToOpenNanos)
    {

    }

    /**
     * An index reader closed with a failure.
     *
     * @param t throwable
     */
    public void indexReaderFailure(Throwable t)
    {

    }

    /**
     * An index reader closed successfully.
     *
     * @param runtimeNanos time in nanos the IndexReader was open for.
     */
    public void indexReaderFinished(long runtimeNanos)
    {

    }

    /**
     * IndexReader skipped out-of-range partition keys.
     *
     * @param skipAhead number of bytes skipped.
     */
    public void indexBytesSkipped(long skipAhead)
    {

    }

    /**
     * IndexReader read bytes.
     *
     * @param bytesRead number of bytes read.
     */
    public void indexBytesRead(long bytesRead)
    {

    }

    /**
     * When a single index entry is consumer.
     */
    public void indexEntryConsumed()
    {

    }

    /**
     * The Summary.db file was read to check start-end token range of associated Index.db file.
     *
     * @param timeNanos time taken in nanos.
     */
    public void indexSummaryFileRead(long timeNanos)
    {

    }

    /**
     * CompressionInfo.db file was read.
     *
     * @param timeNanos time taken in nanos.
     */
    public void indexCompressionFileRead(long timeNanos)
    {

    }

    /**
     * Index.db was fully read
     *
     * @param timeNanos time taken in nanos.
     */
    public void indexFileRead(long timeNanos)
    {

    }

    /**
     * When an Index.db file can be fully skipped because it does not overlap with token range.
     */
    public void indexFileSkipped()
    {

    }
}
