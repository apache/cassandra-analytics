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

package org.apache.cassandra.cdc.stats;

import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;

public interface ICdcStats extends BufferingInputStreamStats<CommitLog>
{
    ICdcStats STUB = new ICdcStats()
    {
    };

    // cdc iterator

    /**
     * On open CdcRowIterator
     */
    default void openedCdcRowIterator()
    {

    }

    /**
     * On iterate to Cdc mutation
     */
    default void nextMutation()
    {

    }

    /**
     * Closed CdcRowIterator
     *
     * @param timeOpenNanos time SparkRowIterator was open in nanos
     */
    default void closedCdcRowIterator(long timeOpenNanos)
    {

    }

    // CDC Stats

    /**
     * Difference between the time change was created and time the same was read by a spark worker
     *
     * @param latency time difference, in milli secs
     */
    default void changeReceived(String keyspace, String table, long latency)
    {
    }

    /**
     * Difference between the time change was created and time the same produced as a spark row
     *
     * @param latency time difference, in milli secs
     */
    default void changeProduced(String keyspace, String table, long latency)
    {
    }

    /**
     * Report the change is published within a single batch.
     *
     * @param keyspace
     * @param table
     */
    default void changePublished(String keyspace, String table)
    {
    }

    /**
     * Report the late change on publishing. A late change is one spans across multiple batches before publishing
     *
     * @param keyspace
     * @param table
     */
    default void lateChangePublished(String keyspace, String table)
    {
    }

    /**
     * Report the number of late mutations dropped from the Cdc state due to not receiving sufficient replica copies in the maxAgeMicros expiration window.
     *
     * @param maxAgeMicros the max allowed age mutations can be stored in the Cdc state.
     * @param count        the number of mutations dropped for being too old.
     */
    default void droppedExpiredMutations(long maxAgeMicros, int count)
    {
    }

    /**
     * An old mutation outside the maxTimestampMicros time window was read in the commit log and dropped for being too old.
     *
     * @param maxTimestampMicros timestamp in microseconds.
     */
    default void droppedOldMutation(String keyspace, String table, long maxTimestampMicros)
    {
    }

    /**
     * Report the change that is not tracked and ignored
     *
     * @param incrCount delta value to add to the count
     */
    default void untrackedChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the change that is out of the token range
     *
     * @param incrCount delta value to add to the count
     */
    default void outOfTokenRangeChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the update that has insufficient replicas to deduplicate
     *
     * @param keyspace
     * @param table
     */
    default void insufficientReplicas(String keyspace, String table)
    {
    }

    /**
     * Number of successfully read mutations
     *
     * @param incrCount delta value to add to the count
     */
    default void mutationsReadCount(long incrCount)
    {
    }

    /**
     * Deserialized size of a successfully read mutation
     *
     * @param nBytes mutation size in bytes
     */
    default void mutationsReadBytes(long nBytes)
    {
    }

    /**
     * Called when received a mutation with unknown table
     *
     * @param incrCount delta value to add to the count
     */
    default void mutationsIgnoredUnknownTableCount(long incrCount)
    {
    }

    /**
     * Called when deserialization of a mutation fails
     *
     * @param incrCount delta value to add to the count
     */
    default void mutationsDeserializeFailedCount(long incrCount)
    {
    }

    /**
     * Called when a mutation's checksum calculation fails or doesn't match with expected checksum
     *
     * @param incrCount delta value to add to the count
     */
    default void mutationsChecksumMismatchCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log file
     *
     * @param timeTaken time taken, in nano secs
     */
    default void commitLogReadTime(long timeTaken)
    {
    }

    /**
     * Number of mutations read by a micro batch
     *
     * @param count mutations count
     */
    default void mutationsReadPerBatch(long count)
    {
    }

    /**
     * Time taken by a micro batch, i.e, to read commit log files of a batch
     *
     * @param timeTaken time taken, in nano secs
     */
    default void mutationsBatchReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to aggregate and filter mutations.
     *
     * @param timeTakenNanos time taken in nanoseconds
     */
    default void mutationsFilterTime(long timeTakenNanos)
    {
    }

    /**
     * Number of unexpected commit log EOF occurrences
     *
     * @param incrCount delta value to add to the count
     */
    default void commitLogSegmentUnexpectedEndErrorCount(long incrCount)
    {
    }

    /**
     * Number of invalid mutation size occurrences
     *
     * @param incrCount delta value to add to the count
     */
    default void commitLogInvalidSizeMutationCount(long incrCount)
    {
    }

    /**
     * Number of IO exceptions seen while reading commit log header
     *
     * @param incrCount delta value to add to the count
     */
    default void commitLogHeaderReadFailureCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log's header
     *
     * @param timeTaken time taken, in nano secs
     */
    default void commitLogHeaderReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to read a commit log's segment/section
     *
     * @param timeTaken time taken, in nano secs
     */
    default void commitLogSegmentReadTime(long timeTaken)
    {
    }

    /**
     * Number of commit logs skipped
     *
     * @param incrCount delta value to add to the count
     */
    default void skippedCommitLogsCount(long incrCount)
    {
    }

    /**
     * Number of bytes skipped/seeked when reading the commit log
     *
     * @param nBytes number of bytes
     */
    default void commitLogBytesSkippedOnRead(long nBytes)
    {
    }

    /**
     * Number of commit log bytes fetched
     *
     * @param nBytes number of bytes
     */
    default void commitLogBytesFetched(long nBytes)
    {
    }

    /**
     * Failed to read a CommitLog due to corruption.
     */
    default void corruptCommitLog(CommitLog log)
    {

    }

    /**
     * @param watermarkerSize current size of watermarker
     */
    default void watermarkerSize(int watermarkerSize)
    {

    }

    /**
     * Watermarker has exceeded permitted size
     *
     * @param watermarkerSize current size of watermarker
     */
    default void watermarkerExceededSize(int watermarkerSize)
    {

    }

    /**
     * Timeout when trying to stop Cdc Consumer.
     */
    default void cdcConsumerStopTimeout()
    {

    }
}
