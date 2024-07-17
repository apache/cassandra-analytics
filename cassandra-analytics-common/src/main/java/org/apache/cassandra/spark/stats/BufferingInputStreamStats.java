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

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;

public interface BufferingInputStreamStats<T extends CassandraFile>
{
    static <T extends CassandraFile> BufferingInputStreamStats<T> doNothingStats()
    {
        return new BufferingInputStreamStats<T>()
        {
        };
    }

    /**
     * When {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream} queue is full, usually indicating
     * job is CPU-bound and blocked on the CompactionIterator
     *
     * @param ssTable the SSTable source for this input stream
     */
    default void inputStreamQueueFull(CassandraFileSource<? extends SSTable> ssTable)
    {
    }

    /**
     * Failure occurred in the {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream}
     *
     * @param ssTable   the SSTable source for this input stream
     * @param throwable throwable
     */
    default void inputStreamFailure(CassandraFileSource<T> ssTable, Throwable throwable)
    {
    }

    /**
     * Time the {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream} spent blocking on queue
     * waiting for bytes. High time spent blocking indicates the job is network-bound, or blocked on the
     * {@link org.apache.cassandra.spark.utils.streaming.CassandraFileSource} to supply the bytes.
     *
     * @param ssTable the SSTable source for this input stream
     * @param nanos   time in nanoseconds
     */
    default void inputStreamTimeBlocked(CassandraFileSource<T> ssTable, long nanos)
    {
    }

    /**
     * Bytes written to {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream}
     * by the {@link org.apache.cassandra.spark.utils.streaming.CassandraFileSource}
     *
     * @param ssTable the SSTable source for this input stream
     * @param length  number of bytes written
     */
    default void inputStreamBytesWritten(CassandraFileSource<T> ssTable, int length)
    {
    }

    /**
     * Bytes read from {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream}
     *
     * @param ssTable         the SSTable source for this input stream
     * @param length          number of bytes read
     * @param queueSize       current queue size
     * @param percentComplete % completion
     */
    default void inputStreamByteRead(CassandraFileSource<T> ssTable,
                                     int length,
                                     int queueSize,
                                     int percentComplete)
    {
    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.CassandraFileSource} has finished writing
     * to {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream} after reaching expected file length
     *
     * @param ssTable the SSTable source for this input stream
     */
    default void inputStreamEndBuffer(CassandraFileSource<T> ssTable)
    {
    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.BufferingInputStream} finished and closed
     *
     * @param ssTable           the SSTable source for this input stream
     * @param runTimeNanos      total time open in nanoseconds
     * @param totalNanosBlocked total time blocked on queue waiting for bytes in nanoseconds
     */
    default void inputStreamEnd(CassandraFileSource<T> ssTable, long runTimeNanos, long totalNanosBlocked)
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
    default void inputStreamBytesSkipped(CassandraFileSource<T> ssTable,
                                         long bufferedSkipped,
                                         long rangeSkipped)
    {
    }
}
