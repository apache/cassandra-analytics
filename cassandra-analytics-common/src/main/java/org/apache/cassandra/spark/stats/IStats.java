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

import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;

/**
 * Generic Stats interface that works across all CassandraFile FileTypes.
 *
 * @param <T>
 */
public interface IStats<T extends CassandraFile>
{
    default void inputStreamEnd(CassandraFileSource<T> source, long runTimeNanos, long totalNanosBlocked)
    {
    }

    default void inputStreamEndBuffer(CassandraFileSource<T> ssTable)
    {
    }

    default void inputStreamTimeBlocked(CassandraFileSource<T> source, long nanos)
    {
    }

    default void inputStreamByteRead(CassandraFileSource<T> source, int len, int queueSize, int percentComplete)
    {
    }

    default void inputStreamFailure(CassandraFileSource<T> source, Throwable t)
    {
    }

    default void inputStreamBytesWritten(CassandraFileSource<T> ssTable, int len)
    {
    }

    default void inputStreamBytesSkipped(CassandraFileSource<T> source, long bufferedSkipped, long rangeSkipped)
    {
    }
}
