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
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;

public abstract class CdcStats implements ICdcStats
{
    // SSTableInputStream

    public void inputStreamEnd(CassandraFileSource<CommitLog> source, long runTimeNanos, long totalNanosBlocked)
    {

    }

    public void inputStreamEndBuffer(CassandraFileSource<CommitLog> ssTable)
    {

    }

    public void inputStreamTimeBlocked(CassandraFileSource<CommitLog> source, long nanos)
    {

    }

    public void inputStreamByteRead(CassandraFileSource<CommitLog> source, int len, int queueSize, int percentComplete)
    {

    }

    public void inputStreamFailure(CassandraFileSource<CommitLog> source, Throwable t)
    {

    }

    public void inputStreamBytesWritten(CassandraFileSource<CommitLog> source, int len)
    {

    }

    public void inputStreamBytesSkipped(CassandraFileSource<CommitLog> source, long bufferedSkipped, long rangeSkipped)
    {

    }
}
