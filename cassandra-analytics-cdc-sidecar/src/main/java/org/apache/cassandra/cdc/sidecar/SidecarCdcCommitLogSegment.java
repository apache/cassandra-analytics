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

package org.apache.cassandra.cdc.sidecar;

import java.time.Duration;

import o.a.c.sidecar.client.shaded.common.response.data.CdcSegmentInfo;
import o.a.c.sidecar.client.shaded.common.utils.HttpRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * The SidecarCdcCommitLogSegment provides a CommitLog source for reading the
 * CommitLog segments over the Sidecar HTTP API.
 */
public class SidecarCdcCommitLogSegment implements CommitLog
{
    private final CassandraInstance instance;
    private final CdcSegmentInfo segment;
    private final CassandraFileSource<CommitLog> source;
    private final ICdcStats stats;

    public SidecarCdcCommitLogSegment(SidecarCdcClient sidecar,
                                      CassandraInstance instance,
                                      CdcSegmentInfo segment,
                                      Sidecar.ClientConfig clientConfig)
    {
        this.instance = instance;
        this.segment = segment;
        this.stats = sidecar.stats;

        final SidecarCdcCommitLogSegment thisLog = this;
        this.source = new CassandraFileSource<CommitLog>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                sidecar.streamCdcCommitLogSegment(instance, segment.name, HttpRange.of(start, end), consumer);
            }

            public CommitLog cassandraFile()
            {
                return thisLog;
            }

            public long maxBufferSize()
            {
                return clientConfig.maxBufferSize();
            }

            public long chunkBufferSize()
            {
                return clientConfig.chunkBufferSize(fileType());
            }

            public FileType fileType()
            {
                return FileType.COMMITLOG;
            }

            public long size()
            {
                return segment.idx;
            }

            @Nullable
            @Override
            public Duration timeout()
            {
                final int timeout = clientConfig.timeoutSeconds();
                return timeout <= 0 ? null : Duration.ofSeconds(timeout);
            }
        };
    }

    public String name()
    {
        return segment.name;
    }

    public String path()
    {
        return "./" + segment.name;
    }

    public long maxOffset()
    {
        return segment.idx;
    }

    @Override
    public long length()
    {
        return segment.size;
    }

    @Override
    public boolean completed()
    {
        return segment.completed;
    }

    public CassandraFileSource<CommitLog> source()
    {
        return source;
    }

    public CassandraInstance instance()
    {
        return instance;
    }

    public ICdcStats stats()
    {
        return stats;
    }

    public void close()
    {

    }

    @Override
    public String toString()
    {
        return '{' +
               "\"node\": \"" + instance.nodeName() + "\"," +
               "\"dc\": \"" + instance.dataCenter() + "\"," +
               "\"token\": \"" + instance.token() + "\"," +
               "\"log\": \"" + segment.name + "\"," +
               "\"idx\": \"" + segment.idx + "\"," +
               "\"size\": \"" + segment.size + '"' +
               '}';
    }
}
