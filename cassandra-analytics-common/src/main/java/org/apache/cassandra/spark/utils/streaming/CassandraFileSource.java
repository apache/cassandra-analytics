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

package org.apache.cassandra.spark.utils.streaming;

import java.time.Duration;

import org.apache.cassandra.spark.data.FileType;
import org.jetbrains.annotations.Nullable;

/**
 * CassandraFileSource to asynchronously provide bytes to StreamConsumer when requested
 *
 * @param <T> CassandraFile type
 */
public interface CassandraFileSource<T extends CassandraFile>
{
    long DEFAULT_MAX_BUFFER_SIZE = 6291460L;
    long DEFAULT_CHUNK_BUFFER_SIZE = 4194300L;

    /**
     * Asynchronously request bytes for the SSTable file component in the range start-end, and pass on to the StreamConsumer when available.
     * The start-end range is inclusive.
     *
     * @param start    the start of the bytes range
     * @param end      the end of the bytes range
     * @param consumer the StreamConsumer to return the bytes to when the request is complete
     */
    void request(long start, long end, StreamConsumer consumer);

    /**
     * @return CassandraFile this source refers to
     */
    T cassandraFile();

    /**
     * @return the SSTable file component type this source refers to
     */
    FileType fileType();

    /**
     * The total size in bytes of the SSTable file component
     *
     * @return the file size, in bytes
     */
    long size();

    /**
     * @return the max bytes the {@code org.apache.cassandra.spark.utils.streaming.BufferingInputStream} can buffer at one time
     */
    default long maxBufferSize()
    {
        return DEFAULT_MAX_BUFFER_SIZE;
    }

    /**
     * @return the chunk size in bytes requested when {@link CassandraFileSource#request(long, long, StreamConsumer)} is called
     */
    default long chunkBufferSize()
    {
        return DEFAULT_CHUNK_BUFFER_SIZE;
    }

    /**
     * For CommitLogs we may only need to read the header to determine if we can skip or not
     * Override this value to reduce bytes requested in first request to read the header.
     * NOTE: this is a best effort to reduce wastefully reading more bytes than the header, but
     * the header might be variable length (e.g. for encrypted CommitLogs) in which case the header may be larger.
     *
     * @return the initial header size in bytes
     */
    default long headerChunkSize()
    {
        return chunkBufferSize();
    }

    /**
     * @return the number of seconds with no activity before timing out the InputStream, null to disable timeouts
     */
    @Nullable
    default Duration timeout()
    {
        return null;  // Disabled by default
    }
}
