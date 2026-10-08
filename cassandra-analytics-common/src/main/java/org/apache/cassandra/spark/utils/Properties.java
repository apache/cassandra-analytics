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

package org.apache.cassandra.spark.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.spark.data.FileType;

import static java.util.Map.entry;

public final class Properties
{
    private static final long KIBI_BYTES = 1024;
    private static final long MEBI_BYTES = 1024 * KIBI_BYTES;

    // Sidecar
    public static final int DEFAULT_SIDECAR_PORT = 9043;
    public static final long DEFAULT_MAX_MILLIS_TO_SLEEP = TimeUnit.SECONDS.toMillis(60);
    public static final int DEFAULT_MAX_RETRIES = 10;
    public static final long DEFAULT_MILLIS_TO_SLEEP = 500;
    public static final int DEFAULT_MAX_POOL_SIZE = 64;
    public static final long DEFAULT_MAX_BUFFER_SIZE = 6 * MEBI_BYTES;
    public static final long DEFAULT_CHUNK_BUFFER_SIZE = 4 * MEBI_BYTES;
    // Defaults for Data.db on S3-backed SSTables. Sized to balance per-GET (and SSE-KMS decrypt) overhead against
    // executor heap pressure when many overlapping SSTables stream concurrently per Spark task.
    public static final long DEFAULT_S3_DATA_CHUNK_BUFFER_SIZE = 8 * MEBI_BYTES;
    public static final long DEFAULT_S3_DATA_MAX_BUFFER_SIZE   = 32 * MEBI_BYTES;
    public static final Map<FileType, Long> DEFAULT_MAX_BUFFER_OVERRIDE = Map.ofEntries(
    entry(FileType.INDEX,            128 * KIBI_BYTES),
    entry(FileType.SUMMARY,          256 * KIBI_BYTES),
    entry(FileType.STATISTICS,       128 * KIBI_BYTES),
    entry(FileType.COMPRESSION_INFO, 128 * KIBI_BYTES),
    entry(FileType.COMMITLOG,         64 * MEBI_BYTES),
    entry(FileType.PARTITIONS_INDEX,  128 * KIBI_BYTES),
    entry(FileType.ROWS_INDEX,        128 * KIBI_BYTES));
    // According to https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#partition-index
    // BTI partition and row index page size equals 4096 bytes.
    public static final Map<FileType, Long> DEFAULT_CHUNK_BUFFER_OVERRIDE = Map.ofEntries(
    entry(FileType.INDEX,             32 * KIBI_BYTES),
    entry(FileType.SUMMARY,          128 * KIBI_BYTES),
    entry(FileType.STATISTICS,        64 * KIBI_BYTES),
    entry(FileType.COMPRESSION_INFO,  64 * KIBI_BYTES),
    entry(FileType.COMMITLOG,         64 * MEBI_BYTES),
    entry(FileType.PARTITIONS_INDEX,  4 * KIBI_BYTES),
    entry(FileType.ROWS_INDEX,        4 * KIBI_BYTES));
    public static final int DEFAULT_TIMEOUT_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10);

    // Expansion and Shrink
    public static final Set<String> NODE_STATUS_NOT_CONSIDERED = new HashSet<>(Arrays.asList("JOINING", "DOWN"));

    private Properties()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }
}
