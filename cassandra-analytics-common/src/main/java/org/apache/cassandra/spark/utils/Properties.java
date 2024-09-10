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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.data.FileType;

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
    public static final boolean DEFAULT_CACHE_COMPRESSION_METADATA = true;
    public static final long DEFAULT_MAX_SIZE_CACHE_COMPRESSION_METADATA_BYTES = 8 * MEBI_BYTES; // 8MiB
    public static final long DEFAULT_MAX_BUFFER_SIZE = 6 * MEBI_BYTES;
    public static final long DEFAULT_CHUNK_BUFFER_SIZE = 4 * MEBI_BYTES;
    public static final Map<FileType, Long> DEFAULT_MAX_BUFFER_OVERRIDE = ImmutableMap.of(
            FileType.INDEX,            128 * KIBI_BYTES,
            FileType.SUMMARY,          256 * KIBI_BYTES,
            FileType.STATISTICS,       128 * KIBI_BYTES,
            FileType.COMPRESSION_INFO, 128 * KIBI_BYTES,
            FileType.COMMITLOG,         64 * MEBI_BYTES);
    public static final Map<FileType, Long> DEFAULT_CHUNK_BUFFER_OVERRIDE = ImmutableMap.of(
            FileType.INDEX,             32 * KIBI_BYTES,
            FileType.SUMMARY,          128 * KIBI_BYTES,
            FileType.STATISTICS,        64 * KIBI_BYTES,
            FileType.COMPRESSION_INFO,  64 * KIBI_BYTES,
            FileType.COMMITLOG,         64 * MEBI_BYTES);
    public static final int DEFAULT_TIMEOUT_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10);

    // Expansion and Shrink
    public static final Set<String> NODE_STATUS_NOT_CONSIDERED = new HashSet<>(Arrays.asList("JOINING", "DOWN"));

    private Properties()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }
}
