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

package org.apache.cassandra.spark.data.backup;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

import org.jetbrains.annotations.NotNull;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.S3ClientConfig;
import org.apache.cassandra.spark.data.SSTableKey;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

/**
 * Minimal in-process {@link BackupReader} for tests that don't want to depend on any
 * vendor-specific implementation. All methods return inert values; tests that need richer
 * behavior should subclass and override the relevant method(s).
 */
public class FakeBackupReader implements BackupReader
{
    private static final long serialVersionUID = 1L;

    private final S3ClientConfig s3Config;
    private final String bucket;

    public FakeBackupReader()
    {
        this(null, "fake-bucket");
    }

    public FakeBackupReader(S3ClientConfig s3Config, String bucket)
    {
        this.s3Config = s3Config;
        this.bucket = bucket;
    }

    @Override
    public void initializeSSTableInfoCache(String clusterName, String keyspace, String table, String datacenter)
        throws IllegalArgumentException
    {
    }

    @Override
    public List<CassandraInstance> instances(String clusterName, String keyspace, String table, String datacenter)
    {
        return Collections.emptyList();
    }

    @Override
    public Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName, String keyspace, String table, String datacenter)
    {
        return Collections.emptyMap();
    }

    @Override
    public Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName, String keyspace, String table,
                                                         String datacenter, String nodeId)
    {
        return Collections.emptyMap();
    }

    @Override
    public CompletableFuture<byte[]> readAsync(String clusterName, String datacenter, String token,
                                               SSTableKey sstableKey, FileType fileType, long start, long end,
                                               Stats stats)
    {
        return CompletableFuture.completedFuture(new byte[0]);
    }

    @Override
    public CompletableFuture<byte[]> readMutableMetadataAsync(String clusterName, String datacenter, String token,
                                                              SSTableKey sstableKey, FileType fileType, long manifestSize,
                                                              Stats stats)
    {
        return CompletableFuture.completedFuture(new byte[0]);
    }

    @Override
    public CompletableFuture<Void> getAsync(String clusterName, String datacenter, String token,
                                            SSTableKey sstableKey, FileType fileType, long start, long end,
                                            @NotNull StreamConsumer consumer, Stats stats)
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> getMutableMetadataAsync(String clusterName, String datacenter, String token,
                                                           SSTableKey sstableKey, FileType fileType, long start,
                                                           long end, @NotNull StreamConsumer consumer,
                                                           LongConsumer actualSizeConsumer, long manifestSize,
                                                           Stats stats)
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean exists(String clusterName, String datacenter, String token, SSTableKey sstableKey, FileType fileType,
                          Stats stats)
    {
        return false;
    }

    @Override
    public long getSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter)
    {
        return 0L;
    }

    @Override
    public long getLatestSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter)
    {
        return 0L;
    }

    @Override
    public String getManifestFingerprint(String clusterName)
    {
        return "fake-fingerprint";
    }

    @Override
    public S3ClientConfig s3Config()
    {
        return s3Config;
    }

    @Override
    public String bucket()
    {
        return bucket;
    }

    @Override
    public void close()
    {
    }
}
