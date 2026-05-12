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

import java.io.Serializable;
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
 * Pluggable backup-reader API. Implementations encapsulate the format-specific knowledge for a
 * given backup implementation. Concrete factories are registered with {@link BackupReaderRegistry} on the
 * driver and selected via the {@code backupReaderType} option; the data layer programs strictly
 * against this interface.
 *
 * <p>Implementations are {@link Serializable} so the data layer can ship a configured reader to
 * executors inside the Spark task closure, and {@link AutoCloseable} so the data layer can
 * release reader-local resources on shutdown.
 */
public interface BackupReader extends Serializable, AutoCloseable
{
    /** Replaces the reader's stats sink. Called per task on executors. */
    void setStats(Stats stats);

    /** Populates the reader's per-(cluster, keyspace, table) cache by walking the backup manifest. */
    void initializeSSTableInfoCache(String clusterName, String keyspace, String table, String datacenter)
        throws IllegalArgumentException;

    /** Lists Cassandra instances present in the backup for the given dataset slice. */
    List<CassandraInstance> instances(String clusterName, String keyspace, String table, String datacenter);

    /** Discovers SSTables across all nodes for the given dataset slice. */
    Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName, String keyspace, String table, String datacenter);

    /** Discovers SSTables for a single node. */
    Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName,
                                                  String keyspace,
                                                  String table,
                                                  String datacenter,
                                                  String nodeId);

    /** Buffered ranged GET; completes with the full {@code [start, end]} byte range. */
    CompletableFuture<byte[]> readAsync(String clusterName,
                                        String datacenter,
                                        String token,
                                        SSTableKey sstableKey,
                                        FileType fileType,
                                        long start,
                                        long end);

    /**
     * Buffered read for {@linkplain FileType#isMutableMetadata mutable-metadata} components whose
     * on-disk size may change over time. {@code manifestSize} is a sizing hint.
     */
    CompletableFuture<byte[]> readMutableMetadataAsync(String clusterName,
                                                       String datacenter,
                                                       String token,
                                                       SSTableKey sstableKey,
                                                       FileType fileType,
                                                       long manifestSize);

    /** Streaming ranged GET. Chunks are pushed to {@code consumer} and the future completes on termination. */
    CompletableFuture<Void> getAsync(String clusterName,
                                     String datacenter,
                                     String token,
                                     SSTableKey sstableKey,
                                     FileType fileType,
                                     long start,
                                     long end,
                                     @NotNull StreamConsumer consumer);

    /**
     * Streaming variant of {@link #readMutableMetadataAsync}. {@code actualSizeConsumer} is invoked
     * once with the resolved size when the GET response is received.
     */
    CompletableFuture<Void> getMutableMetadataAsync(String clusterName,
                                                    String datacenter,
                                                    String token,
                                                    SSTableKey sstableKey,
                                                    FileType fileType,
                                                    long start,
                                                    long end,
                                                    @NotNull StreamConsumer consumer,
                                                    LongConsumer actualSizeConsumer,
                                                    long manifestSize);

    /** Checks whether a specific SSTable component exists in the backup. */
    boolean exists(String clusterName,
                   String datacenter,
                   String token,
                   SSTableKey sstableKey,
                   FileType fileType);

    /** Earliest per-node snapshot epoch (seconds) across the contributing nodes. */
    long getSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter);

    /** Latest per-node snapshot epoch (seconds) across the contributing nodes. */
    long getLatestSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter);

    /**
     * Per-cluster manifest fingerprint. Used by the data layer's intern cache to disambiguate
     * manifests that share {@code (earliestEpoch, latestEpoch)} bounds but differ in per-node
     * contributions.
     */
    String getManifestFingerprint(String clusterName);

    /** Returns the underlying {@link S3ClientConfig}. Used for intern-cache identity. */
    S3ClientConfig s3Config();

    /** Returns the S3 bucket the reader was constructed against. Used for intern-cache identity. */
    String bucket();

    /** Closes any reader-local resources. Should be a no-op for resources owned by shared pools. */
    @Override
    void close();
}
