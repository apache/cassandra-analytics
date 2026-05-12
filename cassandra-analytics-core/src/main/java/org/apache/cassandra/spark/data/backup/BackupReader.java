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
    /**
     * Populates the reader's per-(cluster, keyspace, table) cache by walking the backup manifest.
     *
     * @param clusterName logical cluster identity (UUID or human-readable name)
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     */
    void initializeSSTableInfoCache(String clusterName, String keyspace, String table, String datacenter)
        throws IllegalArgumentException;

    /**
     * Lists Cassandra instances present in the backup for the given dataset slice.
     *
     * @param clusterName logical cluster identity
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     * @return Cassandra instances contributing to this slice
     */
    List<CassandraInstance> instances(String clusterName, String keyspace, String table, String datacenter);

    /**
     * Discovers SSTables across all nodes for the given dataset slice.
     *
     * @param clusterName logical cluster identity
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     * @return map of SSTable key to per-file-type sizes
     */
    Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName, String keyspace, String table, String datacenter);

    /**
     * Discovers SSTables for a single node.
     *
     * @param clusterName logical cluster identity
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     * @param nodeId      node identifier whose SSTables to enumerate
     * @return map of SSTable key to per-file-type sizes for the given node
     */
    Map<SSTableKey, Map<FileType, Long>> sstables(String clusterName,
                                                  String keyspace,
                                                  String table,
                                                  String datacenter,
                                                  String nodeId);

    /**
     * Buffered ranged GET; completes with the full {@code [start, end]} byte range.
     *
     * @param clusterName logical cluster identity
     * @param datacenter  datacenter to read from
     * @param token       Cassandra token (used for path resolution by some implementations)
     * @param sstableKey  identifies the SSTable
     * @param fileType    which SSTable component to read
     * @param start       inclusive byte offset
     * @param end         inclusive byte offset
     * @param stats       per-task stats sink for S3 operation metrics
     * @return future completing with the read bytes
     */
    CompletableFuture<byte[]> readAsync(String clusterName,
                                        String datacenter,
                                        String token,
                                        SSTableKey sstableKey,
                                        FileType fileType,
                                        long start,
                                        long end,
                                        Stats stats);

    /**
     * Buffered read for {@linkplain FileType#isMutableMetadata mutable-metadata} components whose
     * on-disk size may change over time.
     *
     * @param clusterName  logical cluster identity
     * @param datacenter   datacenter to read from
     * @param token        Cassandra token
     * @param sstableKey   identifies the SSTable
     * @param fileType     mutable-metadata component to read
     * @param manifestSize sizing hint from the manifest (may differ from actual on-disk size)
     * @param stats        per-task stats sink
     * @return future completing with the read bytes
     */
    CompletableFuture<byte[]> readMutableMetadataAsync(String clusterName,
                                                       String datacenter,
                                                       String token,
                                                       SSTableKey sstableKey,
                                                       FileType fileType,
                                                       long manifestSize,
                                                       Stats stats);

    /**
     * Streaming ranged GET. Chunks are pushed to {@code consumer} and the future completes on termination.
     *
     * @param clusterName logical cluster identity
     * @param datacenter  datacenter to read from
     * @param token       Cassandra token
     * @param sstableKey  identifies the SSTable
     * @param fileType    SSTable component to read
     * @param start       inclusive byte offset
     * @param end         inclusive byte offset
     * @param consumer    receives streamed chunks
     * @param stats       per-task stats sink
     * @return future completing when the stream terminates
     */
    CompletableFuture<Void> getAsync(String clusterName,
                                     String datacenter,
                                     String token,
                                     SSTableKey sstableKey,
                                     FileType fileType,
                                     long start,
                                     long end,
                                     @NotNull StreamConsumer consumer,
                                     Stats stats);

    /**
     * Streaming variant of {@link #readMutableMetadataAsync}.
     *
     * @param clusterName        logical cluster identity
     * @param datacenter         datacenter to read from
     * @param token              Cassandra token
     * @param sstableKey         identifies the SSTable
     * @param fileType           mutable-metadata component to read
     * @param start              inclusive byte offset
     * @param end                inclusive byte offset
     * @param consumer           receives streamed chunks
     * @param actualSizeConsumer invoked once with the resolved size when the GET response is received
     * @param manifestSize       sizing hint from the manifest
     * @param stats              per-task stats sink
     * @return future completing when the stream terminates
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
                                                    long manifestSize,
                                                    Stats stats);

    /**
     * Checks whether a specific SSTable component exists in the backup.
     *
     * @param clusterName logical cluster identity
     * @param datacenter  datacenter to read from
     * @param token       Cassandra token
     * @param sstableKey  identifies the SSTable
     * @param fileType    SSTable component
     * @param stats       per-task stats sink
     * @return {@code true} iff the component exists in the backup
     */
    boolean exists(String clusterName,
                   String datacenter,
                   String token,
                   SSTableKey sstableKey,
                   FileType fileType,
                   Stats stats);

    /**
     * Earliest per-node snapshot epoch (seconds) across the contributing nodes.
     *
     * @param clusterName logical cluster identity
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     * @return epoch seconds of the earliest contributing snapshot
     */
    long getSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter);

    /**
     * Latest per-node snapshot epoch (seconds) across the contributing nodes.
     *
     * @param clusterName logical cluster identity
     * @param keyspace    Cassandra keyspace
     * @param table       Cassandra table
     * @param datacenter  datacenter to read from
     * @return epoch seconds of the latest contributing snapshot
     */
    long getLatestSnapshotEpochSecond(String clusterName, String keyspace, String table, String datacenter);

    /**
     * Per-cluster manifest fingerprint. Used by the data layer's intern cache to disambiguate
     * manifests that share {@code (earliestEpoch, latestEpoch)} bounds but differ in per-node
     * contributions.
     *
     * @param clusterName logical cluster identity
     * @return stable fingerprint string identifying the materialized manifest set
     */
    String getManifestFingerprint(String clusterName);

    /**
     * Returns the underlying {@link S3ClientConfig}. Used for intern-cache identity.
     *
     * @return the S3 client configuration this reader was constructed with
     */
    S3ClientConfig s3Config();

    /**
     * Returns the S3 bucket the reader was constructed against. Used for intern-cache identity.
     *
     * @return the S3 bucket name
     */
    String bucket();

    /** Closes any reader-local resources. Should be a no-op for resources owned by shared pools. */
    @Override
    void close();
}
