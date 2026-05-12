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

package org.apache.cassandra.spark.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.clients.ExecutorHolder;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.backup.BackupReader;
import org.apache.cassandra.spark.data.backup.BackupReaderRegistry;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.cassandra.spark.sparksql.SnapshotTimestampDecorator;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.sparksql.SparkCustomMetricsStats;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.S3SnapshotTimeProvider;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.apache.cassandra.spark.common.S3SizingFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.ShutdownHookManager;

import org.apache.commons.lang.NotImplementedException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.OptionalLong;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.utils.RangeUtils;

/**
 * S3-backed CassandraDataLayer. The concrete backup format is provided by a pluggable
 * {@link BackupReader} resolved via {@link BackupReaderRegistry} using the
 * {@code backupReaderType} option (no default; callers must register a factory).
 * <p>
 * Assumes that Murmur3Partitioner is used. The backup reader is expected to return a list of
 * Cassandra instances per individual vnode.
 * <p>
 **/
public class S3CassandraDataLayer extends PartitionedDataLayer implements Serializable
{
    private static final long serialVersionUID = 1997L;

    private static final Logger LOGGER = LoggerFactory.getLogger(S3CassandraDataLayer.class);

    /**
     * JVM-wide intern cache that canonicalizes {@link BackupReader} instances per executor, so
     * all tasks reading the same manifest+S3 identity share a single reader (and its
     * implementation-specific cache, e.g. {@code sstableInfoCache}) instead of one copy per
     * deserialized task.
     * <p>
     * Key: {@code (cluster, keyspace, table, datacenter, earliestEpoch, latestEpoch,
     * manifestFingerprint, s3Region, s3Bucket, s3EndpointOverride, s3CredentialsFingerprint,
     * s3HttpMaxConcurrency)}. The fingerprint (SHA-256 over sorted {@code (nodeId, epoch)}
     * pairs) is the authoritative manifest identity — without it, two manifest sets sharing
     * the same {@code (min, max)} epochs alias and silently read stale SSTables (real failure
     * mode with 3+ nodes when a middle node rolls independently). S3 identity fields prevent a
     * caller with a different {@code s3Config} from reading through the wrong endpoint.
     * <p>
     * Credentials in the key isolate IAM principals and force a fresh reader after static-key
     * rotation. For prod (EMR instance role / IRSA / IMDSv2) the access keys are null and the
     * fingerprint collapses to a constant; STS rotation happens inside the SDK and does not
     * invalidate the key.
     * <p>
     * Values are weak ({@link CacheBuilder#weakValues()}), so canonical readers are GC'd once
     * no layer references them. {@link Cache#get(Object, java.util.concurrent.Callable)} is
     * the atomic install-or-return primitive and pins the returned value across the call.
     * <p>
     * Caveat: {@link BackupReader#setStats} mutates a per-reader field, so the most recent
     * task's {@code Stats} wins during overlap; a task-scoped Stats refactor is a follow-up.
     */
    private static final class ReaderInternCache
    {
        // weakValues: entries auto-evict once no layer references the canonical reader.
        // Reachability is the correct lifecycle signal here; do not add time-based eviction.
        private static final Cache<Key, BackupReader> CACHE =
            CacheBuilder.newBuilder().weakValues().build();

        private static BackupReader canonicalize(String clusterName,
                                                 String keyspace,
                                                 String table,
                                                 String datacenter,
                                                 long earliestSnapshotEpochSecond,
                                                 long latestSnapshotEpochSecond,
                                                 @NotNull BackupReader fresh)
        {
            // Bypass when manifest identity isn't fully materialized: production sets all three
            // (both epochs and fingerprint) inside initializeS3BackupReader. Hitting any of
            // these branches means we'd otherwise install a partially-keyed entry.
            if (earliestSnapshotEpochSecond <= 0 || latestSnapshotEpochSecond <= 0)
            {
                return fresh;
            }
            S3ClientConfig fingerprintConfig = fresh.s3Config();
            String fingerprintBucket = fresh.bucket();
            if (fingerprintConfig == null || fingerprintBucket == null)
            {
                // Mock readers in reflection-driven tests land here.
                return fresh;
            }
            String manifestFingerprint = fresh.getManifestFingerprint(clusterName);
            if (manifestFingerprint == null || manifestFingerprint.isEmpty())
            {
                return fresh;
            }

            Key key = Key.from(clusterName, keyspace, table, datacenter,
                               earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                               manifestFingerprint,
                               fingerprintConfig, fingerprintBucket);

            try
            {
                // Cache.get(key, loader) is atomic install-or-return; losing-candidate fresh
                // readers never publish and are GC-eligible immediately on return.
                BackupReader canonical = CACHE.get(key, () -> {
                    LOGGER.info("ReaderInternCache: installed canonical BackupReader "
                                + "cluster={} keyspace={} table={} datacenter={} earliestEpoch={} latestEpoch={} "
                                + "manifestFingerprint={} region={} bucket={} endpoint={} maxConcurrency={} identity={}",
                                clusterName, keyspace, table, datacenter,
                                earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                                manifestFingerprint,
                                fingerprintConfig.s3Region(), fingerprintBucket,
                                fingerprintConfig.s3EndpointOverride(),
                                fingerprintConfig.s3HttpMaxConcurrency(),
                                System.identityHashCode(fresh));
                    return fresh;
                });

                if (canonical != fresh)
                {
                    LOGGER.debug("ReaderInternCache: reused canonical BackupReader "
                                 + "cluster={} keyspace={} table={} datacenter={} earliestEpoch={} latestEpoch={} "
                                 + "manifestFingerprint={} region={} bucket={} canonicalIdentity={} discardedFreshIdentity={}",
                                 clusterName, keyspace, table, datacenter,
                                 earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                                 manifestFingerprint,
                                 fingerprintConfig.s3Region(), fingerprintBucket,
                                 System.identityHashCode(canonical), System.identityHashCode(fresh));

                    // Defense in depth against a future Key regression that aliases buckets.
                    if (!fingerprintBucket.equals(canonical.bucket()))
                    {
                        LOGGER.error("ReaderInternCache: bucket mismatch on canonical reader for "
                                     + "key={}. Canonical bucket={} fresh bucket={}. Replacing canonical "
                                     + "with fresh reader to avoid wrong-bucket reads.",
                                     key, canonical.bucket(), fingerprintBucket);
                        CACHE.put(key, fresh);
                        return fresh;
                    }
                }
                return canonical;
            }
            catch (java.util.concurrent.ExecutionException e)
            {
                // Loader does not throw checked exceptions; unreachable today.
                throw new RuntimeException("ReaderInternCache loader unexpectedly threw", e.getCause());
            }
        }

        @VisibleForTesting
        static void clearForTesting()
        {
            CACHE.invalidateAll();
            // Drain weak-ref eviction queue so sizeForTesting() is stable.
            CACHE.cleanUp();
        }

        @VisibleForTesting
        static long sizeForTesting()
        {
            CACHE.cleanUp();
            return CACHE.size();
        }

        private static final class Key
        {
            private final String clusterName;
            private final String keyspace;
            private final String table;
            private final String datacenter;
            private final long earliestSnapshotEpochSecond;
            private final long latestSnapshotEpochSecond;
            // SHA-256 over sorted (nodeId, autosnapEpoch) pairs. Disambiguates manifest sets
            // that share the same (min, max) epochs but differ on a middle node's rotation.
            private final String manifestFingerprint;
            private final String s3Region;
            private final String s3Bucket;
            @Nullable
            private final String s3EndpointOverride;
            // "<accessKey>|<secretHash>", mirroring S3ClientCache.getCacheKey. Null/empty
            // access keys normalize to "default", empty secrets to hash 0; raw secret never
            // enters the key. Prod (EMR/IRSA/IMDSv2) collapses to a constant "default|0".
            private final String s3CredentialsFingerprint;
            private final int s3HttpMaxConcurrency;

            private Key(String clusterName, String keyspace, String table, String datacenter,
                        long earliestSnapshotEpochSecond, long latestSnapshotEpochSecond,
                        String manifestFingerprint,
                        String s3Region, String s3Bucket, @Nullable String s3EndpointOverride,
                        String s3CredentialsFingerprint, int s3HttpMaxConcurrency)
            {
                this.clusterName = clusterName;
                this.keyspace = keyspace;
                this.table = table;
                this.datacenter = datacenter;
                this.earliestSnapshotEpochSecond = earliestSnapshotEpochSecond;
                this.latestSnapshotEpochSecond = latestSnapshotEpochSecond;
                this.manifestFingerprint = manifestFingerprint;
                this.s3Region = s3Region;
                this.s3Bucket = s3Bucket;
                this.s3EndpointOverride = s3EndpointOverride;
                this.s3CredentialsFingerprint = s3CredentialsFingerprint;
                this.s3HttpMaxConcurrency = s3HttpMaxConcurrency;
            }

            static Key from(String clusterName, String keyspace, String table, String datacenter,
                            long earliestSnapshotEpochSecond, long latestSnapshotEpochSecond,
                            String manifestFingerprint,
                            S3ClientConfig s3Config, String bucket)
            {
                return new Key(clusterName, keyspace, table, datacenter,
                               earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                               manifestFingerprint,
                               s3Config.s3Region(), bucket, s3Config.s3EndpointOverride(),
                               credentialsFingerprint(s3Config),
                               s3Config.s3HttpMaxConcurrency());
            }

            // Mirrors S3ClientCache.getCacheKey credential portion: accessKey|secretHash.
            private static String credentialsFingerprint(S3ClientConfig s3Config)
            {
                String accessKey = s3Config.s3AccessKeyId();
                String secret = s3Config.s3SecretAccessKey();
                String normalizedAccessKey = (accessKey != null && !accessKey.isEmpty()) ? accessKey : "default";
                int secretHash = (secret != null && !secret.isEmpty()) ? secret.hashCode() : 0;
                return normalizedAccessKey + "|" + secretHash;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (!(o instanceof Key))
                {
                    return false;
                }
                Key other = (Key) o;
                return earliestSnapshotEpochSecond == other.earliestSnapshotEpochSecond
                       && latestSnapshotEpochSecond == other.latestSnapshotEpochSecond
                       && s3HttpMaxConcurrency == other.s3HttpMaxConcurrency
                       && Objects.equals(clusterName, other.clusterName)
                       && Objects.equals(keyspace, other.keyspace)
                       && Objects.equals(table, other.table)
                       && Objects.equals(datacenter, other.datacenter)
                       && Objects.equals(manifestFingerprint, other.manifestFingerprint)
                       && Objects.equals(s3Region, other.s3Region)
                       && Objects.equals(s3Bucket, other.s3Bucket)
                       && Objects.equals(s3EndpointOverride, other.s3EndpointOverride)
                       && Objects.equals(s3CredentialsFingerprint, other.s3CredentialsFingerprint);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(clusterName, keyspace, table, datacenter,
                                    earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                                    manifestFingerprint,
                                    s3Region, s3Bucket, s3EndpointOverride,
                                    s3CredentialsFingerprint, s3HttpMaxConcurrency);
            }

            @Override
            public String toString()
            {
                // Error-logging only; raw credentials never appear (already a hash).
                return "ReaderInternCache.Key{cluster=" + clusterName
                       + " keyspace=" + keyspace
                       + " table=" + table
                       + " dc=" + datacenter
                       + " earliestEpoch=" + earliestSnapshotEpochSecond
                       + " latestEpoch=" + latestSnapshotEpochSecond
                       + " manifestFingerprint=" + manifestFingerprint
                       + " region=" + s3Region
                       + " bucket=" + s3Bucket
                       + " endpoint=" + s3EndpointOverride
                       + " credsFingerprint=" + s3CredentialsFingerprint
                       + " maxConcurrency=" + s3HttpMaxConcurrency
                       + "}";
            }
        }
    }

    /**
     * Test-only: clear the JVM-wide reader intern cache between tests. Production must never
     * call this — it will not free memory (canonical readers stay referenced by live layers)
     * and the next deserialization will install a duplicate.
     */
    @VisibleForTesting
    public static void clearReaderInternCacheForTesting()
    {
        ReaderInternCache.clearForTesting();
    }

    @VisibleForTesting
    public static long readerInternCacheSizeForTesting()
    {
        return ReaderInternCache.sizeForTesting();
    }

    /**
     * Test-only entry to {@link ReaderInternCache#canonicalize}, bypassing layer construction.
     * Layer constructors register a Spark shutdown hook that pins the layer (and reader) for
     * JVM lifetime, which would defeat weak-value GC assertions.
     */
    @VisibleForTesting
    public static BackupReader canonicalizeForTesting(String clusterName,
                                                      String keyspace,
                                                      String table,
                                                      String datacenter,
                                                      long earliestSnapshotEpochSecond,
                                                      long latestSnapshotEpochSecond,
                                                      BackupReader fresh)
    {
        return ReaderInternCache.canonicalize(clusterName, keyspace, table, datacenter,
                                              earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                                              fresh);
    }

    private String clusterName;
    private String keyspace;
    private String table;
    private String s3Region;
    private String s3Bucket;
    @Nullable
    private String s3EndpointOverride;
    @Nullable
    private String s3AccessKeyId;
    @Nullable
    private String s3SecretAccessKey;

    protected transient CassandraBridge bridge;

    private CassandraRing ring;
    private TokenPartitioner tokenPartitioner;
    protected CqlTable cqlTable;

    @Nullable
    protected String lastModifiedTimestampField;
    @Nullable
    protected String snapshotTimestampField;
    protected List<SchemaFeature> requestedFeatures;
    protected int sstableS3ReadTimeoutSeconds;
    protected long latestSnapshotEpochSecond;

    // Data.db ranged-GET buffer sizes. Carried as instance fields (not just on S3DataSourceClientConfig)
    // so they survive Spark serialization to executors.
    private long dataChunkBufferSize = org.apache.cassandra.spark.utils.Properties.DEFAULT_S3_DATA_CHUNK_BUFFER_SIZE;
    private long dataMaxBufferSize   = org.apache.cassandra.spark.utils.Properties.DEFAULT_S3_DATA_MAX_BUFFER_SIZE;

    // Switch for Data.db ranged-GET delivery. Default true: Data.db reads use the
    // AsyncResponseTransformer.toPublisher() streaming path. When false,
    // AsyncResponseTransformer.toBytes() is used (single materialized byte[] per ranged GET).
    // Non-Data file types and mutable metadata reads always use their existing paths regardless of this
    // flag. Carried as instance field so it survives Spark serialization to executors.
    private boolean sstableDataPublisherReadEnabled = true;

    // SSTable metadata cache sizes forwarded to {@code SSTableCache} via JVM sysprops. Carried here so
    // executor-side deserialization can re-apply them; defaults mirror S3DataSourceClientConfig.
    private int sstableCacheSummaryMaxEntries          = 32768;
    private int sstableCacheIndexMaxEntries            = 16384;
    private int sstableCacheStatsMaxEntries            = 16384;
    private int sstableCacheFilterMaxEntries           = 16384;
    private int sstableCacheCompressionInfoMaxEntries  = 16384;

    private boolean sstableTokenIndexEnabled = false;
    private int sstableTokenIndexPrebuildPartitions = 0;
    private int sstableTokenIndexPrebuildPerTaskConcurrency = 4;
    private transient SSTableTokenIndex sstableTokenIndex;

    private BackupReader s3BackupReader = null;
    protected transient TimeProvider timeProvider;
    private transient Stats stats;
    private S3DataSourceClientConfig s3Config;
    // Selects the BackupReaderFactory. Non-final so readObject can reassign it on executors.
    private String backupReaderType;

    public S3CassandraDataLayer(@NotNull S3DataSourceClientConfig config)
    {
        super(config.consistencyLevel(), config.datacenter());
        this.s3Config = config;
        this.clusterName = config.clusterName();
        this.keyspace = config.keyspace();
        this.table = config.table();
        this.s3Region = config.s3Region();
        this.s3Bucket = config.s3Bucket();
        this.s3EndpointOverride = config.s3EndpointOverride();
        this.s3AccessKeyId = config.s3AccessKeyId();
        this.s3SecretAccessKey = config.s3SecretAccessKey();
        this.sstableS3ReadTimeoutSeconds = config.sstableS3ReadTimeoutSeconds();
        this.dataChunkBufferSize = config.s3DataChunkBufferSize();
        this.dataMaxBufferSize   = config.s3DataMaxBufferSize();
        this.sstableDataPublisherReadEnabled = config.sstableDataPublisherReadEnabled();
        this.sstableCacheSummaryMaxEntries          = config.sstableCacheSummaryMaxEntries();
        this.sstableCacheIndexMaxEntries            = config.sstableCacheIndexMaxEntries();
        this.sstableCacheStatsMaxEntries            = config.sstableCacheStatsMaxEntries();
        this.sstableCacheFilterMaxEntries           = config.sstableCacheFilterMaxEntries();
        this.sstableCacheCompressionInfoMaxEntries  = config.sstableCacheCompressionInfoMaxEntries();
        this.sstableTokenIndexEnabled = config.sstableTokenIndexEnabled();
        this.sstableTokenIndexPrebuildPartitions = config.sstableTokenIndexPrebuildPartitions();
        this.sstableTokenIndexPrebuildPerTaskConcurrency = config.sstableTokenIndexPrebuildPerTaskConcurrency();
        this.backupReaderType = config.backupReaderType();

        // Driver-side apply; executor side is covered from readObject / Kryo Serializer.read.
        applySSTableCacheSystemProperties();

        LOGGER.info("Initializing S3CassandraDataLayer for cluster={}, keyspace={}, table={}, "
                    + "dataChunkBufferSize={} bytes, dataMaxBufferSize={} bytes, "
                    + "sstableDataPublisherReadEnabled={}",
                    clusterName, keyspace, table, dataChunkBufferSize, dataMaxBufferSize,
                    sstableDataPublisherReadEnabled);

        // Initialize stats before initializing s3BackupReader such that stats can be passed to s3BackupReader
        this.stats = new SparkCustomMetricsStats();

        initializeS3BackupReader();

        // list Cassandra instances in S3 bucket
        final List<CassandraInstance> instances = s3BackupReader.instances(clusterName, config.keyspace(), config.table(), config.datacenter());
        // build CassandraRing and TokenPartitioner
        final Partitioner partitioner = Partitioner.Murmur3Partitioner;
        final ReplicationFactor rf = config.getParsedReplicationFactor();
        this.ring = new CassandraRing(Partitioner.Murmur3Partitioner, config.keyspace(), rf, instances);

        // Calculate effective number of cores using dynamic sizing. config.numberSplits() honors the
        // optional `number_splits` DataSource option; -1 (DEFAULT_NUM_SPLITS) falls back to the
        // (defaultParallelism, numCores) formula in TokenPartitioner.
        int effectiveNumberOfCores = getSizing(rf, config).getEffectiveNumberOfCores();
        this.tokenPartitioner = new TokenPartitioner(ring,
                                                     config.numberSplits(),
                                                     config.defaultParallelism(),
                                                     effectiveNumberOfCores);

        // build cqlTable based on tableCreateStmt and provided udts.
        this.bridge = CassandraBridgeFactory.get(config.cassandraVersion());
        this.cqlTable = bridge().buildSchema(config.tableCreateStmt(), config.keyspace(), rf, partitioner, config.parsedUdts());
        this.lastModifiedTimestampField = config.lastModifiedTimestampField();
        this.snapshotTimestampField = config.snapshotTimestampField();
        this.requestedFeatures = config.requestedFeatures();
        if (this.lastModifiedTimestampField != null)
        {
            CassandraDataLayer.aliasLastModifiedTimestamp(this.requestedFeatures, this.lastModifiedTimestampField);
        }
        final long earliestEpoch = s3BackupReader.getSnapshotEpochSecond(clusterName, config.keyspace(), config.table(), config.datacenter());
        this.latestSnapshotEpochSecond = s3BackupReader.getLatestSnapshotEpochSecond(clusterName, config.keyspace(), config.table(), config.datacenter());
        this.timeProvider = new S3SnapshotTimeProvider(earliestEpoch);
        injectSnapshotTimestamp(this.requestedFeatures, this.latestSnapshotEpochSecond, this.snapshotTimestampField);

        // Always assign s3BackupReader via the intern cache so future executor-side refactors
        // can't reintroduce per-task readers. Must run after epochs are populated above.
        this.s3BackupReader = ReaderInternCache.canonicalize(clusterName, config.keyspace(), config.table(),
                                                             config.datacenter(),
                                                             earliestEpoch, this.latestSnapshotEpochSecond,
                                                             this.s3BackupReader);

        // Register shutdown hook to clean up S3 resources
        ShutdownHookManager.addShutdownHook(org.apache.spark.util.ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY(),
                                            ScalaFunctions.wrapLambda(this::shutdownHook));
    }

    @Override
    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    // For deserialization
    @VisibleForTesting
    // CHECKSTYLE IGNORE: Constructor with many parameters
    public S3CassandraDataLayer(@NotNull final String clusterName,
                                @NotNull final String keyspace,
                                @NotNull final String table,
                                @NotNull String datacenter,
                                @NotNull String s3Region,
                                @NotNull String s3Bucket,
                                @Nullable String s3EndpointOverride,
                                @Nullable String s3AccessKeyId,
                                @Nullable String s3SecretAccessKey,
                                int sstableS3ReadTimeoutSeconds,
                                @NotNull final TokenPartitioner tokenPartitioner,
                                @NotNull CassandraVersion version,
                                @NotNull final CassandraRing ring,
                                @NotNull final CqlTable cqlTable,
                                @Nullable ConsistencyLevel consistencyLevel,
                                @Nullable String lastModifiedTimestampField,
                                @Nullable String snapshotTimestampField,
                                long latestSnapshotEpochSecond,
                                List<SchemaFeature> requestedFeatures,
                                TimeProvider timeProvider,
                                @NotNull final BackupReader s3BackupReader)
    {
        this(clusterName, keyspace, table, datacenter, s3Region, s3Bucket, s3EndpointOverride,
             s3AccessKeyId, s3SecretAccessKey, sstableS3ReadTimeoutSeconds, tokenPartitioner,
             version, ring, cqlTable, consistencyLevel, lastModifiedTimestampField,
             snapshotTimestampField, latestSnapshotEpochSecond, requestedFeatures, timeProvider,
             s3BackupReader, /* backupReaderType */ "test");
    }

    // For deserialization with backupReaderType (Kryo path)
    @VisibleForTesting
    // CHECKSTYLE IGNORE: Constructor with many parameters
    public S3CassandraDataLayer(@NotNull final String clusterName,
                                   @NotNull final String keyspace,
                                   @NotNull final String table,
                                   @NotNull String datacenter,
                                   @NotNull String s3Region,
                                   @NotNull String s3Bucket,
                                   @Nullable String s3EndpointOverride,
                                   @Nullable String s3AccessKeyId,
                                   @Nullable String s3SecretAccessKey,
                                   int sstableS3ReadTimeoutSeconds,
                                   @NotNull final TokenPartitioner tokenPartitioner,
                                   @NotNull CassandraVersion version,
                                   @NotNull final CassandraRing ring,
                                   @NotNull final CqlTable cqlTable,
                                   @Nullable ConsistencyLevel consistencyLevel,
                                   @Nullable String lastModifiedTimestampField,
                                   @Nullable String snapshotTimestampField,
                                   long latestSnapshotEpochSecond,
                                   List<SchemaFeature> requestedFeatures,
                                   TimeProvider timeProvider,
                                   @NotNull final BackupReader s3BackupReader,
                                   @NotNull String backupReaderType)
    {
        super(consistencyLevel, datacenter);

        // Initialize stats first to ensure it's never null
        this.stats = new SparkCustomMetricsStats();

        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.table = table;
        this.bridge = CassandraBridgeFactory.get(version);
        this.cqlTable = cqlTable;
        this.tokenPartitioner = tokenPartitioner;
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.s3EndpointOverride = s3EndpointOverride;
        this.s3AccessKeyId = s3AccessKeyId;
        this.s3SecretAccessKey = s3SecretAccessKey;
        this.sstableS3ReadTimeoutSeconds = sstableS3ReadTimeoutSeconds;
        this.lastModifiedTimestampField = lastModifiedTimestampField;
        this.snapshotTimestampField = snapshotTimestampField;
        this.latestSnapshotEpochSecond = latestSnapshotEpochSecond;
        this.backupReaderType = backupReaderType;
        this.requestedFeatures = requestedFeatures;
        if (lastModifiedTimestampField != null)
        {
            CassandraDataLayer.aliasLastModifiedTimestamp(this.requestedFeatures, this.lastModifiedTimestampField);
        }
        injectSnapshotTimestamp(this.requestedFeatures, this.latestSnapshotEpochSecond, this.snapshotTimestampField);
        this.ring = ring;
        this.timeProvider = timeProvider;
        // Tests pass latestSnapshotEpochSecond=0 to short-circuit canonicalize (keeps mock
        // readers isolated). Null-guard timeProvider for the same reason.
        long earliestSnapshotEpochSecond = (timeProvider != null) ? timeProvider.referenceEpochInSeconds() : 0L;
        this.s3BackupReader = ReaderInternCache.canonicalize(clusterName, keyspace, table, datacenter,
                                                             earliestSnapshotEpochSecond, latestSnapshotEpochSecond,
                                                             s3BackupReader);

        // Shared mutation under canonicalization; see ReaderInternCache javadoc caveat.
        this.s3BackupReader.setStats(this.stats);

        // No shutdown hook here: production never reaches this ctor (Spark task closures use
        // JDK readObject, which doesn't run constructors; Kryo is unused for this layer).
        // Registering a hook would pin `this` for JVM lifetime and defeat weakValues() on the
        // canonical reader. The driver-side primary ctor registers exactly one hook per JVM.
    }

    /**
     * Replaces the placeholder {@link SchemaFeatureSet#SNAPSHOT_TIMESTAMP} enum entry in the
     * requested features list with a custom {@link SchemaFeature} instance that carries the
     * actual latest snapshot epoch and column alias.
     * <p>
     * If {@code snapshotTimestampField} is null, the feature is left as-is (using its default
     * field name {@code "snapshot_timestamp"}).
     *
     * @param requestedFeatures      the mutable list of requested features
     * @param latestSnapshotEpoch    the latest autosnap epoch in seconds across all nodes
     * @param snapshotTimestampField the user-supplied column alias, or null for the default name
     */
    static void injectSnapshotTimestamp(List<SchemaFeature> requestedFeatures,
                                        long latestSnapshotEpoch,
                                        @Nullable String snapshotTimestampField)
    {
        int index = requestedFeatures.indexOf(SchemaFeatureSet.SNAPSHOT_TIMESTAMP);
        if (index < 0)
        {
            return;
        }

        final String alias = snapshotTimestampField != null
                             ? snapshotTimestampField
                             : SchemaFeatureSet.SNAPSHOT_TIMESTAMP.fieldName();

        SchemaFeature injected = new SchemaFeature()
        {
            @Override
            public String optionName()
            {
                return SchemaFeatureSet.SNAPSHOT_TIMESTAMP.optionName();
            }

            @Override
            public String fieldName()
            {
                return alias;
            }

            @Override
            public DataType fieldDataType()
            {
                return DataTypes.TimestampType;
            }

            @Override
            public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
            {
                return new SnapshotTimestampDecorator<>(builder, alias, latestSnapshotEpoch);
            }

            @Override
            public boolean fieldNullable()
            {
                return SchemaFeatureSet.SNAPSHOT_TIMESTAMP.fieldNullable();
            }
        };
        requestedFeatures.set(index, injected);
    }

    private void initializeS3BackupReader()
    {
        if (s3BackupReader == null)
        {
            this.s3BackupReader = BackupReaderRegistry.create(this.backupReaderType,
                                                              this.s3Config.toBackupReaderConfig().withStats(stats));
            this.s3BackupReader.initializeSSTableInfoCache(clusterName, keyspace, table, datacenter);
        }
    }

    @Override
    public CqlTable cqlTable()
    {
        return cqlTable;
    }

    @Override
    public TimeProvider timeProvider()
    {
        return timeProvider;
    }

    public boolean sstableTokenIndexEnabled()
    {
        return sstableTokenIndexEnabled;
    }

    public int sstableTokenIndexPrebuildPartitions(int sparkDefaultParallelism)
    {
        return s3Config.resolveSSTableTokenIndexPrebuildPartitions(sstableCountForTokenIndex(), sparkDefaultParallelism);
    }

    public int sstableTokenIndexPrebuildPerTaskConcurrency()
    {
        return sstableTokenIndexPrebuildPerTaskConcurrency;
    }

    public S3ClientConfig s3ClientConfig()
    {
        return s3Config.s3Config();
    }

    /** Returns the {@link BackupReader} type this layer was constructed with. */
    public String backupReaderType()
    {
        return backupReaderType;
    }

    public String clusterName()
    {
        return clusterName;
    }

    public String datacenter()
    {
        return datacenter;
    }

    public int sstableCountForTokenIndex()
    {
        return s3BackupReader.sstables(clusterName, keyspace, table, datacenter).size();
    }

    public List<SSTableSummaryWorkItem> sstableTokenIndexWorkItems()
    {
        Map<String, String> tokenByNode = new HashMap<>();
        for (CassandraInstance instance : s3BackupReader.instances(clusterName, keyspace, table, datacenter))
        {
            tokenByNode.putIfAbsent(instance.nodeName(), instance.token());
        }
        Set<String> activeNodeIds = new HashSet<>(tokenByNode.keySet());

        return s3BackupReader.sstables(clusterName, keyspace, table, datacenter)
                             .entrySet()
                             .stream()
                             .filter(entry -> activeNodeIds.contains(entry.getKey().getNodeId()))
                             .map(entry -> new SSTableSummaryWorkItem(entry.getKey(),
                                                                       tokenByNode.getOrDefault(entry.getKey().getNodeId(), ""),
                                                                       entry.getValue()))
                             .collect(Collectors.toList());
    }

    public void setSSTableTokenIndex(@Nullable SSTableTokenIndex sstableTokenIndex)
    {
        this.sstableTokenIndex = sstableTokenIndex;
    }

    /**
     * Lists all SSTables for the given partitionId, token range, and instance.
     * <p>
     * For now, token range filtering is not applied and happens later during SSTable reading with SparkRangeFilter.
     * @param partitionId the partition ID to list SSTables for
     * @param range       the range of tokens to filter SSTables
     * @param instance    the Cassandra instance to list SSTables for
     * @return a CompletableFuture containing a stream of SSTable objects
     */
    @Override
    public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                           @NotNull Range<BigInteger> range,
                                                           @NotNull CassandraInstance instance)
    {
        // list all Data.db files for the specific instance
        // and create an S3SSTable object per Data.db file
        String nodeName = instance.nodeName();
        TokenRange tokenRange = RangeUtils.toTokenRange(range);
        // One context per listInstance call; carries only the fields S3SSTable / S3SSTableSource need.
        S3SSTableContext context = newS3SSTableContext(sstableDataPublisherReadEnabled);
        Stream<SSTable> sstableStream = s3BackupReader.sstables(clusterName, keyspace, table, instance.dataCenter(), nodeName)
                                                      .entrySet().stream()
                                                      .filter(entry -> shouldIncludeSSTable(entry.getKey(), tokenRange))
                                                      .peek(ssTable -> LOGGER.info("Opening SSTable node={} SSTableKey={}", nodeName,
                                                                                   ssTable.getKey()))
                                                      .map(entry -> {
                                                          SSTableKey sstableKey = entry.getKey();
                                                          String sstableFileName = sstableKey.getDataFileName();
                                                          return new S3SSTable(instance.token(), sstableFileName, entry.getValue(), sstableKey,
                                                                               context);
                                                      });

        return CompletableFuture.completedFuture(sstableStream);
    }

    private boolean shouldIncludeSSTable(SSTableKey sstableKey, TokenRange tokenRange)
    {
        if (sstableTokenIndex == null)
        {
            return true;
        }
        boolean include = sstableTokenIndex.include(sstableKey, tokenRange);
        if (!include)
        {
            LOGGER.debug("Pruned SSTable by token index SSTableKey={} tokenRange={}", sstableKey, tokenRange);
        }
        return include;
    }

    @Override
    public CassandraRing ring()
    {
        return ring;
    }

    @Override
    public TokenPartitioner tokenPartitioner()
    {
        return tokenPartitioner;
    }

    @Override
    public ReplicationFactor replicationFactor(String keyspace)
    {
        return this.ring.replicationFactor();
    }

    @Override
    protected ExecutorService executorService()
    {
        return ExecutorHolder.EXECUTOR_SERVICE;
    }

    public String jobId()
    {
        throw new NotImplementedException("Cdc has not been implemented for the S3DataLayer");
    }

    /**
     * Returns the {@link Sizing} object based on the {@code sizing} option provided by the user,
     * or {@link DefaultSizing} as the default sizing
     *
     * @param replicationFactor the replication factor
     * @param options           the {@link S3DataSourceClientConfig} options
     * @return the {@link Sizing} object based on the {@code sizing} option provided by the user
     */
    protected Sizing getSizing(ReplicationFactor replicationFactor, S3DataSourceClientConfig options)
    {
        return S3SizingFactory.create(replicationFactor, options, consistencyLevel, keyspace, table, datacenter, s3BackupReader, clusterName);
    }

    /**
     * Override to provide efficient SSTable size calculation using S3 backup metadata.
     * This avoids the need to iterate through individual SSTable suppliers and directly
     * uses the cached SSTable information from S3.
     *
     * @return OptionalLong containing the total size in bytes of all SSTable Data.db files,
     * or empty if calculation fails
     */
    @Override
    public OptionalLong calculateTotalSSTableSize()
    {
        try
        {
            // Get all SSTables for this table from S3 backup reader cache
            Map<SSTableKey, Map<FileType, Long>> sstables = s3BackupReader.sstables(clusterName, keyspace, table, datacenter);

            long totalSize = 0;

            // Sum up the Data.db file sizes from all SSTables
            for (Map<FileType, Long> componentSizes : sstables.values())
            {
                Long dataFileSize = componentSizes.get(FileType.DATA);
                if (dataFileSize != null)
                {
                    totalSize += dataFileSize;
                }
            }

            return OptionalLong.of(totalSize);
        }
        catch (Exception e)
        {
            // If S3-specific calculation fails, fall back to default implementation
            return super.calculateTotalSSTableSize();
        }
    }

    /**
     * Override to provide metrics collection for S3-based Cassandra data reading.
     * `stats` is always initialized in constructors, so no null check needed.
     */
    @Override
    public Stats stats()
    {
        return stats;
    }

    /** DATA uses the configurable size; other FileTypes fall back to per-FileType then global defaults. */
    private long bufferSizeForChunk(FileType fileType)
    {
        if (fileType == FileType.DATA)
        {
            return dataChunkBufferSize;
        }
        Long override = org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_OVERRIDE.get(fileType);
        return override != null ? override : org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_SIZE;
    }

    private long bufferSizeForMax(FileType fileType)
    {
        if (fileType == FileType.DATA)
        {
            return dataMaxBufferSize;
        }
        Long override = org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_OVERRIDE.get(fileType);
        return override != null ? override : org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_SIZE;
    }

    @VisibleForTesting
    long dataChunkBufferSize()
    {
        return dataChunkBufferSize;
    }

    @VisibleForTesting
    long dataMaxBufferSize()
    {
        return dataMaxBufferSize;
    }

    @VisibleForTesting
    boolean sstableDataPublisherReadEnabled()
    {
        return sstableDataPublisherReadEnabled;
    }

    @VisibleForTesting
    void setSstableDataPublisherReadEnabledForTesting(boolean enabled)
    {
        this.sstableDataPublisherReadEnabled = enabled;
    }

    @VisibleForTesting
    BackupReader s3BackupReaderForTesting()
    {
        return s3BackupReader;
    }

    @VisibleForTesting
    void setS3BackupReaderForTesting(BackupReader reader)
    {
        this.s3BackupReader = reader;
    }

    /** Snapshot the current layer fields into an {@link S3SSTableContext}. */
    private S3SSTableContext newS3SSTableContext(boolean publisherReadEnabled)
    {
        return new S3SSTableContext(clusterName, datacenter, s3BackupReader,
                                    dataChunkBufferSize, dataMaxBufferSize,
                                    sstableS3ReadTimeoutSeconds, publisherReadEnabled, stats());
    }

    /**
     * Test-only factory mirroring {@code listInstance}'s SSTable construction. Lets tests choose the
     * captured publisher-read flag at construction time without needing to name {@link S3SSTableContext}.
     */
    @VisibleForTesting
    public S3SSTable newSSTableForTesting(String token,
                                   String fileName,
                                   Map<FileType, Long> componentSizes,
                                   SSTableKey sstableKey,
                                   boolean publisherReadEnabled)
    {
        return new S3SSTable(token, fileName, componentSizes, sstableKey,
                             newS3SSTableContext(publisherReadEnabled));
    }

    /**
     * Apply SSTable metadata cache sizes as JVM sysprops for {@code SSTableCache} to read at first load.
     * Operator-set {@code -D} flags win (existing sysprops are preserved). No-op if {@code SSTableCache}
     * has already been class-loaded in this JVM.
     */
    @VisibleForTesting
    public void applySSTableCacheSystemProperties()
    {
        setSysPropIfUnset("sbr.cache.summary.maxEntries",         String.valueOf(sstableCacheSummaryMaxEntries));
        setSysPropIfUnset("sbr.cache.index.maxEntries",           String.valueOf(sstableCacheIndexMaxEntries));
        setSysPropIfUnset("sbr.cache.stats.maxEntries",           String.valueOf(sstableCacheStatsMaxEntries));
        setSysPropIfUnset("sbr.cache.filter.maxEntries",          String.valueOf(sstableCacheFilterMaxEntries));
        setSysPropIfUnset("sbr.cache.compressionInfo.maxEntries", String.valueOf(sstableCacheCompressionInfoMaxEntries));
        // S3 backup objects are immutable for a given (path, generation), so cached metadata cannot
        // go stale within a job and TTL only forces wasteful re-fetches (extra S3 GETs + KMS
        // decrypts). Set a long TTL on every cache for the S3 path; bounded memory is still enforced
        // by maximumSize() LRU. Non-S3 readers keep upstream's 15 / 60 min TTLs.
        String s3CacheExpireMins = String.valueOf(TimeUnit.DAYS.toMinutes(1));
        setSysPropIfUnset("sbr.cache.summary.expireAfterMins",         s3CacheExpireMins);
        setSysPropIfUnset("sbr.cache.index.expireAfterMins",           s3CacheExpireMins);
        setSysPropIfUnset("sbr.cache.stats.expireAfterMins",           s3CacheExpireMins);
        setSysPropIfUnset("sbr.cache.filter.expireAfterMins",          s3CacheExpireMins);
        setSysPropIfUnset("sbr.cache.compressionInfo.expireAfterMins", s3CacheExpireMins);
    }

    private static void setSysPropIfUnset(String name, String value)
    {
        if (System.getProperty(name) == null)
        {
            System.setProperty(name, value);
        }
    }

    /**
     * Shutdown hook to clean up resources used by S3CassandraDataLayer.
     * Closes the s3BackupReader and releases all cached S3 clients.
     */
    protected void shutdownHook()
    {
        try
        {
            if (s3BackupReader != null)
            {
                s3BackupReader.close();
            }
            S3ClientCache.closeAll();
        }
        catch (Exception exception)
        {
            LOGGER.warn("Unable to close S3 resources", exception);
        }
    }

    public void close()
    {
        try
        {
            if (s3BackupReader != null)
            {
                s3BackupReader.close();
            }
            sstableTokenIndex = null;
        }
        catch (Exception exception)
        {
            LOGGER.warn("Unable to close S3 Cassandra data layer resources", exception);
        }
    }

    /**
     * Immutable bundle of fields that {@link S3SSTable} and {@link S3SSTableSource} need at runtime.
     * Carrying these in a separate object lets both classes be {@code static}, so cached SSTable
     * keys in {@code SSTableCache} no longer pin their owning {@link S3CassandraDataLayer} via a
     * synthetic outer reference.
     */
    static final class S3SSTableContext
    {
        final String clusterName;
        final String datacenter;
        final BackupReader s3BackupReader;
        final long dataChunkBufferSize;
        final long dataMaxBufferSize;
        final int sstableS3ReadTimeoutSeconds;
        final boolean sstableDataPublisherReadEnabled;
        // Stats reference (not a resolved BufferingInputStreamStats) preserves per-open resolution.
        final Stats stats;

        S3SSTableContext(String clusterName,
                         String datacenter,
                         BackupReader s3BackupReader,
                         long dataChunkBufferSize,
                         long dataMaxBufferSize,
                         int sstableS3ReadTimeoutSeconds,
                         boolean sstableDataPublisherReadEnabled,
                         Stats stats)
        {
            this.clusterName = clusterName;
            this.datacenter = datacenter;
            this.s3BackupReader = s3BackupReader;
            this.dataChunkBufferSize = dataChunkBufferSize;
            this.dataMaxBufferSize = dataMaxBufferSize;
            this.sstableS3ReadTimeoutSeconds = sstableS3ReadTimeoutSeconds;
            this.sstableDataPublisherReadEnabled = sstableDataPublisherReadEnabled;
            this.stats = stats;
        }

        long bufferSizeForChunk(FileType fileType)
        {
            if (fileType == FileType.DATA)
            {
                return dataChunkBufferSize;
            }
            Long override = org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_OVERRIDE.get(fileType);
            return override != null ? override : org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_SIZE;
        }

        long bufferSizeForMax(FileType fileType)
        {
            if (fileType == FileType.DATA)
            {
                return dataMaxBufferSize;
            }
            Long override = org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_OVERRIDE.get(fileType);
            return override != null ? override : org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_SIZE;
        }
    }

    public static class S3SSTable extends SSTable
    {
        private final String token;
        private final String fileName;
        private final Map<FileType, Long> componentSizes;
        private final SSTableKey sstableKey;
        private final S3SSTableContext context;
        private final ConcurrentMap<FileType, Long> actualComponentSizes = new ConcurrentHashMap<>();

        // Package-private: only listInstance(...) and newSSTableForTesting(...) construct these.
        S3SSTable(String token,
                  String fileName,
                  Map<FileType, Long> componentSizes,
                  SSTableKey sstableKey,
                  @NotNull S3SSTableContext context)
        {
            this.token = token;
            this.fileName = fileName;
            this.componentSizes = componentSizes;
            this.sstableKey = sstableKey;
            this.context = context;
        }

        @Nullable
        protected InputStream openInputStream(FileType fileType)
        {
            // open an InputStream on the SSTable file component
            final Long size = componentSizes.get(fileType);
            if (size == null)
            {
                // file doesn't exist
                return null;
            }
            // using the SSTableInputStream allows us to open many SSTables without OOMing
            // by buffering and requesting more on demand
            return new BufferingInputStream<>(new S3SSTableSource(this, fileType, size, context),
                                              context.stats.bufferingInputStreamStats());
        }

        @VisibleForTesting
        public CassandraFileSource<SSTable> newSourceForTesting(FileType fileType, long size)
        {
            return new S3SSTableSource(this, fileType, size, context);
        }

        /**
         * Cross-package test hook delegating to the protected {@link #openInputStream(FileType)}.
         * Production code should use the {@code BufferingInputStream} accessors on
         * {@link SSTable} (e.g. {@code openSummaryStream()}) instead.
         */
        @VisibleForTesting
        public InputStream openInputStreamForTesting(FileType fileType)
        {
            return openInputStream(fileType);
        }

        public long length(FileType fileType)
        {
            Long actualSize = actualComponentSizes.get(fileType);
            if (actualSize != null)
            {
                return actualSize;
            }
            final Long size = componentSizes.get(fileType);
            if (size == null)
            {
                throw new IncompleteSSTableException(fileType);
            }
            return size;
        }

        public boolean isMissing(FileType fileType)
        {
            if (componentSizes != null)
            {
                return !componentSizes.containsKey(fileType);
            }
            return !context.s3BackupReader.exists(context.clusterName, context.datacenter, token, sstableKey, fileType);
        }

        public String getDataFileName()
        {
            return fileName;
        }

        public int hashCode()
        {
            return Objects.hash(token, fileName, sstableKey);
        }

        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final S3SSTable rhs = (S3SSTable) obj;
            return token.equals(rhs.token)
                   && fileName.equals(rhs.fileName)
                   && sstableKey.equals(rhs.sstableKey);
        }
    }

    /**
     * Async data source for streaming bytes from a single SSTable component to BufferingInputStream.
     * Sibling of {@link S3SSTable} so both can be {@code static} (no synthetic outer reference) while
     * still sharing the same package-private surface.
     */
    private static class S3SSTableSource implements CassandraFileSource<SSTable>
    {
        private final S3SSTable ssTable;
        private final FileType fileType;
        private final long manifestSize;
        private final S3SSTableContext context;
        private volatile long size;
        private volatile boolean actualSizeResolved;

        S3SSTableSource(S3SSTable ssTable, FileType fileType, long size, S3SSTableContext context)
        {
            this.ssTable = ssTable;
            this.fileType = fileType;
            this.manifestSize = size;
            this.context = context;
            Long actualSize = ssTable.actualComponentSizes.get(fileType);
            this.actualSizeResolved = actualSize != null;
            this.size = actualSize == null ? size : actualSize;
        }

        public void request(long start, long end, StreamConsumer consumer)
        {
            // Mutable metadata (Summary.db, Filter.db, Statistics.db) supports
            // size-drift handling for stale autosnap manifests.
            if (fileType.isMutableMetadata())
            {
                if (actualSizeResolved)
                {
                    if (start >= size)
                    {
                        consumer.onEnd();
                        return;
                    }
                    context.s3BackupReader.getAsync(context.clusterName,
                                                    context.datacenter,
                                                    ssTable.token,
                                                    ssTable.sstableKey,
                                                    fileType,
                                                    start,
                                                    Math.min(end, size - 1),
                                                    consumer);
                    return;
                }
                context.s3BackupReader.getMutableMetadataAsync(context.clusterName,
                                                                context.datacenter,
                                                                ssTable.token,
                                                                ssTable.sstableKey,
                                                                fileType,
                                                                start,
                                                                end,
                                                                consumer,
                                                                this::setActualSize,
                                                                manifestSize);
                return;
            }

            // Data.db ranged GETs go to the toBytes() path by default and only opt
            // into the toPublisher() streaming path when sstableDataPublisherReadEnabled=true.
            // All non-Data immutable components (Index.db, CompressionInfo.db, etc.) continue to
            // use the streaming path regardless of the flag.
            if (fileType == FileType.DATA && !context.sstableDataPublisherReadEnabled)
            {
                // Timeout semantics for the toBytes() path:
                //   BufferingInputStream's no-activity poll timeout (sstableS3ReadTimeoutSeconds,
                //   default 600s) covers the entire chunk materialization window because onRead is
                //   invoked exactly once per ranged GET (after the byte[] is fully assembled).
                //   Stalled sockets are caught earlier by the AWS SDK's NettyNioAsyncHttpClient
                //   readTimeout (S3ClientCache.READ_TIMEOUT_SECONDS=120s) and surfaced here via
                //   consumer.onError, so the BufferingInputStream poll timeout is the secondary
                //   safety net rather than the primary stall detector.
                context.s3BackupReader.readAsync(context.clusterName,
                                                  context.datacenter,
                                                  ssTable.token,
                                                  ssTable.sstableKey,
                                                  fileType,
                                                  start,
                                                  end)
                                       .whenComplete((bytes, throwable) -> {
                                           if (throwable != null)
                                           {
                                               consumer.onError(throwable);
                                               return;
                                           }
                                           try
                                           {
                                               // BufferingInputStream expects onRead(...) followed by onEnd().
                                               // wrap() does not copy: it adopts the byte[] reference.
                                               consumer.onRead(org.apache.cassandra.spark.utils.streaming.StreamBuffer.wrap(bytes));
                                               consumer.onEnd();
                                           }
                                           catch (Throwable forwardErr)
                                           {
                                               consumer.onError(forwardErr);
                                           }
                                       });
                return;
            }

            context.s3BackupReader.getAsync(context.clusterName, context.datacenter, ssTable.token,
                                             ssTable.sstableKey, fileType, start, end, consumer);
        }

        public S3SSTable cassandraFile()
        {
            return ssTable;
        }

        public FileType fileType()
        {
            return fileType;
        }

        public long size()
        {
            return size;
        }

        private void setActualSize(long actualSize)
        {
            if (actualSize > 0L)
            {
                size = actualSize;
                actualSizeResolved = true;
                ssTable.actualComponentSizes.put(fileType, actualSize);
            }
        }

        @Override
        public long chunkBufferSize()
        {
            return context.bufferSizeForChunk(fileType);
        }

        @Override
        public long maxBufferSize()
        {
            return context.bufferSizeForMax(fileType);
        }

        @Nullable
        @Override
        public Duration timeout()
        {
            return context.sstableS3ReadTimeoutSeconds > 0
                   ? Duration.ofSeconds(context.sstableS3ReadTimeoutSeconds)
                   : null;
        }
    }

    // jdk serialization

    @Nullable
    private static String readNullable(ObjectInputStream in) throws IOException
    {
        if (in.readBoolean())
        {
            return in.readUTF();
        }
        return null;
    }

    private static void writeNullable(ObjectOutputStream out, @Nullable String string) throws IOException
    {
        if (string == null)
        {
            out.writeBoolean(false);
        }
        else
        {
            out.writeBoolean(true);
            out.writeUTF(string);
        }
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.info("Falling back to JDK deserialization");
        this.clusterName = in.readUTF();
        this.keyspace = in.readUTF();
        this.table = in.readUTF();
        this.bridge = CassandraBridgeFactory.get(CassandraVersion.valueOf(in.readUTF()));

        this.s3Region = in.readUTF();
        this.s3Bucket = in.readUTF();
        this.s3EndpointOverride = readNullable(in);
        this.s3AccessKeyId = readNullable(in);
        this.s3SecretAccessKey = readNullable(in);
        this.sstableS3ReadTimeoutSeconds = in.readInt();
        this.dataChunkBufferSize = in.readLong();
        this.dataMaxBufferSize   = in.readLong();
        this.sstableCacheSummaryMaxEntries          = in.readInt();
        this.sstableCacheIndexMaxEntries            = in.readInt();
        this.sstableCacheStatsMaxEntries            = in.readInt();
        this.sstableCacheFilterMaxEntries           = in.readInt();
        this.sstableCacheCompressionInfoMaxEntries  = in.readInt();
        this.sstableTokenIndexEnabled = in.readBoolean();
        this.sstableTokenIndexPrebuildPartitions = in.readInt();
        this.sstableTokenIndexPrebuildPerTaskConcurrency = in.readInt();
        this.sstableDataPublisherReadEnabled = in.readBoolean();
        this.backupReaderType = in.readUTF();

        this.cqlTable = bridge.javaDeserialize(in, CqlTable.class);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        this.tokenPartitioner = (TokenPartitioner) in.readObject();
        this.ring = (CassandraRing) in.readObject();
        this.lastModifiedTimestampField = readNullable(in);
        this.snapshotTimestampField = readNullable(in);
        this.latestSnapshotEpochSecond = in.readLong();
        int features = in.readShort();
        List<SchemaFeature> requestedFeatures = new ArrayList<>(features);
        for (int feature = 0; feature < features; feature++)
        {
            String featureName = in.readUTF();
            requestedFeatures.add(SchemaFeatureSet.valueOf(featureName.toUpperCase()));
        }
        this.requestedFeatures = requestedFeatures;
        if (this.lastModifiedTimestampField != null)
        {
            CassandraDataLayer.aliasLastModifiedTimestamp(this.requestedFeatures, this.lastModifiedTimestampField);
        }
        injectSnapshotTimestamp(this.requestedFeatures, this.latestSnapshotEpochSecond, this.snapshotTimestampField);
        this.timeProvider = new S3SnapshotTimeProvider(in.readLong());
        // The executor JVM must have the concrete BackupReader implementation on its classpath.
        BackupReader deserializedReader = (BackupReader) in.readObject();
        // Canonicalize per task so all tasks on this executor share one reader per manifest.
        // The discarded fresh reader is reclaimed on the next GC.
        if (deserializedReader != null)
        {
            this.s3BackupReader = ReaderInternCache.canonicalize(this.clusterName, this.keyspace, this.table,
                                                                 this.datacenter,
                                                                 this.timeProvider.referenceEpochInSeconds(),
                                                                 this.latestSnapshotEpochSecond,
                                                                 deserializedReader);
        }

        // Mirror constructor: re-apply cache sysprops so executor-side cache sizes match driver.
        applySSTableCacheSystemProperties();

        // setStats on a canonicalized reader is shared mutation; see ReaderInternCache javadoc.
        this.stats = new SparkCustomMetricsStats();
        if (this.s3BackupReader != null)
        {
            this.s3BackupReader.setStats(this.stats);
        }
    }

    private void writeObject(final ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.info("Falling back to JDK serialization");
        out.writeUTF(this.clusterName);
        out.writeUTF(this.keyspace);
        out.writeUTF(this.table);
        out.writeUTF(this.version().name());
        out.writeUTF(this.s3Region);
        out.writeUTF(this.s3Bucket);
        writeNullable(out, this.s3EndpointOverride);
        writeNullable(out, this.s3AccessKeyId);
        writeNullable(out, this.s3SecretAccessKey);
        out.writeInt(this.sstableS3ReadTimeoutSeconds);
        out.writeLong(this.dataChunkBufferSize);
        out.writeLong(this.dataMaxBufferSize);
        out.writeInt(this.sstableCacheSummaryMaxEntries);
        out.writeInt(this.sstableCacheIndexMaxEntries);
        out.writeInt(this.sstableCacheStatsMaxEntries);
        out.writeInt(this.sstableCacheFilterMaxEntries);
        out.writeInt(this.sstableCacheCompressionInfoMaxEntries);
        out.writeBoolean(this.sstableTokenIndexEnabled);
        out.writeInt(this.sstableTokenIndexPrebuildPartitions);
        out.writeInt(this.sstableTokenIndexPrebuildPerTaskConcurrency);
        out.writeBoolean(this.sstableDataPublisherReadEnabled);
        out.writeUTF(this.backupReaderType);
        bridge.javaSerialize(out, this.cqlTable);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        out.writeObject(this.tokenPartitioner);
        out.writeObject(this.ring);
        writeNullable(out, this.lastModifiedTimestampField);
        writeNullable(out, this.snapshotTimestampField);
        out.writeLong(this.latestSnapshotEpochSecond);
        // Write the list of requested features: first write the size, then write the feature names
        out.writeShort(this.requestedFeatures.size());
        for (SchemaFeature feature : requestedFeatures)
        {
            out.writeUTF(feature.optionName());
        }
        out.writeLong(timeProvider.referenceEpochInSeconds());
        // Ensure s3BackupReader is initialized before serialization
        if (this.s3BackupReader == null)
        {
            initializeS3BackupReader();
        }
        // Carry the reader (with its populated manifest cache) over to the executor.
        out.writeObject(this.s3BackupReader);
    }

    // Kryo serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<S3CassandraDataLayer>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final S3CassandraDataLayer obj)
        {
            LOGGER.info("Serializing S3CassandraDataLayer with Kryo");
            out.writeString(obj.clusterName);
            out.writeString(obj.keyspace);
            out.writeString(obj.table);
            out.writeString(obj.datacenter);
            out.writeString(obj.s3Region);
            out.writeString(obj.s3Bucket);
            kryo.writeObjectOrNull(out, obj.s3EndpointOverride, String.class);
            kryo.writeObjectOrNull(out, obj.s3AccessKeyId, String.class);
            kryo.writeObjectOrNull(out, obj.s3SecretAccessKey, String.class);
            out.writeInt(obj.sstableS3ReadTimeoutSeconds);
            kryo.writeObject(out, obj.tokenPartitioner);
            kryo.writeObject(out, obj.version());
            kryo.writeObject(out, obj.ring);
            kryo.writeObject(out, obj.cqlTable);
            kryo.writeObject(out, obj.consistencyLevel);
            kryo.writeObjectOrNull(out, obj.lastModifiedTimestampField, String.class);
            kryo.writeObjectOrNull(out, obj.snapshotTimestampField, String.class);
            out.writeLong(obj.latestSnapshotEpochSecond);
            // Write the list of requested features: first write the size, then write the feature names
            S3CassandraDataLayer.Serializer.SchemaFeaturesListWrapper listWrapper = new S3CassandraDataLayer.Serializer.SchemaFeaturesListWrapper();
            listWrapper.requestedFeatureNames = obj.requestedFeatures.stream()
                                                                     .map(SchemaFeature::optionName)
                                                                     .collect(Collectors.toList());
            kryo.writeObject(out, listWrapper);
            out.writeLong(obj.timeProvider.referenceEpochInSeconds());
            // Ensure s3BackupReader is initialized before serialization
            if (obj.s3BackupReader == null)
            {
                obj.initializeS3BackupReader();
            }
            // Polymorphic write so core does not depend on a specific BackupReader subclass.
            // This Kryo path is currently unused at runtime (closure serialization is Java).
            kryo.writeClassAndObject(out, obj.s3BackupReader);
            out.writeString(obj.backupReaderType);

            // Trailing fields (set on the layer post-construction in read()) — the @VisibleForTesting
            // constructor below is frozen and does not accept them.
            out.writeLong(obj.dataChunkBufferSize);
            out.writeLong(obj.dataMaxBufferSize);
            out.writeInt(obj.sstableCacheSummaryMaxEntries);
            out.writeInt(obj.sstableCacheIndexMaxEntries);
            out.writeInt(obj.sstableCacheStatsMaxEntries);
            out.writeInt(obj.sstableCacheFilterMaxEntries);
            out.writeInt(obj.sstableCacheCompressionInfoMaxEntries);
            out.writeBoolean(obj.sstableTokenIndexEnabled);
            out.writeInt(obj.sstableTokenIndexPrebuildPartitions);
            out.writeInt(obj.sstableTokenIndexPrebuildPerTaskConcurrency);
            out.writeBoolean(obj.sstableDataPublisherReadEnabled);
        }

        @Override
        public S3CassandraDataLayer read(final Kryo kryo, final Input in, final Class<S3CassandraDataLayer> type)
        {
            LOGGER.info("Deserializing S3CassandraDataLayer with Kryo");
            String clusterName = in.readString();
            String keyspace = in.readString();
            String table = in.readString();
            String datacenter = in.readString();
            String s3Region = in.readString();
            String s3Bucket = in.readString();
            String s3EndpointOverride = kryo.readObjectOrNull(in, String.class);
            String s3AccessKeyId = kryo.readObjectOrNull(in, String.class);
            String s3SecretAccessKey = kryo.readObjectOrNull(in, String.class);
            int sstableS3ReadTimeoutSeconds = in.readInt();
            TokenPartitioner tokenPartitioner = kryo.readObject(in, TokenPartitioner.class);
            CassandraVersion version = kryo.readObject(in, CassandraVersion.class);
            CassandraRing ring = kryo.readObject(in, CassandraRing.class);
            CqlTable cqlTable = kryo.readObject(in, CqlTable.class);
            ConsistencyLevel consistencyLevel = kryo.readObject(in, ConsistencyLevel.class);
            String lastModifiedTimestampField = kryo.readObjectOrNull(in, String.class);
            String snapshotTimestampField = kryo.readObjectOrNull(in, String.class);
            long latestSnapshotEpochSecond = in.readLong();
            List<SchemaFeature> requestedFeatures = kryo.readObject(in, S3CassandraDataLayer.Serializer.SchemaFeaturesListWrapper.class).toList();
            TimeProvider timeProvider = new S3SnapshotTimeProvider(in.readLong());
            BackupReader s3BackupReader = (BackupReader) kryo.readClassAndObject(in);
            String backupReaderType = in.readString();

            S3CassandraDataLayer layer = new S3CassandraDataLayer(
            clusterName, keyspace, table, datacenter, s3Region, s3Bucket,
            s3EndpointOverride, s3AccessKeyId, s3SecretAccessKey, sstableS3ReadTimeoutSeconds,
            tokenPartitioner, version, ring, cqlTable, consistencyLevel,
            lastModifiedTimestampField, snapshotTimestampField, latestSnapshotEpochSecond,
            requestedFeatures, timeProvider, s3BackupReader, backupReaderType);

            layer.dataChunkBufferSize                   = in.readLong();
            layer.dataMaxBufferSize                     = in.readLong();
            layer.sstableCacheSummaryMaxEntries         = in.readInt();
            layer.sstableCacheIndexMaxEntries           = in.readInt();
            layer.sstableCacheStatsMaxEntries           = in.readInt();
            layer.sstableCacheFilterMaxEntries          = in.readInt();
            layer.sstableCacheCompressionInfoMaxEntries = in.readInt();
            layer.sstableTokenIndexEnabled = in.readBoolean();
            layer.sstableTokenIndexPrebuildPartitions = in.readInt();
            layer.sstableTokenIndexPrebuildPerTaskConcurrency = in.readInt();
            layer.sstableDataPublisherReadEnabled = in.readBoolean();

            // Executor-side equivalent of the constructor's apply.
            layer.applySSTableCacheSystemProperties();
            return layer;
        }

        // Wrapper only used internally for Kryo serialization/deserialization
        private static class SchemaFeaturesListWrapper
        {
            public List<String> requestedFeatureNames;  // CHECKSTYLE IGNORE: Public mutable field

            public List<SchemaFeature> toList()
            {
                return requestedFeatureNames.stream()
                                            .map(name -> SchemaFeatureSet.valueOf(name.toUpperCase()))
                                            .collect(Collectors.toList());
            }
        }
    }
}
