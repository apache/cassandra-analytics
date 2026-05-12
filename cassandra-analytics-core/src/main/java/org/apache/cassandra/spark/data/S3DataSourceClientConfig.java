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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.bridge.BigNumberConfigImpl;
import org.jetbrains.annotations.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.backup.BackupReaderConfig;
import org.apache.cassandra.spark.data.backup.BackupReaderFactory;
import org.apache.cassandra.spark.data.backup.BackupReaderRegistry;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.cassandra.spark.utils.Properties;

import static org.apache.cassandra.spark.data.CassandraDataLayer.aliasLastModifiedTimestamp;

/**
 * Configuration for S3-based Cassandra batch data sources.
 * Composes shared S3 and Cassandra schema configs with batch-specific settings.
 */
public class S3DataSourceClientConfig implements Serializable
{
    private static final long serialVersionUID = 2L;

    protected final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    // Batch-specific option keys
    public static final String DEFAULT_PARALLELISM_KEY = "defaultParallelism";
    public static final String NUM_CORES_KEY = "numCores";
    public static final String CONSISTENCY_LEVEL_KEY = "consistencyLevel";
    public static final String ENABLE_STATS_KEY = "enableStats";
    public static final String LAST_MODIFIED_COLUMN_NAME_KEY = "lastModifiedColumnName";
    public static final String SNAPSHOT_TIMESTAMP_COLUMN_NAME_KEY = "snapshotTimestampColumnName";
    public static final String READ_INDEX_OFFSET_KEY = "readIndexOffset";
    public static final String SIZING_KEY = "sizing";
    public static final String SIZING_DEFAULT = "default";
    public static final String SIZING_DYNAMIC = "dynamic";
    public static final String NUMBER_SPLITS_KEY = "number_splits";
    // When calculating sizing for dynamic sizing, each partition is maxPartitionSize GB in size.
    public static final String MAX_PARTITION_SIZE_KEY = "maxPartitionSize";
    public static final String SSTABLE_S3_READ_TIMEOUT_KEY = "sstable-s3-read-timeout";

    /**
     * Required option that selects a {@link BackupReaderFactory} from {@link BackupReaderRegistry}.
     * There is no default; vendor-specific implementation modules register their factory and
     * document the corresponding option value.
     */
    public static final String BACKUP_READER_TYPE_KEY = "backupReaderType";

    // Data.db ranged-GET buffering (DataSource options). Only Data.db; metadata components keep their
    // smaller per-FileType defaults in Properties#DEFAULT_CHUNK_BUFFER_OVERRIDE.
    public static final String S3_DATA_CHUNK_BUFFER_SIZE_KEY = "sstableDataChunkBufferSize";
    public static final String S3_DATA_MAX_BUFFER_SIZE_KEY   = "sstableDataMaxBufferSize";

    // When false, Data.db reads use the AsyncResponseTransformer.toBytes() path
    // (single materialized byte[] per ranged GET).
    // When true (default), Data.db reads use the AsyncResponseTransformer.toPublisher() streaming path
    // (many small ByteBuffer chunks fanned through BufferingInputStream). Non-Data file types
    // and mutable metadata are not affected.
    public static final String SSTABLE_DATA_PUBLISHER_READ_ENABLED_KEY = "sstableDataPublisherReadEnabled";

    public static final String SSTABLE_TOKEN_INDEX_ENABLED_KEY = "sstableTokenIndexEnabled";
    public static final String SSTABLE_TOKEN_INDEX_PREBUILD_PARTITIONS_KEY = "sstableTokenIndexPrebuildPartitions";
    public static final String SSTABLE_TOKEN_INDEX_PREBUILD_PER_TASK_CONCURRENCY_KEY = "sstableTokenIndexPrebuildPerTaskConcurrency";
    private static final int DEFAULT_SSTABLES_PER_TOKEN_INDEX_PREBUILD_PARTITION = 10_000;
    private static final int DEFAULT_SSTABLE_TOKEN_INDEX_PREBUILD_PER_TASK_CONCURRENCY = 4;

    // SSTable metadata cache sizing (forwarded to SSTableCache via sbr.cache.*.maxEntries sysprops).
    public static final String SSTABLE_CACHE_SUMMARY_MAX_ENTRIES_KEY          = "sstableCacheSummaryMaxEntries";
    public static final String SSTABLE_CACHE_INDEX_MAX_ENTRIES_KEY            = "sstableCacheIndexMaxEntries";
    public static final String SSTABLE_CACHE_STATS_MAX_ENTRIES_KEY            = "sstableCacheStatsMaxEntries";
    public static final String SSTABLE_CACHE_FILTER_MAX_ENTRIES_KEY           = "sstableCacheFilterMaxEntries";
    public static final String SSTABLE_CACHE_COMPRESSION_INFO_MAX_ENTRIES_KEY = "sstableCacheCompressionInfoMaxEntries";

    public static final int DEFAULT_NUM_SPLITS = -1;

    // Composed configs
    @NotNull
    private final S3ClientConfig s3Config;
    @NotNull
    private final CassandraSchemaConfig schemaConfig;

    // Batch-specific fields
    private final int defaultParallelism;
    private final int numCores;
    private final ConsistencyLevel consistencyLevel;
    private final Map<String, BigNumberConfigImpl> bigNumberConfigMap;
    private final boolean enableStats;
    private final boolean readIndexOffset;
    private final String sizing;
    private final int numberSplits;
    private final int maxPartitionSize;
    private final List<SchemaFeature> requestedFeatures;
    private final String lastModifiedTimestampField;
    private final String snapshotTimestampField;
    private final int sstableS3ReadTimeoutSeconds;
    private final long s3DataChunkBufferSize;
    private final long s3DataMaxBufferSize;
    private final boolean sstableDataPublisherReadEnabled;
    private final boolean sstableTokenIndexEnabled;
    private final int sstableTokenIndexPrebuildPartitions;
    private final int sstableTokenIndexPrebuildPerTaskConcurrency;
    private final int sstableCacheSummaryMaxEntries;
    private final int sstableCacheIndexMaxEntries;
    private final int sstableCacheStatsMaxEntries;
    private final int sstableCacheFilterMaxEntries;
    private final int sstableCacheCompressionInfoMaxEntries;
    @NotNull
    private final String backupReaderType;

    protected S3DataSourceClientConfig(Map<String, String> options)
    {
        // Create composed configs
        this.s3Config = S3ClientConfig.create(options);
        this.schemaConfig = CassandraSchemaConfig.create(options);

        // Batch-specific options
        this.defaultParallelism = MapUtils.getInt(options, DEFAULT_PARALLELISM_KEY, 1);
        this.numCores = MapUtils.getInt(options, NUM_CORES_KEY, 1);
        this.consistencyLevel = Optional.ofNullable(options.get(MapUtils.lowerCaseKey(CONSISTENCY_LEVEL_KEY)))
                                        .map(ConsistencyLevel::valueOf)
                                        .orElse(null);
        this.bigNumberConfigMap = BigNumberConfigImpl.build(options);
        this.enableStats = MapUtils.getBoolean(options, ENABLE_STATS_KEY, true);
        this.readIndexOffset = MapUtils.getBoolean(options, READ_INDEX_OFFSET_KEY, true);
        this.sizing = MapUtils.getOrDefault(options, SIZING_KEY, SIZING_DEFAULT);
        this.maxPartitionSize = MapUtils.getInt(options, MAX_PARTITION_SIZE_KEY, 1);
        this.lastModifiedTimestampField = MapUtils.getOrDefault(options, LAST_MODIFIED_COLUMN_NAME_KEY, null);
        this.snapshotTimestampField = MapUtils.getOrDefault(options, SNAPSHOT_TIMESTAMP_COLUMN_NAME_KEY, null);
        this.numberSplits = MapUtils.getInt(options, NUMBER_SPLITS_KEY, DEFAULT_NUM_SPLITS, "number of splits");
        this.requestedFeatures = initRequestedFeatures(options);
        this.sstableS3ReadTimeoutSeconds = MapUtils.getInt(options, SSTABLE_S3_READ_TIMEOUT_KEY, 600);
        // Floor at 1 MiB to avoid pathological overrides.
        this.s3DataChunkBufferSize = Math.max(1024L * 1024L,
                                              MapUtils.getLong(options, S3_DATA_CHUNK_BUFFER_SIZE_KEY,
                                                               Properties.DEFAULT_S3_DATA_CHUNK_BUFFER_SIZE));
        // Must be >= chunk so BufferingInputStream can enqueue a full chunk.
        this.s3DataMaxBufferSize = Math.max(this.s3DataChunkBufferSize,
                                            MapUtils.getLong(options, S3_DATA_MAX_BUFFER_SIZE_KEY,
                                                             Properties.DEFAULT_S3_DATA_MAX_BUFFER_SIZE));
        // Default true: Data.db reads use the AsyncResponseTransformer.toPublisher() streaming path.
        this.sstableDataPublisherReadEnabled = MapUtils.getBoolean(options, SSTABLE_DATA_PUBLISHER_READ_ENABLED_KEY, true);
        this.sstableTokenIndexEnabled = MapUtils.getBoolean(options, SSTABLE_TOKEN_INDEX_ENABLED_KEY, false);
        this.sstableTokenIndexPrebuildPartitions = optionalPositiveInt(options, SSTABLE_TOKEN_INDEX_PREBUILD_PARTITIONS_KEY);
        this.sstableTokenIndexPrebuildPerTaskConcurrency =
                Math.max(1, MapUtils.getInt(options,
                                            SSTABLE_TOKEN_INDEX_PREBUILD_PER_TASK_CONCURRENCY_KEY,
                                            DEFAULT_SSTABLE_TOKEN_INDEX_PREBUILD_PER_TASK_CONCURRENCY));
        this.sstableCacheSummaryMaxEntries          = nonNegativeInt(options, SSTABLE_CACHE_SUMMARY_MAX_ENTRIES_KEY,          32768);
        this.sstableCacheIndexMaxEntries            = nonNegativeInt(options, SSTABLE_CACHE_INDEX_MAX_ENTRIES_KEY,            16384);
        this.sstableCacheStatsMaxEntries            = nonNegativeInt(options, SSTABLE_CACHE_STATS_MAX_ENTRIES_KEY,            16384);
        this.sstableCacheFilterMaxEntries           = nonNegativeInt(options, SSTABLE_CACHE_FILTER_MAX_ENTRIES_KEY,           16384);
        this.sstableCacheCompressionInfoMaxEntries  = nonNegativeInt(options, SSTABLE_CACHE_COMPRESSION_INFO_MAX_ENTRIES_KEY, 16384);
        this.backupReaderType = requiredBackupReaderType(options);
    }

    /**
     * Validates that the {@code backupReaderType} option is present and matches a registered
     * factory, falling back to a precise error message that lists the registered types if not.
     * Validating eagerly here surfaces the misconfiguration at config-parse time rather than at
     * first read.
     */
    private static String requiredBackupReaderType(Map<String, String> options)
    {
        String type = options.get(MapUtils.lowerCaseKey(BACKUP_READER_TYPE_KEY));
        if (type == null || type.trim().isEmpty())
        {
            throw new IllegalArgumentException(
                "Missing required option '" + BACKUP_READER_TYPE_KEY + "'. "
                + "Register a backup reader factory via BackupReaderRegistry.register(...) at driver "
                + "startup and set this option to its registered type. Registered types: "
                + BackupReaderRegistry.registeredTypes());
        }
        // Fail fast if the type is unregistered; factoryFor throws a precise message listing
        // the registered types. Resolved factory is intentionally discarded — the driver only
        // needs it later at reader-construction time.
        BackupReaderRegistry.factoryFor(type);
        return type;
    }

    /** Parse a non-negative int option; throws with the option key in the message on negatives. */
    private static int nonNegativeInt(Map<String, String> options, String key, int defaultValue)
    {
        int value = MapUtils.getInt(options, key, defaultValue);
        if (value < 0)
        {
            throw new IllegalArgumentException(
                "Invalid value for option '" + key + "': " + value + " (must be >= 0; use 0 to disable the cache)");
        }
        return value;
    }

    private static int optionalPositiveInt(Map<String, String> options, String key)
    {
        String value = options.get(MapUtils.lowerCaseKey(key));
        return value == null ? 0 : Math.max(1, Integer.parseInt(value));
    }

    public static S3DataSourceClientConfig create(Map<String, String> options)
    {
        return new S3DataSourceClientConfig(options);
    }

    // ========================================================================
    // Composed Config Accessors
    // ========================================================================

    /**
     * Get the S3 client configuration.
     */
    @NotNull
    public S3ClientConfig s3Config()
    {
        return s3Config;
    }

    /**
     * Get the Cassandra schema configuration.
     */
    @NotNull
    public CassandraSchemaConfig schemaConfig()
    {
        return schemaConfig;
    }

    // ========================================================================
    // Batch-Specific Accessors
    // ========================================================================

    public int defaultParallelism()
    {
        return defaultParallelism;
    }

    public int numCores()
    {
        return numCores;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    public Map<String, BigNumberConfigImpl> bigNumberConfigMap()
    {
        return bigNumberConfigMap;
    }

    public boolean enableStats()
    {
        return enableStats;
    }

    public boolean readIndexOffset()
    {
        return readIndexOffset;
    }

    public String sizing()
    {
        return sizing;
    }

    public int maxPartitionSize()
    {
        return maxPartitionSize;
    }

    public int numberSplits()
    {
        return numberSplits;
    }

    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    public String lastModifiedTimestampField()
    {
        return lastModifiedTimestampField;
    }

    public String snapshotTimestampField()
    {
        return snapshotTimestampField;
    }

    public int sstableS3ReadTimeoutSeconds()
    {
        return sstableS3ReadTimeoutSeconds;
    }

    /** Ranged-GET chunk size (bytes) for {@code Data.db}; trades per-GET / KMS overhead vs per-chunk heap. */
    public long s3DataChunkBufferSize()
    {
        return s3DataChunkBufferSize;
    }

    /** Per-stream buffer cap for Data.db; values above the chunk size allow one in-flight + one draining. */
    public long s3DataMaxBufferSize()
    {
        return s3DataMaxBufferSize;
    }

    /**
     * When false, Data.db reads use {@code AsyncResponseTransformer.toBytes()}.
     * When true (default), Data.db reads use {@code AsyncResponseTransformer.toPublisher()}.
     * Non-Data file types and mutable metadata reads are unaffected.
     */
    public boolean sstableDataPublisherReadEnabled()
    {
        return sstableDataPublisherReadEnabled;
    }

    public boolean sstableTokenIndexEnabled()
    {
        return sstableTokenIndexEnabled;
    }

    public int sstableTokenIndexPrebuildPartitions()
    {
        return sstableTokenIndexPrebuildPartitions;
    }

    public int sstableTokenIndexPrebuildPerTaskConcurrency()
    {
        return sstableTokenIndexPrebuildPerTaskConcurrency;
    }

    public int resolveSSTableTokenIndexPrebuildPartitions(int sstableCount, int sparkDefaultParallelism)
    {
        if (sstableTokenIndexPrebuildPartitions > 0)
        {
            return sstableTokenIndexPrebuildPartitions;
        }

        int defaultParallelismCap = Math.max(1, sparkDefaultParallelism / 4);
        int sstableCountPartitions = Math.max(1, (sstableCount + DEFAULT_SSTABLES_PER_TOKEN_INDEX_PREBUILD_PARTITION - 1)
                                                / DEFAULT_SSTABLES_PER_TOKEN_INDEX_PREBUILD_PARTITION);
        return Math.min(defaultParallelismCap, sstableCountPartitions);
    }

    public int sstableCacheSummaryMaxEntries()
    {
        return sstableCacheSummaryMaxEntries;
    }

    public int sstableCacheIndexMaxEntries()
    {
        return sstableCacheIndexMaxEntries;
    }

    public int sstableCacheStatsMaxEntries()
    {
        return sstableCacheStatsMaxEntries;
    }

    public int sstableCacheFilterMaxEntries()
    {
        return sstableCacheFilterMaxEntries;
    }

    public int sstableCacheCompressionInfoMaxEntries()
    {
        return sstableCacheCompressionInfoMaxEntries;
    }

    /** Returns the configured backup reader type (a registered key in {@link BackupReaderRegistry}). */
    @NotNull
    public String backupReaderType()
    {
        return backupReaderType;
    }

    /**
     * Builds a {@link BackupReaderConfig} from this config's {@link S3ClientConfig}. Stats are
     * intentionally left unset; callers install the appropriate {@code Stats} instance
     * (executor-local {@code SparkCustomMetricsStats} for reads, {@code DoNothingStats.INSTANCE}
     * for prebuild) via {@link BackupReaderConfig#withStats(org.apache.cassandra.analytics.stats.Stats)}.
     */
    public BackupReaderConfig toBackupReaderConfig()
    {
        return BackupReaderConfig.of(s3Config);
    }

    /**
     * Driver-side apply of SSTable cache sizes as JVM sysprops; existing values are preserved.
     * Executor-side propagation lives on {@link S3CassandraDataLayer#applySSTableCacheSystemProperties()}.
     */
    public void applySSTableCacheSystemProperties()
    {
        setIfUnset("sbr.cache.summary.maxEntries",         String.valueOf(sstableCacheSummaryMaxEntries));
        setIfUnset("sbr.cache.index.maxEntries",           String.valueOf(sstableCacheIndexMaxEntries));
        setIfUnset("sbr.cache.stats.maxEntries",           String.valueOf(sstableCacheStatsMaxEntries));
        setIfUnset("sbr.cache.filter.maxEntries",          String.valueOf(sstableCacheFilterMaxEntries));
        setIfUnset("sbr.cache.compressionInfo.maxEntries", String.valueOf(sstableCacheCompressionInfoMaxEntries));
    }

    private static void setIfUnset(String name, String value)
    {
        if (System.getProperty(name) == null)
        {
            System.setProperty(name, value);
        }
    }

    // ========================================================================
    // Convenience Delegate Methods (for backward compatibility)
    // These delegate to the composed configs for callers that access these
    // properties directly on S3DataSourceClientConfig.
    // ========================================================================

    /**
     * Get the cluster identifier (UUID). Resolved eagerly at config creation.
     * Delegates to schemaConfig.
     */
    public String clusterName()
    {
        return schemaConfig.clusterName();
    }

    /**
     * Get the keyspace name. Delegates to schemaConfig.
     */
    public String keyspace()
    {
        return schemaConfig.keyspace();
    }

    /**
     * Get the table name. Delegates to schemaConfig.
     */
    public String table()
    {
        return schemaConfig.table();
    }

    /**
     * Get the datacenter. Delegates to schemaConfig.
     */
    public String datacenter()
    {
        return schemaConfig.datacenter();
    }

    /**
     * Get the CREATE TABLE statement. Delegates to schemaConfig.
     */
    public String tableCreateStmt()
    {
        return schemaConfig.tableCreateStmt();
    }

    /**
     * Get the Cassandra version. Delegates to schemaConfig.
     */
    public org.apache.cassandra.bridge.CassandraVersion cassandraVersion()
    {
        return schemaConfig.cassandraVersion();
    }

    /**
     * Get raw UDT definitions string. Delegates to schemaConfig.
     */
    public String udts()
    {
        return schemaConfig.udts();
    }

    /**
     * Get parsed UDTs. Delegates to schemaConfig.
     */
    public java.util.Set<String> parsedUdts()
    {
        return schemaConfig.parsedUdts();
    }

    /**
     * Get parsed replication factor. Delegates to schemaConfig.
     */
    public ReplicationFactor getParsedReplicationFactor()
    {
        return schemaConfig.getParsedReplicationFactor();
    }

    /**
     * Get the S3 region. Delegates to s3Config.
     */
    public String s3Region()
    {
        return s3Config.s3Region();
    }

    /**
     * Get the S3 bucket. Delegates to s3Config.
     */
    public String s3Bucket()
    {
        return s3Config.s3Bucket();
    }

    /**
     * Get the S3 endpoint override. Delegates to s3Config.
     */
    public String s3EndpointOverride()
    {
        return s3Config.s3EndpointOverride();
    }

    /**
     * Get the S3 access key ID. Delegates to s3Config.
     */
    public String s3AccessKeyId()
    {
        return s3Config.s3AccessKeyId();
    }

    /**
     * Get the S3 secret access key. Delegates to s3Config.
     */
    public String s3SecretAccessKey()
    {
        return s3Config.s3SecretAccessKey();
    }

    protected List<SchemaFeature> initRequestedFeatures(Map<String, String> options)
    {
        Map<String, String> optionsCopy = new HashMap<>(options);
        String lastModifiedColumnName = MapUtils.getOrDefault(options, LAST_MODIFIED_COLUMN_NAME_KEY, null);
        if (lastModifiedColumnName != null)
        {
            optionsCopy.put(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName(), "true");
        }
        String snapshotTimestampColumnName = MapUtils.getOrDefault(options, SNAPSHOT_TIMESTAMP_COLUMN_NAME_KEY, null);
        if (snapshotTimestampColumnName != null)
        {
            optionsCopy.put(SchemaFeatureSet.SNAPSHOT_TIMESTAMP.optionName(), "true");
        }
        List<SchemaFeature> features = SchemaFeatureSet.initializeFromOptions(optionsCopy);
        if (lastModifiedColumnName != null)
        {
            aliasLastModifiedTimestamp(features, lastModifiedColumnName);
        }
        return features;
    }
}
