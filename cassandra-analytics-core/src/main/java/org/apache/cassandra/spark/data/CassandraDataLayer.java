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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import o.a.c.sidecar.client.shaded.common.response.ListSnapshotFilesResponse;
import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.common.response.SchemaResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.BigNumberConfigImpl;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.SSTableVersionAnalyzer;
import org.apache.cassandra.clients.ExecutorHolder;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SimpleSidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.client.exception.RetriesExhaustedException;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.common.SidecarInstanceFactory;
import org.apache.cassandra.spark.common.SizingFactory;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.sparksql.LastModifiedTimestampDecorator;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.ReaderTimeProvider;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.TokenRangeUtils;
import org.apache.cassandra.spark.validation.CassandraValidation;
import org.apache.cassandra.spark.validation.SidecarValidation;
import org.apache.cassandra.spark.validation.StartupValidatable;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.util.ShutdownHookManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.CqlUtils.isTimeRangeFilterSupported;
import static org.apache.cassandra.spark.utils.Properties.NODE_STATUS_NOT_CONSIDERED;

public class CassandraDataLayer extends PartitionedDataLayer implements StartupValidatable, Serializable
{
    private static final long serialVersionUID = -9038926850642710787L;

    public static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataLayer.class);
    private static final Cache<String, CompletableFuture<List<SSTable>>> SNAPSHOT_CACHE =
    CacheBuilder.newBuilder()
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .maximumSize(128)
                .build();

    protected String snapshotName;
    protected boolean quoteIdentifiers;
    protected String keyspace;
    protected String table;
    protected String maybeQuotedKeyspace;
    protected String maybeQuotedTable;
    protected CassandraBridge bridge;
    // create clusterConfig from sidecarInstances and sidecarPort, see initializeClusterConfig
    protected String sidecarInstances;
    protected int sidecarPort;
    protected transient Set<SidecarInstance> clusterConfig;
    protected TokenPartitioner tokenPartitioner;
    protected Map<String, AvailabilityHint> availabilityHints;
    protected Sidecar.ClientConfig sidecarClientConfig;
    protected Map<String, BigNumberConfigImpl> bigNumberConfigMap;
    protected boolean enableStats;
    protected boolean readIndexOffset;
    protected boolean useIncrementalRepair;
    protected List<SchemaFeature> requestedFeatures;
    protected Map<String, ReplicationFactor> rfMap;
    @Nullable
    protected String lastModifiedTimestampField;
    protected Set<String> sstableVersionsOnCluster;
    // volatile in order to publish the reference for visibility
    protected volatile CqlTable cqlTable;
    protected transient TimeProvider timeProvider;
    protected transient SidecarClient sidecar;

    private SslConfig sslConfig;
    private SSTableTimeRangeFilter sstableTimeRangeFilter;

    @VisibleForTesting
    transient Map<String, SidecarInstance> sidecarInstanceMap;

    public CassandraDataLayer(@NotNull ClientConfig options,
                              @NotNull Sidecar.ClientConfig sidecarClientConfig,
                              @Nullable SslConfig sslConfig)
    {
        super(options.consistencyLevel(), options.datacenter());
        this.snapshotName = options.snapshotName();
        this.keyspace = options.keyspace();
        this.table = options.table();
        this.quoteIdentifiers = options.quoteIdentifiers();
        this.sidecarClientConfig = sidecarClientConfig;
        this.sidecarInstances = options.sidecarContactPoints;
        this.sidecarPort = options.sidecarPort;
        this.sslConfig = sslConfig;
        this.bigNumberConfigMap = options.bigNumberConfigMap();
        this.enableStats = options.enableStats();
        this.readIndexOffset = options.readIndexOffset();
        this.useIncrementalRepair = options.useIncrementalRepair();
        this.lastModifiedTimestampField = options.lastModifiedTimestampField();
        this.requestedFeatures = options.requestedFeatures();
        this.sstableTimeRangeFilter = options.sstableTimeRangeFilter;
    }

    // For serialization
    @VisibleForTesting
    // CHECKSTYLE IGNORE: Constructor with many parameters
    protected CassandraDataLayer(@Nullable String keyspace,
                                 @Nullable String table,
                                 boolean quoteIdentifiers,
                                 @NotNull String snapshotName,
                                 @Nullable String datacenter,
                                 @NotNull Sidecar.ClientConfig sidecarClientConfig,
                                 @Nullable SslConfig sslConfig,
                                 @NotNull CqlTable cqlTable,
                                 @NotNull TokenPartitioner tokenPartitioner,
                                 @NotNull CassandraVersion bridgeVersion,
                                 @NotNull ConsistencyLevel consistencyLevel,
                                 @NotNull String sidecarInstances,
                                 @NotNull int sidecarPort,
                                 @NotNull Map<String, PartitionedDataLayer.AvailabilityHint> availabilityHints,
                                 @NotNull Map<String, BigNumberConfigImpl> bigNumberConfigMap,
                                 boolean enableStats,
                                 boolean readIndexOffset,
                                 boolean useIncrementalRepair,
                                 @Nullable String lastModifiedTimestampField,
                                 List<SchemaFeature> requestedFeatures,
                                 @NotNull Map<String, ReplicationFactor> rfMap,
                                 TimeProvider timeProvider,
                                 SSTableTimeRangeFilter sstableTimeRangeFilter,
                                 Set<String> sstableVersionsOnCluster)
    {
        super(consistencyLevel, datacenter);
        this.snapshotName = snapshotName;
        this.bridge = CassandraBridgeFactory.get(bridgeVersion);
        this.keyspace = keyspace;
        this.table = table;
        this.quoteIdentifiers = quoteIdentifiers;
        this.cqlTable = cqlTable;
        this.tokenPartitioner = tokenPartitioner;
        this.clusterConfig = initializeClusterConfig(sidecarInstances, sidecarPort);
        this.availabilityHints = availabilityHints;
        this.sidecarClientConfig = sidecarClientConfig;
        this.sslConfig = sslConfig;
        this.bigNumberConfigMap = bigNumberConfigMap;
        this.enableStats = enableStats;
        this.readIndexOffset = readIndexOffset;
        this.useIncrementalRepair = useIncrementalRepair;
        this.lastModifiedTimestampField = lastModifiedTimestampField;
        this.requestedFeatures = requestedFeatures;
        if (lastModifiedTimestampField != null)
        {
            aliasLastModifiedTimestamp(this.requestedFeatures, this.lastModifiedTimestampField);
        }
        this.rfMap = rfMap;
        this.timeProvider = timeProvider;
        this.sstableTimeRangeFilter = sstableTimeRangeFilter;
        this.sstableVersionsOnCluster = sstableVersionsOnCluster;
        this.maybeQuoteKeyspaceAndTable();
        this.initSidecarClient();
        this.initInstanceMap();
        this.startupValidate();
    }

    public void initialize(@NotNull ClientConfig options)
    {
        dialHome(options);

        timeProvider = new ReaderTimeProvider();
        LOGGER.info("Starting Cassandra Spark job snapshotName={} keyspace={} table={} dc={} referenceEpoch={}",
                    snapshotName, keyspace, table, datacenter, timeProvider.referenceEpochInSeconds());

        // Load cluster config from options
        clusterConfig = initializeClusterConfig(options);
        initSidecarClient();

        // Get cluster info from Cassandra Sidecar
        int effectiveNumberOfCores;
        try
        {
            effectiveNumberOfCores = initBulkReader(options);
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
        catch (ExecutionException exception)
        {
            throw new RuntimeException(ThrowableUtils.rootCause(exception));
        }
        initInstanceMap();
        LOGGER.info("Initialized Cassandra Bulk Reader with effectiveNumberOfCores={}", effectiveNumberOfCores);
    }

    private int initBulkReader(@NotNull ClientConfig options) throws ExecutionException, InterruptedException
    {
        Preconditions.checkArgument(keyspace != null, "Keyspace must be non-null for Cassandra Bulk Reader");
        Preconditions.checkArgument(table != null, "Table must be non-null for Cassandra Bulk Reader");
        ShutdownHookManager.addShutdownHook(org.apache.spark.util.ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY(),
                                            ScalaFunctions.wrapLambda(() -> shutdownHook(options)));

        NodeSettings nodeSettings = sidecar.nodeSettings().get();
        String cassandraVersion = getEffectiveCassandraVersionForRead(clusterConfig, nodeSettings);
        Partitioner partitioner = Partitioner.from(nodeSettings.partitioner());

        // Initialize SSTable versions and bridge version
        CassandraVersion bridgeVersion = initializeSSTableVersionsAndBridgeVersion(cassandraVersion);
        bridge = CassandraBridgeFactory.get(bridgeVersion);

        // optionally quote identifiers if the option has been set, we need an instance for the bridge
        maybeQuoteKeyspaceAndTable();

        // Now we can use the quoted keyspace and table if needed
        CompletableFuture<RingResponse> ringFuture = sidecar.ring(maybeQuotedKeyspace);
        CompletableFuture<SchemaResponse> schemaFuture = sidecar.schema(maybeQuotedKeyspace);
        CompletableFuture<Map<String, PartitionedDataLayer.AvailabilityHint>> snapshotFuture;
        if (options.createSnapshot())
        {
            // Use create snapshot request to capture instance availability hint
            LOGGER.info("Creating snapshot snapshotName={} keyspace={} table={} dc={}",
                        snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter);
            snapshotFuture = ringFuture.thenCompose(ringResponse -> createSnapshot(options, ringResponse));
        }
        else
        {
            snapshotFuture = CompletableFuture.completedFuture(new HashMap<>());
        }
        availabilityHints = snapshotFuture.get();

        String fullSchema = schemaFuture.get().schema();
        String createStmt = CqlUtils.extractTableSchema(fullSchema, keyspace, table);
        int indexCount = CqlUtils.extractIndexCount(fullSchema, keyspace, table);
        Set<String> udts = CqlUtils.extractUdts(fullSchema, keyspace);
        ReplicationFactor replicationFactor = CqlUtils.extractReplicationFactor(fullSchema, keyspace);

        String tableSchemaWithProps = CqlUtils.extractCleanedTableSchema(fullSchema, keyspace, table, true);
        String compactionStrategy = CqlUtils.extractCompactionStrategy(tableSchemaWithProps);
        if (sstableTimeRangeFilter != null
            && sstableTimeRangeFilter != SSTableTimeRangeFilter.ALL
            && !isTimeRangeFilterSupported(compactionStrategy))
        {
            throw new UnsupportedOperationException("SSTableTimeRangeFilter is only supported with TimeWindowCompactionStrategy. " +
                                                    "Current compaction strategy is: " + compactionStrategy);
        }

        rfMap = Map.of(keyspace, replicationFactor);
        CompletableFuture<Integer> sizingFuture = CompletableFuture.supplyAsync(
        () -> getSizing(ringFuture, replicationFactor, options).getEffectiveNumberOfCores(),
        ExecutorHolder.EXECUTOR_SERVICE);
        validateReplicationFactor(replicationFactor);
        udts.forEach(udt -> LOGGER.info("Adding schema UDT: '{}'", udt));

        cqlTable = bridge().buildSchema(createStmt, keyspace, replicationFactor, partitioner, udts, null, indexCount, false);

        TokenRangeReplicasResponse tokenRangeReplicas = sidecar.tokenRangeReplicas(
        new ArrayList<>(clusterConfig), maybeQuotedKeyspace).get();
        TokenRangeUtils.validateTokenRangeReplicasResponse(tokenRangeReplicas);

        CassandraRing ring = createCassandraRingFromRing(partitioner, replicationFactor, ringFuture.get(), tokenRangeReplicas);

        int effectiveNumberOfCores = sizingFuture.get();
        tokenPartitioner = new TokenPartitioner(ring, options.defaultParallelism(), effectiveNumberOfCores);
        return effectiveNumberOfCores;
    }

    /**
     * Checks if SSTable version-based bridge selection is disabled, reading the flag from the active
     * (driver-side) {@link SparkSession}. Protected to allow test overrides without a SparkSession.
     *
     * @return true if disabled, false if enabled
     */
    @VisibleForTesting
    protected boolean isSSTableVersionBasedBridgeDisabled()
    {
        // Read from the active SparkSession on the driver. This intentionally does not create a SparkContext:
        // SparkSession.active() fails fast if called without an active/default session (e.g. on an executor),
        // rather than silently building a master-less context.
        return SparkSession.active()
                           .sparkContext()
                           .getConf()
                           .getBoolean(BulkSparkConf.DISABLE_SSTABLE_VERSION_BASED_BRIDGE, false);
    }

    /**
     * Initializes SSTable versions from cluster gossip and determines bridge version.
     *
     * @param cassandraVersion the Cassandra version
     * @return the determined bridge version
     */
    @VisibleForTesting
    protected CassandraVersion initializeSSTableVersionsAndBridgeVersion(String cassandraVersion)
    {
        // Check if user has explicitly disabled SSTable version-based selection via Spark configuration
        boolean isSSTableVersionBasedBridgeDisabled = isSSTableVersionBasedBridgeDisabled();

        CassandraVersion bridgeVersion;
        if (isSSTableVersionBasedBridgeDisabled)
        {
            // Disabled: skip cluster SSTable-version retrieval and fall back to legacy mode.
            // Use an empty set (never null) so downstream code - including executor-side validation and
            // serialization - needs no null handling; on executors an empty set signals the feature was disabled
            // on the driver. HashSet specifically, because Kryo reads this field back via kryo.readObject(in, HashSet.class).
            this.sstableVersionsOnCluster = new HashSet<>();
            bridgeVersion = CassandraVersion.fromVersion(cassandraVersion)
                                            .orElseThrow(() -> new UnsupportedOperationException(
                                            "Unsupported Cassandra version: " + cassandraVersion));
            LOGGER.info("SSTable version-based bridge selection is disabled; determined bridge version {} for read "
                        + "from the cluster's Cassandra release version {} (legacy mode)",
                        bridgeVersion.versionName(), cassandraVersion);
        }
        else
        {
            // Wrap in a HashSet: getSSTableVersionsFromCluster() may return Collections.emptySet()
            // or an unspecified Set type from Collectors.toSet(), but Kryo reads this field back via
            // kryo.readObject(in, HashSet.class), so the concrete type must be HashSet.
            this.sstableVersionsOnCluster = new HashSet<>(getSSTableVersionsFromCluster());
            // Pick the highest (mutually-compatible) version present on the cluster. determineBridgeVersionForRead
            // fails fast with an actionable hint when no SSTable versions were retrieved while the feature is enabled.
            bridgeVersion = SSTableVersionAnalyzer.determineBridgeVersionForRead(sstableVersionsOnCluster);
        }

        return bridgeVersion;
    }

    /**
     * Retrieves SSTable versions from cluster via Sidecar gossip.
     * Protected to allow test overrides.
     *
     * @return set of SSTable versions from cluster
     */
    @VisibleForTesting
    protected Set<String> getSSTableVersionsFromCluster()
    {
        return Sidecar.getSSTableVersionsFromCluster(sidecar,
                                                     clusterConfig,
                                                     sidecarClientConfig.maxMillisToSleep(),
                                                     sidecarClientConfig.maxRetries(),
                                                     sidecarClientConfig.timeoutSeconds()
        );
    }

    protected void shutdownHook(ClientConfig options)
    {
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = options.clearSnapshotStrategy();
        if (clearSnapshotStrategy.shouldClearOnCompletion())
        {
            if (options.createSnapshot())
            {
                clearSnapshot(clusterConfig, options);
            }
            else
            {
                LOGGER.warn("Skipping clearing snapshot because it was not created by this job. "
                            + "Only the job that created the snapshot can clear it. "
                            + "snapshotName={} keyspace={} table={} dc={}",
                            snapshotName, keyspace, table, datacenter);
            }
        }
        else if (clearSnapshotStrategy.hasTTL())
        {
            LOGGER.warn("Skipping clearing snapshot because clearSnapshotStrategy '{}' is used", clearSnapshotStrategy);
        }

        try
        {
            sidecar.close();
        }
        catch (Exception exception)
        {
            LOGGER.warn("Unable to close Sidecar", exception);
        }
    }

    private CompletionStage<Map<String, AvailabilityHint>> createSnapshot(ClientConfig options, RingResponse ring)
    {
        Map<String, PartitionedDataLayer.AvailabilityHint> availabilityHints = new ConcurrentHashMap<>(ring.size());

        // Set to ensure a snapshot is only created once per host
        Set<String> distinctInstances = new HashSet<>();

        // Fire off create snapshot request across the entire cluster
        List<CompletableFuture<Void>> futures =
        ring.stream()
            .filter(ringEntry -> datacenter == null || datacenter.equals(ringEntry.datacenter()))
            .filter(ringEntry -> distinctInstances.add(ringEntry.fqdn() + ':' + ringEntry.port()))
            .map(ringEntry -> {
                PartitionedDataLayer.AvailabilityHint hint =
                PartitionedDataLayer.AvailabilityHint.fromState(ringEntry.status(), ringEntry.state());

                CompletableFuture<PartitionedDataLayer.AvailabilityHint> createSnapshotFuture;
                if (NODE_STATUS_NOT_CONSIDERED.contains(ringEntry.state()))
                {
                    LOGGER.warn("Skip snapshot creating when node is joining or down "
                                + "snapshotName={} keyspace={} table={} datacenter={} fqdn={} status={} state={}",
                                snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter, ringEntry.fqdn(), ringEntry.status(), ringEntry.state());
                    createSnapshotFuture = CompletableFuture.completedFuture(hint);
                }
                else
                {
                    LOGGER.info("Creating snapshot on instance snapshotName={} keyspace={} table={} datacenter={} fqdn={}",
                                snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter, ringEntry.fqdn());
                    SidecarInstance sidecarInstance = new SidecarInstanceImpl(ringEntry.fqdn(), sidecarClientConfig.effectivePort());
                    ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = options.clearSnapshotStrategy();
                    createSnapshotFuture = sidecar.createSnapshot(sidecarInstance, maybeQuotedKeyspace,
                                                                  maybeQuotedTable, snapshotName, clearSnapshotStrategy.ttl())
                                                  .handle((resp, throwable) -> {
                                                      if (throwable == null)
                                                      {
                                                          // Create snapshot succeeded
                                                          return hint;
                                                      }

                                                      if (isExhausted(throwable))
                                                      {
                                                          LOGGER.warn("Failed to create snapshot on instance", throwable);
                                                          return PartitionedDataLayer.AvailabilityHint.DOWN;
                                                      }

                                                      LOGGER.error("Unexpected error creating snapshot on instance", throwable);
                                                      return PartitionedDataLayer.AvailabilityHint.UNKNOWN;
                                                  });
                }

                return createSnapshotFuture
                       .thenAccept(h -> availabilityHints.put(ringEntry.fqdn(), h));
            })
            .collect(Collectors.toList());

        return CompletableFuture
               .allOf(futures.toArray(new CompletableFuture[0]))
               .handle((results, throwable) -> availabilityHints);
    }

    protected boolean isExhausted(@Nullable Throwable throwable)
    {
        return throwable != null && (throwable instanceof RetriesExhaustedException || isExhausted(throwable.getCause()));
    }

    @Override
    public TimeProvider timeProvider()
    {
        return timeProvider;
    }

    @Override
    public boolean useIncrementalRepair()
    {
        return useIncrementalRepair;
    }

    @Override
    public boolean readIndexOffset()
    {
        return readIndexOffset;
    }

    protected void initInstanceMap()
    {
        Preconditions.checkState(tokenPartitioner != null, "tokenPartitioner cannot be absent");
        sidecarInstanceMap = tokenPartitioner
                             .ring()
                             .instances()
                             .stream()
                             .filter(instance -> datacenter == null || datacenter.equals(instance.dataCenter()))
                             .map(CassandraInstance::nodeName)
                             .distinct()
                             .map(nodeName -> new SidecarInstanceImpl(nodeName, sidecarClientConfig.effectivePort()))
                             .collect(Collectors.toMap(SidecarInstance::hostname, Function.identity()));
        LOGGER.info("Initialized CassandraDataLayer sidecarInstanceMap numInstances={}", sidecarInstanceMap.size());
    }

    protected void initSidecarClient()
    {
        try
        {
            SslConfigSecretsProvider secretsProvider = sslConfig != null
                                                       ? new SslConfigSecretsProvider(sslConfig)
                                                       : null;
            sidecar = Sidecar.from(new SimpleSidecarInstancesProvider(new ArrayList<>(clusterConfig)),
                                   sidecarClientConfig,
                                   secretsProvider);
        }
        catch (IOException ioException)
        {
            throw new RuntimeException("Unable to build sidecar client", ioException);
        }
        LOGGER.info("Initialized sidecar client");
    }

    @Override
    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }

    @Override
    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    @Override
    public CassandraRing ring()
    {
        return tokenPartitioner.ring();
    }

    @Override
    public TokenPartitioner tokenPartitioner()
    {
        return tokenPartitioner;
    }

    @Override
    protected ExecutorService executorService()
    {
        return ExecutorHolder.EXECUTOR_SERVICE;
    }

    @Override
    public String jobId()
    {
        return null;
    }

    @NotNull
    @Override
    public SSTableTimeRangeFilter sstableTimeRangeFilter()
    {
        return sstableTimeRangeFilter;
    }

    @Override
    public CqlTable cqlTable()
    {
        if (cqlTable == null)
        {
            throw new RuntimeException("Schema not initialized");
        }
        return cqlTable;
    }

    @Override
    public ReplicationFactor replicationFactor(String keyspace)
    {
        return rfMap.get(keyspace);
    }

    @Override
    protected PartitionedDataLayer.AvailabilityHint getAvailability(CassandraInstance instance)
    {
        // Hint CassandraInstance availability to parent PartitionedDataLayer
        PartitionedDataLayer.AvailabilityHint hint = availabilityHints.get(instance.nodeName());
        return hint != null ? hint : PartitionedDataLayer.AvailabilityHint.UNKNOWN;
    }

    private String snapshotKey(SidecarInstance instance)
    {
        return String.format("%s/%s/%d/%s/%s/%s",
                             datacenter, instance.hostname(), instance.port(), keyspace, table, snapshotName);
    }

    @Override
    public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                           @NotNull Range<BigInteger> range,
                                                           @NotNull CassandraInstance instance)
    {
        SidecarInstance sidecarInstance = sidecarInstanceMap.get(instance.nodeName());
        if (sidecarInstance == null)
        {
            throw new IllegalStateException("Could not find matching cassandra instance: " + instance.nodeName());
        }
        String key = snapshotKey(sidecarInstance);  // NOTE: We don't currently support token filtering in list snapshot
        LOGGER.info("Listing snapshot partition={} lowerBound={} upperBound={} "
                    + "instance={} port={} keyspace={} tableName={} snapshotName={}",
                    partitionId, range.lowerEndpoint(), range.upperEndpoint(),
                    sidecarInstance.hostname(), sidecarInstance.port(), maybeQuotedKeyspace, maybeQuotedTable, snapshotName);
        try
        {
            return SNAPSHOT_CACHE.get(key, () -> {
                LOGGER.info("Listing instance snapshot partition={} lowerBound={} upperBound={} "
                            + "instance={} port={} keyspace={} tableName={} snapshotName={} cacheKey={}",
                            partitionId, range.lowerEndpoint(), range.upperEndpoint(),
                            sidecarInstance.hostname(), sidecarInstance.port(), maybeQuotedKeyspace, maybeQuotedTable, snapshotName, key);
                return sidecar.listSnapshotFiles(sidecarInstance, maybeQuotedKeyspace, maybeQuotedTable, snapshotName, false)
                              .thenApply(response -> collectSSTableList(sidecarInstance, response, partitionId));
            }).thenApply(Collection::stream);
        }
        catch (ExecutionException exception)
        {
            CompletableFuture<Stream<SSTable>> future = new CompletableFuture<>();
            future.completeExceptionally(ThrowableUtils.rootCause(exception));
            return future;
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private List<SSTable> collectSSTableList(SidecarInstance sidecarInstance,
                                             ListSnapshotFilesResponse response,
                                             int partitionId)
    {
        if (response == null)
        {
            throw new IncompleteSSTableException();
        }
        List<ListSnapshotFilesResponse.FileInfo> snapshotFilesInfo = response.snapshotFilesInfo();
        if (snapshotFilesInfo == null)
        {
            throw new IncompleteSSTableException();
        }

        // Group SSTable components together
        Map<String, Map<FileType, ListSnapshotFilesResponse.FileInfo>> result = new LinkedHashMap<>(1024);
        for (ListSnapshotFilesResponse.FileInfo file : snapshotFilesInfo)
        {
            String fileName = file.fileName;
            int lastIndexOfDash = fileName.lastIndexOf('-');
            if (lastIndexOfDash < 0)
            {
                // E.g. dd manifest.json file
                continue;
            }
            String ssTableName = fileName.substring(0, lastIndexOfDash);
            try
            {
                FileType fileType = FileType.fromExtension(fileName.substring(lastIndexOfDash + 1));
                result.computeIfAbsent(ssTableName, k -> new LinkedHashMap<>())
                      .put(fileType, file);
            }
            catch (IllegalArgumentException ignore)
            {
                // Ignore unknown SSTable component types
            }
        }

        // Map to SSTable
        List<SSTable> sstables = result.values().stream()
                     .map(components -> new SidecarProvisionedSSTable(sidecar,
                                                                      sidecarClientConfig,
                                                                      sidecarInstance,
                                                                      maybeQuotedKeyspace,
                                                                      maybeQuotedTable,
                                                                      snapshotName,
                                                                      components,
                                                                      partitionId,
                                                                      stats()))
                     .collect(Collectors.toList());

        // Validate SSTable versions against expected versions from gossip
        validateSSTableVersions(sstables);

        return sstables;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), cqlTable, snapshotName, keyspace, table, version());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass() || !super.equals(other))
        {
            return false;
        }

        CassandraDataLayer that = (CassandraDataLayer) other;
        return cqlTable.equals(that.cqlTable)
               && snapshotName.equals(that.snapshotName)
               && keyspace.equals(that.keyspace)
               && table.equals(that.table)
               && version().equals(that.version());
    }

    public Map<String, BigNumberConfigImpl> bigNumberConfigMap()
    {
        return bigNumberConfigMap;
    }

    @Override
    public BigNumberConfig bigNumberConfig(CqlField field)
    {
        BigNumberConfigImpl config = bigNumberConfigMap.get(field.name());
        return config != null ? config : BigNumberConfig.DEFAULT;
    }

    /* Internal Cassandra SSTable */

    @VisibleForTesting
    public CassandraRing createCassandraRingFromRing(Partitioner partitioner,
                                                     ReplicationFactor replicationFactor,
                                                     RingResponse ring,
                                                     TokenRangeReplicasResponse tokenRangeReplicas)
    {
        Collection<CassandraInstance> instances = ring
                                                  .stream()
                                                  .filter(status -> datacenter == null || datacenter.equalsIgnoreCase(status.datacenter()))
                                                  .map(status -> new CassandraInstance(status.token(), status.fqdn(), status.datacenter()))
                                                  .collect(Collectors.toList());
        RangeMap<BigInteger, List<CassandraInstance>> replicasForRanges =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(tokenRangeReplicas, instances, datacenter);
        return new CassandraRing(partitioner, keyspace, replicationFactor, instances, replicasForRanges);
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        int timeoutSeconds = sidecarClientConfig.timeoutSeconds();
        StartupValidator.instance().register(new SidecarValidation(sidecar, timeoutSeconds));
        StartupValidator.instance().register(new CassandraValidation(sidecar, timeoutSeconds));
        StartupValidator.instance().perform();
    }

    /**
     * Uses the bridge {@link CassandraBridge#maybeQuoteIdentifier(String)} to attempt to quote the keyspace and table names
     * when the {@code quoteIdentifiers} option is enabled.
     */
    private void maybeQuoteKeyspaceAndTable()
    {
        if (quoteIdentifiers)
        {
            Objects.requireNonNull(bridge, "bridge must be initialized to maybe quote keyspace and table");
            maybeQuotedKeyspace = bridge.maybeQuoteIdentifier(keyspace);
            maybeQuotedTable = bridge.maybeQuoteIdentifier(table);
        }
        else
        {
            // default to unquoted
            maybeQuotedKeyspace = keyspace;
            maybeQuotedTable = table;
        }
    }

    // JDK Serialization

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK deserialization");
        this.bridge = CassandraBridgeFactory.get(CassandraVersion.valueOf(in.readUTF()));
        this.snapshotName = in.readUTF();
        this.keyspace = readNullable(in);
        this.table = readNullable(in);
        this.quoteIdentifiers = in.readBoolean();
        this.sidecarClientConfig = Sidecar.ClientConfig.create(in.readInt(),
                                                               in.readInt(),
                                                               in.readLong(),
                                                               in.readLong(),
                                                               in.readLong(),
                                                               in.readLong(),
                                                               in.readInt(),
                                                               in.readInt(),
                                                               readNullable(in),
                                                               (Map<FileType, Long>) in.readObject(),
                                                               (Map<FileType, Long>) in.readObject());
        this.sslConfig = (SslConfig) in.readObject();

        this.cqlTable = bridge.javaDeserialize(in, CqlTable.class);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        this.tokenPartitioner = (TokenPartitioner) in.readObject();
        this.sidecarInstances = in.readUTF();
        this.sidecarPort = in.readInt();
        this.clusterConfig = initializeClusterConfig(sidecarInstances, sidecarPort);
        this.availabilityHints = (Map<String, AvailabilityHint>) in.readObject();
        this.bigNumberConfigMap = (Map<String, BigNumberConfigImpl>) in.readObject();
        this.enableStats = in.readBoolean();
        this.readIndexOffset = in.readBoolean();
        this.useIncrementalRepair = in.readBoolean();
        this.lastModifiedTimestampField = readNullable(in);
        int features = in.readShort();
        List<SchemaFeature> requestedFeatures = new ArrayList<>(features);
        for (int feature = 0; feature < features; feature++)
        {
            String featureName = in.readUTF();
            requestedFeatures.add(SchemaFeatureSet.valueOf(featureName.toUpperCase()));
        }
        this.requestedFeatures = requestedFeatures;
        // Has alias for last modified timestamp
        if (this.lastModifiedTimestampField != null)
        {
            aliasLastModifiedTimestamp(this.requestedFeatures, this.lastModifiedTimestampField);
        }
        this.rfMap = (Map<String, ReplicationFactor>) in.readObject();
        this.timeProvider = new ReaderTimeProvider(in.readLong());
        this.sstableTimeRangeFilter = (SSTableTimeRangeFilter) in.readObject();
        this.sstableVersionsOnCluster = (Set<String>) in.readObject();
        this.maybeQuoteKeyspaceAndTable();
        this.initSidecarClient();
        this.initInstanceMap();
        this.startupValidate();
    }

    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK serialization");
        out.writeUTF(this.version().name());
        out.writeUTF(this.snapshotName);
        writeNullable(out, this.keyspace);
        writeNullable(out, this.table);
        out.writeBoolean(this.quoteIdentifiers);
        out.writeInt(this.sidecarClientConfig.userProvidedPort());
        out.writeInt(this.sidecarClientConfig.maxRetries());
        out.writeLong(this.sidecarClientConfig.millisToSleep());
        out.writeLong(this.sidecarClientConfig.maxMillisToSleep());
        out.writeLong(this.sidecarClientConfig.maxBufferSize());
        out.writeLong(this.sidecarClientConfig.chunkBufferSize());
        out.writeInt(this.sidecarClientConfig.maxPoolSize());
        out.writeInt(this.sidecarClientConfig.timeoutSeconds());
        writeNullable(out, this.sidecarClientConfig.cassandraRole());
        out.writeObject(this.sidecarClientConfig.maxBufferOverride());
        out.writeObject(this.sidecarClientConfig.chunkBufferOverride());
        out.writeObject(this.sslConfig);
        bridge.javaSerialize(out, this.cqlTable);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        out.writeObject(this.tokenPartitioner);
        out.writeUTF(this.sidecarInstances);
        out.writeInt(this.sidecarPort);
        out.writeObject(this.availabilityHints);
        out.writeObject(this.bigNumberConfigMap);
        out.writeBoolean(this.enableStats);
        out.writeBoolean(this.readIndexOffset);
        out.writeBoolean(this.useIncrementalRepair);
        // If lastModifiedTimestampField exist, it aliases the LMT field
        writeNullable(out, this.lastModifiedTimestampField);
        // Write the list of requested features: first write the size, then write the feature names
        out.writeShort(this.requestedFeatures.size());
        for (SchemaFeature feature : requestedFeatures)
        {
            out.writeUTF(feature.optionName());
        }
        out.writeObject(this.rfMap);
        out.writeLong(timeProvider.referenceEpochInSeconds());
        out.writeObject(this.sstableTimeRangeFilter);
        out.writeObject(this.sstableVersionsOnCluster);
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

    @Nullable
    private static String readNullable(ObjectInputStream in) throws IOException
    {
        if (in.readBoolean())
        {
            return in.readUTF();
        }
        return null;
    }

    /**
     * Validates that all SSTables being read have versions that were observed in gossip info.
     * This catches cases where SSTables have unexpected versions that weren't seen during driver initialization.
     * This validation runs on executors.
     *
     * @param sstables list of SSTables to validate
     * @throws UnsupportedOperationException if any SSTable has a version not in expected sstable versions
     */
    @VisibleForTesting
    void validateSSTableVersions(List<SSTable> sstables)
    {
        Set<String> expectedVersions = this.sstableVersionsOnCluster;
        // An empty (or null) expected set means SSTable version-based bridge selection was disabled
        // on the driver. In enabled mode the driver fails fast when no versions are observed, so
        // executors never see an empty set in that mode; hence empty here == disabled -> skip.
        // This avoids reading Spark configuration on executors (no SparkContext is available there).
        if (expectedVersions == null || expectedVersions.isEmpty())
        {
            LOGGER.debug("Skipping SSTable version validation on executor - no expected versions; "
                         + "SSTable version based bridge selection is disabled (set {}=true)",
                         BulkSparkConf.DISABLE_SSTABLE_VERSION_BASED_BRIDGE);
            return;
        }

        // Accept any SSTable version the already-selected read bridge can read, not just the exact set observed
        // in the driver's gossip snapshot. The bridge was determined on the driver (the highest version present)
        // and reconstructed here; a flush/compaction producing a still-readable version between the snapshot and
        // the read should not spuriously fail. Genuinely unreadable versions (e.g. a newer major than the bridge)
        // are still rejected.
        Set<String> readableVersions = bridge().getVersion().getSupportedSSTableVersionsForRead();

        for (SSTable ssTable : sstables)
        {
            String ssTableFileName = ssTable.getDataFileName();
            // Extract full version string (e.g., "big-nb" from the filename)
            String ssTableVersion = ssTable.getFormat() + "-" + ssTable.getVersion();

            if (!readableVersions.contains(ssTableVersion))
            {
                String errorMessage = String.format(
                "SSTable '%s' has version '%s' which is not readable by the bridge selected from cluster gossip info. " +
                "Versions observed in gossip: %s. Versions readable by the selected bridge: %s. " +
                "To retry by falling back to legacy mode for bridge selection, " +
                "set %s=true",
                ssTableFileName,
                ssTableVersion,
                expectedVersions,
                readableVersions,
                BulkSparkConf.DISABLE_SSTABLE_VERSION_BASED_BRIDGE);
                LOGGER.error(errorMessage);
                throw new UnsupportedOperationException(errorMessage);
            }
        }

        if (!sstables.isEmpty())
        {
            LOGGER.debug("Validated {} SSTable(s) against versions readable by the selected bridge: {}",
                         sstables.size(), readableVersions);
        }
    }

    // Kryo Serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraDataLayer>
    {
        @Override
        public void write(Kryo kryo, Output out, CassandraDataLayer dataLayer)
        {
            LOGGER.info("Serializing CassandraDataLayer with Kryo");
            out.writeString(dataLayer.keyspace);
            out.writeString(dataLayer.table);
            out.writeBoolean(dataLayer.quoteIdentifiers);
            out.writeString(dataLayer.snapshotName);
            out.writeString(dataLayer.datacenter);
            out.writeInt(dataLayer.sidecarClientConfig.userProvidedPort());
            out.writeInt(dataLayer.sidecarClientConfig.maxRetries());
            out.writeLong(dataLayer.sidecarClientConfig.millisToSleep());
            out.writeLong(dataLayer.sidecarClientConfig.maxMillisToSleep());
            out.writeLong(dataLayer.sidecarClientConfig.maxBufferSize());
            out.writeLong(dataLayer.sidecarClientConfig.chunkBufferSize());
            out.writeInt(dataLayer.sidecarClientConfig.maxPoolSize());
            out.writeInt(dataLayer.sidecarClientConfig.timeoutSeconds());
            out.writeString(dataLayer.sidecarClientConfig.cassandraRole());
            kryo.writeObject(out, dataLayer.sidecarClientConfig.maxBufferOverride());
            kryo.writeObject(out, dataLayer.sidecarClientConfig.chunkBufferOverride());
            kryo.writeObjectOrNull(out, dataLayer.sslConfig, SslConfig.class);
            kryo.writeObject(out, dataLayer.cqlTable);
            kryo.writeObject(out, dataLayer.tokenPartitioner);
            kryo.writeObject(out, dataLayer.version());
            kryo.writeObject(out, dataLayer.consistencyLevel);
            out.writeString(dataLayer.sidecarInstances);
            out.writeInt(dataLayer.sidecarPort);
            kryo.writeObject(out, dataLayer.availabilityHints);
            out.writeBoolean(dataLayer.bigNumberConfigMap.isEmpty());  // Kryo fails to deserialize bigNumberConfigMap map if empty
            if (!dataLayer.bigNumberConfigMap.isEmpty())
            {
                kryo.writeObject(out, dataLayer.bigNumberConfigMap);
            }
            out.writeBoolean(dataLayer.enableStats);
            out.writeBoolean(dataLayer.readIndexOffset);
            out.writeBoolean(dataLayer.useIncrementalRepair);
            // If lastModifiedTimestampField exist, it aliases the LMT field
            out.writeString(dataLayer.lastModifiedTimestampField);
            // Write the list of requested features: first write the size, then write the feature names
            SchemaFeaturesListWrapper listWrapper = new SchemaFeaturesListWrapper();
            listWrapper.requestedFeatureNames = dataLayer.requestedFeatures.stream()
                                                                           .map(SchemaFeature::optionName)
                                                                           .collect(Collectors.toList());
            kryo.writeObject(out, listWrapper);
            kryo.writeObject(out, dataLayer.rfMap);
            out.writeLong(dataLayer.timeProvider.referenceEpochInSeconds());
            kryo.writeObject(out, dataLayer.sstableTimeRangeFilter);
            kryo.writeObject(out, dataLayer.sstableVersionsOnCluster);
        }

        @SuppressWarnings("unchecked")
        @Override
        public CassandraDataLayer read(Kryo kryo, Input in, Class<CassandraDataLayer> type)
        {
            LOGGER.info("Deserializing CassandraDataLayer with Kryo");

            return new CassandraDataLayer(
            in.readString(),
            in.readString(),
            in.readBoolean(),
            in.readString(),
            in.readString(),
            Sidecar.ClientConfig.create(in.readInt(),
                                        in.readInt(),
                                        in.readLong(),
                                        in.readLong(),
                                        in.readLong(),
                                        in.readLong(),
                                        in.readInt(),
                                        in.readInt(),
                                        in.readString(),
                                        (Map<FileType, Long>) kryo.readObject(in, HashMap.class),
                                        (Map<FileType, Long>) kryo.readObject(in, HashMap.class)),
            kryo.readObjectOrNull(in, SslConfig.class),
            kryo.readObject(in, CqlTable.class),
            kryo.readObject(in, TokenPartitioner.class),
            kryo.readObject(in, CassandraVersion.class),
            kryo.readObject(in, ConsistencyLevel.class),
            in.readString(), // sidecarInstances
            in.readInt(), // sidecarPort
            (Map<String, PartitionedDataLayer.AvailabilityHint>) kryo.readObject(in, HashMap.class),
            in.readBoolean() ? Collections.emptyMap()
                             : (Map<String, BigNumberConfigImpl>) kryo.readObject(in, HashMap.class),
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean(),
            in.readString(),
            kryo.readObject(in, SchemaFeaturesListWrapper.class).toList(),
            kryo.readObject(in, HashMap.class),
            new ReaderTimeProvider(in.readLong()),
            kryo.readObject(in, SSTableTimeRangeFilter.class),
            kryo.readObject(in, HashSet.class));
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

    protected Set<SidecarInstance> initializeClusterConfig(ClientConfig options)
    {
        return initializeClusterConfig(options.sidecarContactPoints, options.sidecarPort());
    }

    // not intended to be overridden
    private Set<SidecarInstance> initializeClusterConfig(String sidecarInstances, int sidecarPort)
    {
        return Arrays.stream(sidecarInstances.split(","))
                     .filter(StringUtils::isNotEmpty)
                     .map(hostname -> SidecarInstanceFactory.createFromString(hostname, sidecarPort))
                     .collect(Collectors.toSet());
    }

    protected String getEffectiveCassandraVersionForRead(Set<SidecarInstance> clusterConfig,
                                                         NodeSettings nodeSettings)
    {
        return nodeSettings.releaseVersion();
    }

    protected void dialHome(@NotNull ClientConfig options)
    {
        LOGGER.info("Dial home. clientConfig={}", options);
    }

    protected void clearSnapshot(Set<SidecarInstance> clusterConfig, @NotNull ClientConfig options)
    {
        if (maybeQuotedKeyspace == null || maybeQuotedTable == null)
        {
            LOGGER.warn("Bridge was never initialized. Skipping clearing snapshots. This implies snapshots were never created");
            return;
        }

        LOGGER.info("Clearing snapshot at end of Spark job snapshotName={} keyspace={} table={} dc={}",
                    snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter);
        CountDownLatch latch = new CountDownLatch(sidecarInstanceMap.size());
        try
        {
            for (SidecarInstance instance : sidecarInstanceMap.values())
            {
                sidecar.clearSnapshot(instance, maybeQuotedKeyspace, maybeQuotedTable, snapshotName).whenComplete((resp, throwable) -> {
                    try
                    {
                        if (throwable != null)
                        {
                            LOGGER.warn("Failed to clear snapshot on instance hostname={} port={} snapshotName={} keyspace={} table={} datacenter={}",
                                        instance.hostname(), instance.port(), snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter, throwable);
                        }
                    }
                    finally
                    {
                        latch.countDown();
                    }
                });
            }
            await(latch);
            LOGGER.info("Snapshot cleared snapshotName={} keyspace={} table={} datacenter={}",
                        snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter);
        }
        catch (Throwable throwable)
        {
            LOGGER.warn("Unexpected exception clearing snapshot snapshotName={} keyspace={} table={} dc={}",
                        snapshotName, maybeQuotedKeyspace, maybeQuotedTable, datacenter, throwable);
        }
    }

    /**
     * Returns the {@link Sizing} object based on the {@code sizing} option provided by the user,
     * or {@link DefaultSizing} as the default sizing
     *
     * @param ringFuture        a future with a view of the ring
     * @param replicationFactor the replication factor
     * @param options           the {@link ClientConfig} options
     * @return the {@link Sizing} object based on the {@code sizing} option provided by the user
     */
    protected Sizing getSizing(CompletableFuture<RingResponse> ringFuture,
                               ReplicationFactor replicationFactor,
                               ClientConfig options)
    {
        return SizingFactory.create(replicationFactor, options, consistencyLevel, keyspace, table, datacenter, sidecar, sidecarPort, ringFuture);
    }

    protected void await(CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    static void aliasLastModifiedTimestamp(List<SchemaFeature> requestedFeatures, String alias)
    {
        SchemaFeature featureAlias = new SchemaFeature()
        {
            @Override
            public String optionName()
            {
                return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName();
            }

            @Override
            public String fieldName()
            {
                return alias;
            }

            @Override
            public DataType fieldDataType()
            {
                return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.fieldDataType();
            }

            @Override
            public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
            {
                return new LastModifiedTimestampDecorator<>(builder, alias);
            }

            @Override
            public boolean fieldNullable()
            {
                return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.fieldNullable();
            }
        };
        int index = requestedFeatures.indexOf(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP);
        if (index >= 0)
        {
            requestedFeatures.set(index, featureAlias);
        }
    }
}
