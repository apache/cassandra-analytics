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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
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
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.BigNumberConfigImpl;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.clients.ExecutorHolder;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.spark.common.SidecarInstanceFactory;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.sparksql.LastModifiedTimestampDecorator;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.ReaderTimeProvider;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.validation.CassandraValidation;
import org.apache.cassandra.spark.validation.SidecarValidation;
import org.apache.cassandra.spark.validation.StartupValidatable;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.util.ShutdownHookManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    // volatile in order to publish the reference for visibility
    protected volatile CqlTable cqlTable;
    protected transient TimeProvider timeProvider;
    protected transient SidecarClient sidecar;

    private SslConfig sslConfig;

    @VisibleForTesting
    transient Map<String, SidecarInstance> instanceMap;

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
                                 @NotNull CassandraVersion version,
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
                                 TimeProvider timeProvider)
    {
        super(consistencyLevel, datacenter);
        this.snapshotName = snapshotName;
        this.bridge = CassandraBridgeFactory.get(version);
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
        this.maybeQuoteKeyspaceAndTable();
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
        initInstanceMap();

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
        bridge = CassandraBridgeFactory.get(cassandraVersion);
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
        rfMap = ImmutableMap.of(keyspace, replicationFactor);
        CompletableFuture<Integer> sizingFuture = CompletableFuture.supplyAsync(
        () -> getSizing(clusterConfig, replicationFactor, options).getEffectiveNumberOfCores(),
        ExecutorHolder.EXECUTOR_SERVICE);
        validateReplicationFactor(replicationFactor);
        udts.forEach(udt -> LOGGER.info("Adding schema UDT: '{}'", udt));

        cqlTable = bridge().buildSchema(createStmt, keyspace, replicationFactor, partitioner, udts, null, indexCount, false);
        CassandraRing ring = createCassandraRingFromRing(partitioner, replicationFactor, ringFuture.get());

        int effectiveNumberOfCores = sizingFuture.get();
        tokenPartitioner = new TokenPartitioner(ring, options.defaultParallelism(), effectiveNumberOfCores);
        return effectiveNumberOfCores;
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

        // Fire off create snapshot request across the entire cluster
        List<CompletableFuture<Void>> futures =
        ring.stream()
            .filter(ringEntry -> datacenter == null || datacenter.equals(ringEntry.datacenter()))
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
        instanceMap = clusterConfig.stream().collect(Collectors.toMap(SidecarInstance::hostname, Function.identity()));
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
        LOGGER.info("Initialized CassandraDataLayer instanceMap numInstances={}", instanceMap.size());
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
        SidecarInstance sidecarInstance = instanceMap.get(instance.nodeName());
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
                return sidecar.listSnapshotFiles(sidecarInstance, maybeQuotedKeyspace, maybeQuotedTable, snapshotName)
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
        return result.values().stream()
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
                                                     RingResponse ring)
    {
        Collection<CassandraInstance> instances = ring
                                                  .stream()
                                                  .filter(status -> datacenter == null || datacenter.equalsIgnoreCase(status.datacenter()))
                                                  .map(status -> new CassandraInstance(status.token(), status.fqdn(), status.datacenter()))
                                                  .collect(Collectors.toList());
        return new CassandraRing(partitioner, keyspace, replicationFactor, instances);
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
        this.timeProvider = new ReaderTimeProvider(in.readInt());
        this.maybeQuoteKeyspaceAndTable();
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
        out.writeInt(timeProvider.referenceEpochInSeconds());
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
            out.writeInt(dataLayer.timeProvider.referenceEpochInSeconds());
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
            new ReaderTimeProvider(in.readInt()));
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
        CountDownLatch latch = new CountDownLatch(clusterConfig.size());
        try
        {
            for (SidecarInstance instance : clusterConfig)
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
     * @param clusterConfig     the cluster configuration
     * @param replicationFactor the replication factor
     * @param options           the {@link ClientConfig} options
     * @return the {@link Sizing} object based on the {@code sizing} option provided by the user
     */
    protected Sizing getSizing(Set<SidecarInstance> clusterConfig,
                               ReplicationFactor replicationFactor,
                               ClientConfig options)
    {
        return new DefaultSizing(options.numCores());
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
