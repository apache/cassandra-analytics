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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.spark.common.stats.LogStatsPublisher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base class for BulkWriterContext implementations.
 *
 * <p>Serialization Architecture:</p>
 * <p>This class is NOT serialized directly. Instead:
 * <ol>
 *   <li>Driver creates BulkWriterContext using constructor</li>
 *   <li>Driver extracts BulkWriterConfig in {@link CassandraBulkSourceRelation} constructor</li>
 *   <li>BulkWriterConfig gets broadcast to executors</li>
 *   <li>Executors reconstruct BulkWriterContext via {@link BulkWriterConfig#toBulkWriterContext()}</li>
 * </ol>
 *
 * <p>Broadcastable wrappers used in BulkWriterConfig:
 * <ul>
 *   <li>{@link IBroadcastableClusterInfo} -&gt; reconstructs to {@link CassandraClusterInfo} or {@link CassandraClusterInfoGroup}</li>
 *   <li>{@link BroadcastableJobInfo} -&gt; reconstructs to {@link CassandraJobInfo}</li>
 *   <li>{@link BroadcastableSchemaInfo} -&gt; reconstructs to {@link CassandraSchemaInfo}</li>
 * </ul>
 *
 * <p>Implements KryoSerializable with fail-fast approach to detect missing Kryo registration.
 */
public abstract class AbstractBulkWriterContext implements BulkWriterContext, KryoSerializable
{
    // log as the concrete implementation; but use private to not expose the logger to implementations
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private final BulkSparkConf conf;
    private final int sparkDefaultParallelism;
    private final JobInfo jobInfo;
    private final ClusterInfo clusterInfo;
    private final SchemaInfo schemaInfo;
    private final String bridgeVersion;
    // Note: do not declare transient fields as final; but they need to be volatile as there could be contention when recreating them after deserialization
    // For the transient field, they are assigned null once deserialized, remember to use getOrRebuildAfterDeserialization for their getters
    private transient volatile CassandraBridge bridge;
    private transient volatile JobStatsPublisher jobStatsPublisher;
    private transient volatile TransportContext transportContext;

    /**
     * Constructor for driver usage.
     * Builds all components fresh on the driver.
     *
     * @param conf                    Bulk Spark configuration
     * @param structType              DataFrame schema
     * @param sparkDefaultParallelism Spark default parallelism
     */
    protected AbstractBulkWriterContext(@NotNull BulkSparkConf conf,
                                        @NotNull StructType structType,
                                        @NotNull int sparkDefaultParallelism)
    {
        this.conf = conf;
        this.sparkDefaultParallelism = sparkDefaultParallelism;

        // Build everything fresh on driver
        this.clusterInfo = buildClusterInfo();
        this.clusterInfo.startupValidate();
        this.bridgeVersion = findBridgeVersion();
        this.bridge = buildCassandraBridge();
        this.jobInfo = buildJobInfo();
        this.schemaInfo = buildSchemaInfo(structType);
        this.jobStatsPublisher = buildJobStatsPublisher();
        this.transportContext = buildTransportContext(true);  // isOnDriver = true
    }

    /**
     * Constructor for executor usage.
     * Reconstructs components from broadcast configuration on executors.
     * This is used by {@link BulkWriterConfig#toBulkWriterContext()}.
     *
     * @param config immutable configuration for the bulk writer with pre-computed values
     */
    protected AbstractBulkWriterContext(@NotNull BulkWriterConfig config)
    {
        this.conf = config.getConf();
        this.sparkDefaultParallelism = config.getSparkDefaultParallelism();

        // Reconstruct from broadcast data on executor
        this.clusterInfo = reconstructClusterInfoOnExecutor(config.getBroadcastableClusterInfo());
        this.bridgeVersion = config.getBridgeVersion();
        this.bridge = buildCassandraBridge();
        this.jobInfo = reconstructJobInfoOnExecutor(config.getBroadcastableJobInfo());
        this.schemaInfo = reconstructSchemaInfoOnExecutor(config.getBroadcastableSchemaInfo());
        this.jobStatsPublisher = buildJobStatsPublisher();
        this.transportContext = buildTransportContext(false);  // isOnDriver = false
    }

    public final BulkSparkConf bulkSparkConf()
    {
        return conf;
    }

    protected final int sparkDefaultParallelism()
    {
        return sparkDefaultParallelism;
    }

    protected String bridgeVersion()
    {
        return bridgeVersion;
    }

    /*---  Methods to build required fields   ---*/

    protected abstract ClusterInfo buildClusterInfo();

    /**
     * Reconstructs ClusterInfo on executors from broadcastable versions.
     * This method is only called on executors when reconstructing BulkWriterContext from
     * broadcast BulkWriterConfig. Each broadcastable type knows how to reconstruct itself
     * into the appropriate full ClusterInfo implementation.
     *
     * @param clusterInfo the BroadcastableClusterInfo from broadcast
     * @return reconstructed ClusterInfo (CassandraClusterInfo or CassandraClusterInfoGroup)
     */
    protected ClusterInfo reconstructClusterInfoOnExecutor(IBroadcastableClusterInfo clusterInfo)
    {
        return clusterInfo.reconstruct();
    }

    /**
     * Reconstructs JobInfo on executors from BroadcastableJobInfo.
     * This method is only called on executors when reconstructing BulkWriterContext from
     * broadcast BulkWriterConfig. It rebuilds CassandraJobInfo with TokenPartitioner reconstructed
     * from the broadcastable partition mappings.
     *
     * @param jobInfo the BroadcastableJobInfo from broadcast
     * @return reconstructed CassandraJobInfo
     */
    protected JobInfo reconstructJobInfoOnExecutor(BroadcastableJobInfo jobInfo)
    {
        return new CassandraJobInfo(jobInfo);
    }

    /**
     * Reconstructs SchemaInfo on executors from BroadcastableSchemaInfo.
     * This method is only called on executors when reconstructing BulkWriterContext from
     * broadcast BulkWriterConfig. It reconstructs CassandraSchemaInfo and TableSchema from
     * the broadcast data (no Sidecar calls needed).
     *
     * @param schemaInfo the BroadcastableSchemaInfo from broadcast
     * @return reconstructed CassandraSchemaInfo
     */
    protected SchemaInfo reconstructSchemaInfoOnExecutor(BroadcastableSchemaInfo schemaInfo)
    {
        return new CassandraSchemaInfo(schemaInfo);
    }

    protected abstract void validateKeyspaceReplication();

    protected JobInfo buildJobInfo()
    {
        validateKeyspaceReplication();
        BulkSparkConf conf = bulkSparkConf();
        TokenRangeMapping<RingInstance> tokenRangeMapping = cluster().getTokenRangeMapping(true);
        TokenPartitioner tokenPartitioner = new TokenPartitioner(tokenRangeMapping,
                                                                 conf.numberSplits,
                                                                 sparkDefaultParallelism(),
                                                                 conf.getCores());
        return new CassandraJobInfo(conf, generateRestoreJobIds(), tokenPartitioner);
    }

    /**
     * Generate the restore job IDs used in the receiving Cassandra Sidecar clusters.
     * In the coordinated write mode, there should be a unique uuid per cluster;
     * In the single cluster write mode, the MultiClusterContainer would contain one single entry.
     * @return restore job ids that are unique per cluster
     */
    protected abstract MultiClusterContainer<UUID> generateRestoreJobIds();

    protected CassandraBridge buildCassandraBridge()
    {
        return CassandraBridgeFactory.get(bridgeVersion());
    }

    protected TransportContext buildTransportContext(boolean isOnDriver)
    {
        return createTransportContext(isOnDriver);
    }

    protected JobStatsPublisher buildJobStatsPublisher()
    {
        return new LogStatsPublisher();
    }

    protected String findBridgeVersion()
    {
        return cluster().getBridgeVersion();
    }

    protected SchemaInfo buildSchemaInfo(StructType structType)
    {
        QualifiedTableName tableName = job().qualifiedTableName();
        String keyspace = tableName.keyspace();
        String table = tableName.table();
        String keyspaceSchema = cluster().getKeyspaceSchema(true);
        Partitioner partitioner = cluster().getPartitioner();
        String createTableSchema = CqlUtils.extractTableSchema(keyspaceSchema, keyspace, table);
        Set<String> udts = CqlUtils.extractUdts(keyspaceSchema, keyspace);
        ReplicationFactor replicationFactor = CqlUtils.extractReplicationFactor(keyspaceSchema, keyspace);
        int indexCount = CqlUtils.extractIndexCount(keyspaceSchema, keyspace, table);
        CqlTable cqlTable = bridge().buildSchema(createTableSchema, keyspace, replicationFactor, partitioner, udts, null, indexCount, false);

        TableInfoProvider tableInfoProvider = new CqlTableInfoProvider(createTableSchema, cqlTable);
        TableSchema tableSchema = initializeTableSchema(bulkSparkConf(), structType, tableInfoProvider, bridgeVersion());
        return new CassandraSchemaInfo(tableSchema, udts);
    }

    /*-------------------------------------------*/

    @Override
    public JobInfo job()
    {
        return jobInfo;
    }

    @Override
    public ClusterInfo cluster()
    {
        return clusterInfo;
    }

    @Override
    public SchemaInfo schema()
    {
        return schemaInfo;
    }

    @Override
    public CassandraBridge bridge()
    {
        bridge = getOrRebuildAfterDeserialization(() -> bridge, this::buildCassandraBridge);
        return bridge;
    }

    @Override
    public JobStatsPublisher jobStats()
    {
        jobStatsPublisher = getOrRebuildAfterDeserialization(() -> jobStatsPublisher, this::buildJobStatsPublisher);
        return jobStatsPublisher;
    }

    @Override
    public TransportContext transportContext()
    {
        transportContext = getOrRebuildAfterDeserialization(() -> transportContext, () -> buildTransportContext(false));
        return transportContext;
    }

    @Override
    public void shutdown()
    {
        logger.info("Shutting down bulk writer context. contextClass={}", getClass().getSimpleName());

        if (clusterInfo != null)
        {
            clusterInfo.close();
        }

        if (transportContext != null)
        {
            transportContext.close();
        }
    }

    @NotNull
    protected TableSchema initializeTableSchema(@NotNull BulkSparkConf conf,
                                                @NotNull StructType dfSchema,
                                                TableInfoProvider tableInfoProvider,
                                                String bridgeVersion)
    {
        return new TableSchema(dfSchema,
                               tableInfoProvider,
                               conf.writeMode,
                               conf.getTTLOptions(),
                               conf.getTimestampOptions(),
                               bridgeVersion,
                               job().qualifiedTableName().quoteIdentifiers(),
                               conf.skipSecondaryIndexCheck);
    }

    @NotNull
    protected TransportContext createTransportContext(boolean isOnDriver)
    {
        BulkSparkConf conf = bulkSparkConf();
        return conf.getTransportInfo()
                   .getTransport()
                   .createContext(this, conf, isOnDriver);
    }

    /**
     * Use the implementation of the KryoSerializable interface as a detection device to make sure
     * {@link org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator} is properly in place.
     * <p>
     * If this class is serialized by Kryo, it means we're <b>not</b> set up correctly, and therefore we log and fail.
     * This failure will occur early in the job and be very clear, so users can quickly fix their code and get up and
     * running again, rather than having a random NullPointerException further down the line.
     */
    public static final String KRYO_REGISTRATION_WARNING =
    "Spark Bulk Writer Kryo Registrator (SbwKryoRegistrator) was not registered with Spark - "
    + "please see the README.md file for more details on how to register the Spark Bulk Writer.";

    @Override
    public void write(Kryo kryo, Output output)
    {
        failIfKryoNotRegistered();
    }

    @Override
    public void read(Kryo kryo, Input input)
    {
        failIfKryoNotRegistered();
    }

    private void failIfKryoNotRegistered()
    {
        logger.error(KRYO_REGISTRATION_WARNING);
        throw new RuntimeException(KRYO_REGISTRATION_WARNING);
    }

    // returns immediately if current supplies non-null value; otherwise, it invokes the builder in a synchronized block to only build once
    private <T> T getOrRebuildAfterDeserialization(Supplier<T> current, Supplier<T> builder)
    {
        T t = current.get();
        if (t != null)
        {
            return t;
        }

        synchronized (this)
        {
            t = current.get();
            if (t != null)
            {
                return t;
            }

            return builder.get();
        }
    }
}
