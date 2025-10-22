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
 * <p>
 * Serialization Architecture:
 * This class does NOT have a serialVersionUID because it is never directly serialized via Java serialization.
 * It implements KryoSerializable with a fail-fast approach to detect missing Kryo registration
 * (see {@link org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator}).
 * <p>
 * When BulkWriterConfig is broadcast to executors, the config contains all necessary immutable data.
 * On executors, BulkWriterContext instances are reconstructed from the config using
 * {@link BulkWriterContext#from(BulkWriterConfig, boolean)}, not by deserializing BulkWriterContext directly.
 * <p>
 * Transient fields in this class are lazily rebuilt on executors when accessed, using the
 * {@link #getOrRebuildAfterDeserialization} pattern for Kryo serialization safety.
 */
public abstract class AbstractBulkWriterContext implements BulkWriterContext, KryoSerializable
{
    // log as the concrete implementation; but use private to not expose the logger to implementations
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private final BulkSparkConf conf;
    private final int sparkDefaultParallelism;
    private final StructType structType;  // Store for config extraction
    private final JobInfo jobInfo;
    private final ClusterInfo clusterInfo;
    private final SchemaInfo schemaInfo;
    private final String lowestCassandraVersion;
    // Note: do not declare transient fields as final; but they need to be volatile as there could be contention when recreating them after deserialization
    // For the transient field, they are assigned null once deserialized, remember to use getOrRebuildAfterDeserialization for their getters
    private transient volatile CassandraBridge bridge;
    private transient volatile JobStatsPublisher jobStatsPublisher;
    private transient volatile TransportContext transportContext;

    protected AbstractBulkWriterContext(@NotNull BulkSparkConf conf,
                                        @NotNull StructType structType,
                                        @NotNull int sparkDefaultParallelism)
    {
        this(conf, structType, sparkDefaultParallelism, null, null, null, null, true);
    }

    /**
     * Constructor that accepts a BulkWriterConfig and whether this is on the driver.
     * This is used by the factory method {@link BulkWriterContext#from(BulkWriterConfig, boolean)}.
     *
     * @param config     immutable configuration for the bulk writer with pre-computed values
     * @param isOnDriver true if on driver, false if on executor
     */
    protected AbstractBulkWriterContext(@NotNull BulkWriterConfig config, boolean isOnDriver)
    {
        this(config.getConf(),
             config.getStructType(),
             config.getSparkDefaultParallelism(),
             config.getJobInfo(),
             config.getClusterInfo(),
             config.getSchemaInfo(),
             config.getLowestCassandraVersion(),
             isOnDriver);
    }

    /**
     * Internal constructor that initializes all fields.
     *
     * @param conf                    Bulk Spark configuration
     * @param structType              DataFrame schema
     * @param sparkDefaultParallelism Spark default parallelism
     * @param precomputedJobInfo      Pre-computed JobInfo (null to compute)
     * @param precomputedClusterInfo  Pre-computed ClusterInfo (null to compute)
     * @param precomputedSchemaInfo   Pre-computed SchemaInfo (null to compute)
     * @param precomputedVersion      Pre-computed Cassandra version (null to compute)
     * @param isOnDriver              true if on driver, false if on executor
     */
    private AbstractBulkWriterContext(@NotNull BulkSparkConf conf,
                                      @NotNull StructType structType,
                                      int sparkDefaultParallelism,
                                      JobInfo precomputedJobInfo,
                                      ClusterInfo precomputedClusterInfo,
                                      SchemaInfo precomputedSchemaInfo,
                                      String precomputedVersion,
                                      boolean isOnDriver)
    {
        this.conf = conf;
        this.structType = structType;
        this.sparkDefaultParallelism = sparkDefaultParallelism;
        // Note: build sequence matters
        // Use pre-computed values if available (from broadcast), otherwise compute them
        this.clusterInfo = precomputedClusterInfo != null ? precomputedClusterInfo : buildClusterInfo();
        if (precomputedClusterInfo == null)
        {
            this.clusterInfo.startupValidate();
        }
        this.lowestCassandraVersion = precomputedVersion != null ? precomputedVersion : findLowestCassandraVersion();
        this.bridge = buildCassandraBridge();
        this.jobInfo = precomputedJobInfo != null ? precomputedJobInfo : buildJobInfo();
        this.schemaInfo = precomputedSchemaInfo != null ? precomputedSchemaInfo : buildSchemaInfo(structType);
        this.jobStatsPublisher = buildJobStatsPublisher();
        this.transportContext = buildTransportContext(isOnDriver);
    }

    public final BulkSparkConf bulkSparkConf()
    {
        return conf;
    }

    public final StructType structType()
    {
        return structType;
    }

    protected final int sparkDefaultParallelism()
    {
        return sparkDefaultParallelism;
    }

    protected String lowestCassandraVersion()
    {
        return lowestCassandraVersion;
    }

    /*---  Methods to build required fields   ---*/

    protected abstract ClusterInfo buildClusterInfo();

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
        return CassandraBridgeFactory.get(lowestCassandraVersion());
    }

    protected TransportContext buildTransportContext(boolean isOnDriver)
    {
        return createTransportContext(isOnDriver);
    }

    protected JobStatsPublisher buildJobStatsPublisher()
    {
        return new LogStatsPublisher();
    }

    protected String findLowestCassandraVersion()
    {
        return cluster().getLowestCassandraVersion();
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
        TableSchema tableSchema = initializeTableSchema(bulkSparkConf(), structType, tableInfoProvider, lowestCassandraVersion());
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
                                                String lowestCassandraVersion)
    {
        return new TableSchema(dfSchema,
                               tableInfoProvider,
                               conf.writeMode,
                               conf.getTTLOptions(),
                               conf.getTimestampOptions(),
                               lowestCassandraVersion,
                               job().qualifiedTableName().quoteIdentifiers());
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
