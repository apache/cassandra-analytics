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

public abstract class AbstractBulkWriterContext implements BulkWriterContext, KryoSerializable
{
    private static final long serialVersionUID = -6526396615116954510L;
    // log as the concrete implementation; but use private to not expose the logger to implementations
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private final BulkSparkConf conf;
    private final int sparkDefaultParallelism;
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
        this.conf = conf;
        this.sparkDefaultParallelism = sparkDefaultParallelism;
        // Note: build sequence matters
        this.clusterInfo = buildClusterInfo();
        this.clusterInfo.startupValidate();
        this.lowestCassandraVersion = findLowestCassandraVersion();
        this.bridge = buildCassandraBridge();
        this.jobInfo = buildJobInfo();
        this.schemaInfo = buildSchemaInfo(structType);
        this.jobStatsPublisher = buildJobStatsPublisher();
        this.transportContext = buildTransportContext(true);
    }

    protected final BulkSparkConf bulkSparkConf()
    {
        return conf;
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
        UUID restoreJobId = bridge().getTimeUUID(); // used for creating restore job on sidecar
        TokenPartitioner tokenPartitioner = new TokenPartitioner(tokenRangeMapping,
                                                                 conf.numberSplits,
                                                                 sparkDefaultParallelism(),
                                                                 conf.getCores());
        return new CassandraJobInfo(conf, restoreJobId, tokenPartitioner);
    }

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
        CqlTable cqlTable = bridge().buildSchema(createTableSchema, keyspace, replicationFactor, partitioner, udts, null, indexCount);

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
