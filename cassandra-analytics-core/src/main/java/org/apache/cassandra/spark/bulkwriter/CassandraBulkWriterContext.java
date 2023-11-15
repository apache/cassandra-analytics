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

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.ShutdownHookManager;
import org.jetbrains.annotations.NotNull;

// CHECKSTYLE IGNORE: This class cannot be declared as final, because consumers should be able to extend it
public class CassandraBulkWriterContext implements BulkWriterContext, KryoSerializable
{
    private static final long serialVersionUID = 8241993502687688783L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBulkWriterContext.class);

    @NotNull
    private final BulkSparkConf conf;
    private final JobInfo jobInfo;
    private transient DataTransferApi dataTransferApi;
    private final CassandraClusterInfo clusterInfo;
    private final SchemaInfo schemaInfo;

    private CassandraBulkWriterContext(@NotNull BulkSparkConf conf,
                                       @NotNull CassandraClusterInfo clusterInfo,
                                       @NotNull StructType dfSchema,
                                       SparkContext sparkContext)
    {
        this.conf = conf;
        this.clusterInfo = clusterInfo;
        String lowestCassandraVersion = clusterInfo.getLowestCassandraVersion();
        CassandraBridge bridge = CassandraBridgeFactory.get(lowestCassandraVersion);

        CassandraRing<RingInstance> ring = clusterInfo.getRing(true);
        jobInfo = new CassandraJobInfo(conf,
                                       new TokenPartitioner(ring, conf.numberSplits, sparkContext.defaultParallelism(), conf.getCores()));
        Preconditions.checkArgument(!conf.consistencyLevel.isLocal()
                                    || (conf.localDC != null && ring.getReplicationFactor().getOptions().containsKey(conf.localDC)),
                                    String.format("Keyspace %s is not replicated on datacenter %s",
                                                  conf.keyspace, conf.localDC));

        String keyspace = jobInfo.keyspace();
        String table = jobInfo.tableName();

        String keyspaceSchema = clusterInfo.getKeyspaceSchema(true);
        Partitioner partitioner = clusterInfo.getPartitioner();
        String createTableSchema = CqlUtils.extractTableSchema(keyspaceSchema, keyspace, table);
        Set<String> udts = CqlUtils.extractUdts(keyspaceSchema, keyspace);
        ReplicationFactor replicationFactor = CqlUtils.extractReplicationFactor(keyspaceSchema, keyspace);
        int indexCount = CqlUtils.extractIndexCount(keyspaceSchema, keyspace, table);
        CqlTable cqlTable = bridge.buildSchema(createTableSchema, keyspace, replicationFactor, partitioner, udts, null, indexCount);

        TableInfoProvider tableInfoProvider = new CqlTableInfoProvider(createTableSchema, cqlTable);
        TableSchema tableSchema = initializeTableSchema(conf, dfSchema, tableInfoProvider, lowestCassandraVersion);
        schemaInfo = new CassandraSchemaInfo(tableSchema);
    }

    public static BulkWriterContext fromOptions(@NotNull SparkContext sparkContext,
                                                @NotNull Map<String, String> strOptions,
                                                @NotNull StructType dfSchema)
    {
        Preconditions.checkNotNull(dfSchema);

        BulkSparkConf conf = new BulkSparkConf(sparkContext.getConf(), strOptions);
        CassandraClusterInfo clusterInfo = new CassandraClusterInfo(conf);

        clusterInfo.startupValidate();

        CassandraBulkWriterContext bulkWriterContext = new CassandraBulkWriterContext(conf, clusterInfo, dfSchema, sparkContext);
        ShutdownHookManager.addShutdownHook(org.apache.spark.util.ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY(),
                                            ScalaFunctions.wrapLambda(bulkWriterContext::shutdown));
        bulkWriterContext.dialHome(sparkContext.version());

        return bulkWriterContext;
    }

    @Override
    public void shutdown()
    {
        LOGGER.info("Shutting down {}", this);
        synchronized (this)
        {
            if (clusterInfo != null)
            {
                clusterInfo.close();
            }
        }
    }

    /**
     * Use the implementation of the KryoSerializable interface as a detection device to make sure the Spark Bulk
     * Writer's KryoRegistrator is properly in place.
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
        LOGGER.error(KRYO_REGISTRATION_WARNING);
        throw new RuntimeException(KRYO_REGISTRATION_WARNING);
    }

    protected void dialHome(String sparkVersion)
    {
        LOGGER.info("Dial home. clientConfig={}, sparkVersion={}", conf, sparkVersion);
    }

    @Override
    @NotNull
    public ClusterInfo cluster()
    {
        return clusterInfo;
    }

    @Override
    @NotNull
    public JobInfo job()
    {
        return jobInfo;
    }

    @Override
    public SchemaInfo schema()
    {
        return schemaInfo;
    }

    @Override
    @NotNull
    public synchronized DataTransferApi transfer()
    {
        if (dataTransferApi == null)
        {
            CassandraBridge bridge = CassandraBridgeFactory.get(clusterInfo.getLowestCassandraVersion());
            dataTransferApi = new SidecarDataTransferApi(clusterInfo.getCassandraContext(),
                                                         bridge,
                                                         jobInfo,
                                                         conf);
        }
        return dataTransferApi;
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
                               conf.quoteIdentifiers);
    }
}
