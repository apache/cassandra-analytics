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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * BulkWriterContext implementation for single cluster write operations.
 * <p>
 * This class does NOT have a serialVersionUID because it is never directly serialized.
 * See {@link AbstractBulkWriterContext} for details on the serialization architecture.
 */
// CHECKSTYLE IGNORE: This class cannot be declared as final, because consumers should be able to extend it
public class CassandraBulkWriterContext extends AbstractBulkWriterContext
{
    // A temporary CassandraClusterInfo created with bridgeVersion=null during driver-side initialization.
    // This preliminaryClusterInfo provides the Sidecar connectivity needed to determine the bridge version.
    // Once bridge version is determined, buildClusterInfo() promotes this by setting its bridge version.
    // Marked transient because it is only used during the driver-side constructor and must not be serialized
    // when broadcasting to executors.
    private transient CassandraClusterInfo preliminaryClusterInfo;

    protected CassandraBulkWriterContext(@NotNull BulkSparkConf conf,
                                         @NotNull StructType structType,
                                         int sparkDefaultParallelism)
    {
        super(conf, structType, sparkDefaultParallelism);
    }

    /**
     * Constructor used by {@link BulkWriterConfig#toBulkWriterContext()}.
     * This constructor is only used on executors to reconstruct context from broadcast config.
     *
     * @param config immutable configuration for the bulk writer
     */
    protected CassandraBulkWriterContext(@NotNull BulkWriterConfig config)
    {
        super(config);
    }

    @Override
    protected String getLowestCassandraVersion(@NotNull BulkSparkConf conf)
    {
        return getOrCreatePreliminaryClusterInfo(conf).getLowestCassandraVersion();
    }

    @Override
    protected Set<String> getSSTableVersionsOnCluster(@NotNull BulkSparkConf conf)
    {
        return getOrCreatePreliminaryClusterInfo(conf).getSSTableVersionsOnCluster();
    }

    @Override
    protected ClusterInfo buildClusterInfo(CassandraVersion bridgeVersion)
    {
        CassandraClusterInfo clusterInfo = getOrCreatePreliminaryClusterInfo(bulkSparkConf());
        preliminaryClusterInfo = null;
        clusterInfo.setBridgeVersion(bridgeVersion);
        return clusterInfo;
    }

    private CassandraClusterInfo getOrCreatePreliminaryClusterInfo(BulkSparkConf conf)
    {
        if (preliminaryClusterInfo == null)
        {
            preliminaryClusterInfo = new CassandraClusterInfo(conf, null);
        }

        return preliminaryClusterInfo;
    }

    @Override
    protected void validateKeyspaceReplication()
    {
        BulkSparkConf conf = bulkSparkConf();
        // no validation for non-local CL
        if (!conf.consistencyLevel.isLocal())
        {
            return;
        }
        // localDc is not empty and replication option contains localDc
        boolean isReplicatedToLocalDc = !StringUtils.isEmpty(conf.localDC)
                                        && cluster().replicationFactor().getOptions().containsKey(conf.localDC);
        Preconditions.checkState(isReplicatedToLocalDc, "Keyspace %s is not replicated on datacenter %s", conf.keyspace, conf.localDC);
    }

    @Override
    protected MultiClusterContainer<UUID> generateRestoreJobIds()
    {
        return MultiClusterContainer.ofSingle(bridge().getTimeUUID());
    }

    @Override
    public BulkWriterConfig toBulkWriterConfigForBroadcasting(JavaSparkContext sparkContext)
    {
        IBroadcastableClusterInfo broadcastableClusterInfo = BroadcastableClusterInfo.from(cluster(), bulkSparkConf());
        BroadcastableJobInfo broadcastableJobInfo = BroadcastableJobInfo.from(job(), bulkSparkConf());
        BroadcastableSchemaInfo broadcastableSchemaInfo = BroadcastableSchemaInfo.from(schema());

        return new BulkWriterConfig(bulkSparkConf(),
                                    sparkContext.defaultParallelism(),
                                    broadcastableJobInfo,
                                    broadcastableClusterInfo,
                                    broadcastableSchemaInfo,
                                    bridgeVersion());
    }
}
