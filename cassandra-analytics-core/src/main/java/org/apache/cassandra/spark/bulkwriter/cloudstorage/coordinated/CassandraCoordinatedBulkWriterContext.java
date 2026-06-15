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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.AbstractBulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.BroadcastableClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.BroadcastableJobInfo;
import org.apache.cassandra.spark.bulkwriter.BroadcastableSchemaInfo;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.BulkWriterConfig;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.DataTransport;
import org.apache.cassandra.spark.bulkwriter.IBroadcastableClusterInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * BulkWriterContext for coordinated write to multiple clusters.
 * The context requires the coordinated-write configuration to be present.
 * <p>
 * This class does NOT have a serialVersionUID because it is never directly serialized.
 * See {@link AbstractBulkWriterContext} for details on the serialization architecture.
 */
public class CassandraCoordinatedBulkWriterContext extends AbstractBulkWriterContext
{
    // A temporary CassandraClusterInfoGroup created with bridgeVersion=null during driver-side initialization.
    // This preliminaryGroup provides the Sidecar connectivity needed to determine the bridge version.
    // Once bridge version is determined, buildClusterInfo() promotes this by setting its bridge version.
    // Marked transient because it is only used during the driver-side constructor and must not be serialized
    // when broadcasting to executors.
    private transient CassandraClusterInfoGroup preliminaryGroup;

    public CassandraCoordinatedBulkWriterContext(@NotNull BulkSparkConf conf,
                                                 @NotNull StructType structType,
                                                 int sparkDefaultParallelism)
    {
        super(conf, structType, sparkDefaultParallelism);
        validateConfiguration(conf);
    }

    /**
     * Constructor used by {@link BulkWriterConfig#toBulkWriterContext()}.
     * This constructor is only used on executors to reconstruct context from broadcast config.
     *
     * @param config immutable configuration for the bulk writer
     */
    public CassandraCoordinatedBulkWriterContext(@NotNull BulkWriterConfig config)
    {
        super(config);
        validateConfiguration(config.getConf());
    }

    private void validateConfiguration(BulkSparkConf conf)
    {
        Preconditions.checkArgument(conf.isCoordinatedWriteConfigured(),
                                    "Cannot create CassandraCoordinatedBulkWriterContext without CoordinatedWrite configuration");
        // Redundant check, since isCoordinatedWriteConfigured implies using S3_COMPAT mode already.
        // Having it here for clarity.
        Preconditions.checkArgument(conf.getTransportInfo().getTransport() == DataTransport.S3_COMPAT,
                                    "CassandraCoordinatedBulkWriterContext can only be created with " + DataTransport.S3_COMPAT);
    }

    @Override
    protected String getLowestCassandraVersion(@NotNull BulkSparkConf conf)
    {
        return getOrCreatePreliminaryGroup(conf).getLowestCassandraVersion();
    }

    @Override
    protected Set<String> getSSTableVersionsOnCluster(@NotNull BulkSparkConf conf)
    {
        return getOrCreatePreliminaryGroup(conf).getSSTableVersionsOnCluster();
    }

    @Override
    protected ClusterInfo buildClusterInfo(CassandraVersion bridgeVersion)
    {
        CassandraClusterInfoGroup group = getOrCreatePreliminaryGroup(bulkSparkConf());
        preliminaryGroup = null;
        group.setBridgeVersion(bridgeVersion);
        group.startupValidate();
        return group;
    }

    private CassandraClusterInfoGroup getOrCreatePreliminaryGroup(BulkSparkConf conf)
    {
        if (preliminaryGroup == null)
        {
            preliminaryGroup = CassandraClusterInfoGroup.fromBulkSparkConf(conf, (CassandraVersion) null);
        }

        return preliminaryGroup;
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
        CoordinatedWriteConf coordinatedWriteConf = conf.coordinatedWriteConf();
        // all specified clusters contain the corresponding replication DCs
        coordinatedWriteConf.clusters().forEach((clusterId, clusterConf) -> {
            // localDc is not empty and replication option contains localDc
            String localDc = clusterConf.localDc();
            ClusterInfo cluster = clusterInfoGroup().getValueOrNull(clusterId);
            boolean isReplicatedToLocalDc = !StringUtils.isEmpty(localDc)
                                            && cluster != null
                                            && cluster.replicationFactor().getOptions().containsKey(localDc);
            Preconditions.checkState(isReplicatedToLocalDc,
                                     "Keyspace %s is not replicated on datacenter %s in %s",
                                     conf.keyspace, localDc, clusterId);
        });
    }

    @Override
    protected MultiClusterContainer<UUID> generateRestoreJobIds()
    {
        MultiClusterContainer<UUID> result = new MultiClusterContainer<>();
        clusterInfoGroup().forEach((clusterId, ignored) -> result.setValue(clusterId, bridge().getTimeUUID()));
        return result;
    }

    protected CassandraClusterInfoGroup clusterInfoGroup()
    {
        return (CassandraClusterInfoGroup) cluster();
    }

    @Override
    public BulkWriterConfig toBulkWriterConfigForBroadcasting(JavaSparkContext sparkContext)
    {
        CassandraClusterInfoGroup multiCluster = clusterInfoGroup();
        IBroadcastableClusterInfo broadcastableClusterInfo = BroadcastableClusterInfoGroup.from(multiCluster, bulkSparkConf());
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
