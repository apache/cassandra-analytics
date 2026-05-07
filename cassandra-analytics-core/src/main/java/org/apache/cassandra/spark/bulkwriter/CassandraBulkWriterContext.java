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

import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

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
    protected ClusterInfo buildClusterInfo()
    {
        return new CassandraClusterInfo(bulkSparkConf());
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
        ClusterInfo originalClusterInfo = cluster();

        // Extract only broadcast-safe cluster metadata

        // ClusterInfo has transient fields (CassandraContext, token mappings) that are not serializable
        IBroadcastableClusterInfo broadcastableClusterInfo = BroadcastableClusterInfo.from(originalClusterInfo, bulkSparkConf());

        // TokenPartitioner contains a Logger field that is not serializable and expensive for SizeEstimator
        BroadcastableJobInfo broadcastableJobInfo = BroadcastableJobInfo.from(job(), bulkSparkConf());

        // TableSchema contains a Logger field that is not serializable and expensive for SizeEstimator
        BroadcastableSchemaInfo broadcastableSchemaInfo = BroadcastableSchemaInfo.from(schema());

        return new BulkWriterConfig(
                bulkSparkConf(),
                sparkContext.defaultParallelism(),
                broadcastableJobInfo,
                broadcastableClusterInfo,
                broadcastableSchemaInfo,
                lowestCassandraVersion()
        );
    }
}
