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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.spark.bulkwriter.AbstractBulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.DataTransport;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * BulkWriterContext for coordinated write
 * The context requires the coordinated-write configuration to be present.
 */
public class CassandraCoordinatedBulkWriterContext extends AbstractBulkWriterContext
{
    private static final long serialVersionUID = -2296507634642008675L;
    private CassandraClusterInfoGroup clusterInfoGroup;

    public CassandraCoordinatedBulkWriterContext(@NotNull BulkSparkConf conf,
                                                 @NotNull StructType structType,
                                                 int sparkDefaultParallelism)
    {
        super(conf, structType, sparkDefaultParallelism);
        Preconditions.checkArgument(conf.isCoordinatedWriteConfigured(),
                                    "Cannot create CassandraCoordinatedBulkWriterContext without CoordinatedWrite configuration");
        // Redundant check, since isCoordinatedWriteConfigured implies using S3_COMPAT mode already.
        // Having it here for clarity.
        Preconditions.checkArgument(conf.getTransportInfo().getTransport() == DataTransport.S3_COMPAT,
                                    "CassandraCoordinatedBulkWriterContext can only be created with " + DataTransport.S3_COMPAT);
    }

    @Override
    protected ClusterInfo buildClusterInfo()
    {
        clusterInfoGroup = CassandraClusterInfoGroup.fromBulkSparkConf(bulkSparkConf());
        clusterInfoGroup.startupValidate();
        return clusterInfoGroup;
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
            ClusterInfo cluster = clusterInfoGroup.getValueOrNull(clusterId);
            boolean isReplicatedToLocalDc = !StringUtils.isEmpty(localDc)
                                            && cluster != null
                                            && cluster.replicationFactor().getOptions().containsKey(localDc);
            Preconditions.checkState(isReplicatedToLocalDc,
                                     "Keyspace %s is not replicated on datacenter %s in %s",
                                     conf.keyspace, localDc, clusterId);
        });
    }
}
