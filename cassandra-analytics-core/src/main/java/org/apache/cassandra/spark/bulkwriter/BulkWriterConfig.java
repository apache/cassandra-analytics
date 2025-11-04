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

import java.io.Serializable;

import org.jetbrains.annotations.NotNull;

/**
 * Immutable configuration data class for BulkWriter jobs that is safe to broadcast to Spark executors.
 * This class contains pre-computed, serializable values that were computed on the driver.
 * <p>
 * Serialization Architecture:
 * This class is the ONLY object that gets broadcast to Spark executors (via Spark's broadcast mechanism).
 * It contains broadcastable wrapper implementations with ZERO transient fields and NO Logger references:
 * <ul>
 *   <li>{@link BroadcastableClusterInfo} or {@link BroadcastableClusterInfoGroup} - cluster metadata</li>
 *   <li>{@link BroadcastableJobInfo} - job configuration with {@link BroadcastableTokenPartitioner}</li>
 *   <li>{@link BroadcastableSchemaInfo} - schema metadata</li>
 * </ul>
 * <p>
 * On the driver, {@link BulkWriterContext} instances use driver-only implementations:
 * {@link CassandraClusterInfo}, {@link CassandraJobInfo}, {@link CassandraSchemaInfo}.
 * Before broadcasting, these are converted to broadcastable wrappers to avoid Logger references
 * and minimize Spark SizeEstimator overhead.
 * <p>
 * On executors, {@link BulkWriterContext} instances are reconstructed from this config using
 * {@link BulkWriterContext#from(BulkWriterConfig)}, which detects the broadcastable
 * wrappers and reconstructs the full implementations with fresh data from Cassandra Sidecar.
 */
public final class BulkWriterConfig implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final BulkSparkConf conf;
    private final int sparkDefaultParallelism;
    private final BroadcastableJobInfo jobInfo;
    // BroadcastableClusterInfo can be either BroadcastableCluster or BroadcastableClusterInfoGroup
    private final IBroadcastableClusterInfo clusterInfo;
    private final BroadcastableSchemaInfo schemaInfo;
    private final String lowestCassandraVersion;

    /**
     * Creates a new immutable BulkWriterConfig with pre-computed values
     *
     * @param conf                    Bulk writer Spark configuration
     * @param sparkDefaultParallelism Spark default parallelism setting
     * @param jobInfo                 Broadcastable job information
     * @param clusterInfo             Broadcastable cluster information (BroadcastableCluster or BroadcastableClusterInfoGroup)
     * @param schemaInfo              Broadcastable schema information
     * @param lowestCassandraVersion  Lowest Cassandra version in the cluster
     */
    public BulkWriterConfig(@NotNull BulkSparkConf conf,
                            int sparkDefaultParallelism,
                            @NotNull BroadcastableJobInfo jobInfo,
                            @NotNull IBroadcastableClusterInfo clusterInfo,
                            @NotNull BroadcastableSchemaInfo schemaInfo,
                            @NotNull String lowestCassandraVersion)
    {
        this.conf = conf;
        this.sparkDefaultParallelism = sparkDefaultParallelism;
        this.jobInfo = jobInfo;
        this.clusterInfo = clusterInfo;
        this.schemaInfo = schemaInfo;
        this.lowestCassandraVersion = lowestCassandraVersion;
    }

    public BulkSparkConf getConf()
    {
        return conf;
    }

    public int getSparkDefaultParallelism()
    {
        return sparkDefaultParallelism;
    }

    public BroadcastableJobInfo getBroadcastableJobInfo()
    {
        return jobInfo;
    }

    public IBroadcastableClusterInfo getBroadcastableClusterInfo()
    {
        return clusterInfo;
    }

    public BroadcastableSchemaInfo getBroadcastableSchemaInfo()
    {
        return schemaInfo;
    }

    public String getLowestCassandraVersion()
    {
        return lowestCassandraVersion;
    }
}
