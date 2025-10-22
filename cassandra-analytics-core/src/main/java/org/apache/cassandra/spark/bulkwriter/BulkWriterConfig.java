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

import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable configuration data class for BulkWriter jobs that is safe to broadcast to Spark executors.
 * This class contains pre-computed, serializable values that were computed on the driver.
 * <p>
 * Serialization Architecture:
 * This class is the ONLY object that gets broadcast to Spark executors (via Spark's broadcast mechanism).
 * It contains serializable implementations of cluster information ({@link SerializableClusterInfo} or
 * {@link SerializableClusterInfoGroup}) that have zero transient fields for safe serialization.
 * <p>
 * On the driver, {@link BulkWriterContext} instances use driver-only implementations like
 * {@link CassandraClusterInfo}. Before broadcasting, these are converted to serializable forms.
 * On executors, {@link BulkWriterContext} instances are reconstructed from this config using
 * {@link BulkWriterContext#from(BulkWriterConfig, boolean)}, which uses the serializable cluster
 * information directly without converting back to driver-only types.
 */
public final class BulkWriterConfig implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final BulkSparkConf conf;
    private final StructType structType;
    private final int sparkDefaultParallelism;
    private final JobInfo jobInfo;
    private final ClusterInfo clusterInfo;
    private final SchemaInfo schemaInfo;
    private final String lowestCassandraVersion;

    /**
     * Creates a new immutable BulkWriterConfig with pre-computed values
     *
     * @param conf                    Bulk writer Spark configuration
     * @param structType              DataFrame schema structure
     * @param sparkDefaultParallelism Spark default parallelism setting
     * @param jobInfo                 Pre-computed job information
     * @param clusterInfo             Pre-computed cluster information
     * @param schemaInfo              Pre-computed schema information
     * @param lowestCassandraVersion  Lowest Cassandra version in the cluster
     */
    public BulkWriterConfig(@NotNull BulkSparkConf conf,
                            @NotNull StructType structType,
                            int sparkDefaultParallelism,
                            @NotNull JobInfo jobInfo,
                            @NotNull ClusterInfo clusterInfo,
                            @NotNull SchemaInfo schemaInfo,
                            @NotNull String lowestCassandraVersion)
    {
        this.conf = conf;
        this.structType = structType;
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

    public StructType getStructType()
    {
        return structType;
    }

    public int getSparkDefaultParallelism()
    {
        return sparkDefaultParallelism;
    }

    public JobInfo getJobInfo()
    {
        return jobInfo;
    }

    public ClusterInfo getClusterInfo()
    {
        return clusterInfo;
    }

    public SchemaInfo getSchemaInfo()
    {
        return schemaInfo;
    }

    public String getLowestCassandraVersion()
    {
        return lowestCassandraVersion;
    }
}
