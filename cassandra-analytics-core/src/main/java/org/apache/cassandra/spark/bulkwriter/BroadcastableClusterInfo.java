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

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Broadcastable wrapper for single cluster with ZERO transient fields to optimize Spark broadcasting.
 * <p>
 * Only essential fields are broadcast; executors reconstruct CassandraClusterInfo to fetch other data from Sidecar.
 * <p>
 * <b>Why ZERO transient fields matters:</b><br>
 * Spark's {@link org.apache.spark.util.SizeEstimator} uses reflection to estimate object sizes before broadcasting.
 * Each transient field forces SizeEstimator to inspect the field's type hierarchy, which is expensive.
 * Logger references are particularly costly due to their deep object graphs (appenders, layouts, contexts).
 * By eliminating ALL transient fields and Logger references, we:
 * <ul>
 *   <li>Minimize SizeEstimator reflection overhead during broadcast preparation</li>
 *   <li>Reduce broadcast variable serialization size</li>
 *   <li>Avoid accidental serialization of non-serializable objects</li>
 * </ul>
 */
public final class BroadcastableClusterInfo implements IBroadcastableClusterInfo
{
    private static final long serialVersionUID = 4506917240637924802L;

    // Essential fields broadcast to executors
    private final Partitioner partitioner;
    private final String clusterId;
    private final BulkSparkConf conf;

    /**
     * Creates a BroadcastableCluster from a CassandraClusterInfo by extracting essential fields.
     * Executors will reconstruct CassandraClusterInfo to fetch other data from Sidecar.
     *
     * @param source the source ClusterInfo (typically CassandraClusterInfo)
     * @param conf   the BulkSparkConf needed to connect to Sidecar on executors
     */
    public static BroadcastableClusterInfo from(@NotNull ClusterInfo source, @NotNull BulkSparkConf conf)
    {
        return new BroadcastableClusterInfo(source.getPartitioner(),
                                            source.clusterId(),
                                            conf);
    }

    private BroadcastableClusterInfo(@NotNull Partitioner partitioner,
                                     @Nullable String clusterId,
                                     @NotNull BulkSparkConf conf)
    {
        this.partitioner = partitioner;
        this.clusterId = clusterId;
        this.conf = conf;
    }

    @NotNull
    public BulkSparkConf getConf()
    {
        return conf;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    @Nullable
    public String clusterId()
    {
        return clusterId;
    }

    @Override
    public ClusterInfo reconstruct(CassandraVersion bridgeVersion)
    {
        return new CassandraClusterInfo(this, bridgeVersion);
    }
}
