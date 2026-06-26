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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterSupport;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Broadcastable wrapper for coordinated writes with ZERO transient fields to optimize Spark broadcasting.
 * <p>
 * This class wraps multiple BroadcastableCluster instances for multi-cluster scenarios.
 * Pre-computed values (partitioner, bridgeVersion) are extracted from CassandraClusterInfoGroup on the driver
 * to avoid duplicating aggregation/validation logic on executors.
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
public final class BroadcastableClusterInfoGroup implements IBroadcastableClusterInfo, MultiClusterSupport<IBroadcastableClusterInfo>
{
    private static final long serialVersionUID = 8621255452042082506L;

    private final List<BroadcastableClusterInfo> clusterInfos;
    private final String clusterId;
    private final BulkSparkConf conf;
    private final Partitioner partitioner;
    private final String bridgeVersion;

    /**
     * Creates a BroadcastableClusterInfoGroup from a source ClusterInfo group.
     * Extracts pre-computed values (partitioner, bridgeVersion) from the source
     * to avoid duplicating aggregation/validation logic on executors.
     *
     * @param source the source CassandraClusterInfoGroup
     * @param conf   the BulkSparkConf needed to connect to Sidecar on executors
     */
    public static BroadcastableClusterInfoGroup from(@NotNull CassandraClusterInfoGroup source,
                                                     @NotNull BulkSparkConf conf)
    {
        List<BroadcastableClusterInfo> broadcastableInfos = new ArrayList<>();
        source.forEach((clusterId, clusterInfo) -> broadcastableInfos.add(BroadcastableClusterInfo.from(clusterInfo, conf)));

        // Extract pre-computed values from CassandraClusterInfoGroup
        // These have already been validated/computed on the driver
        Partitioner partitioner = source.getPartitioner();
        String bridgeVersion = source.getBridgeVersion();

        return new BroadcastableClusterInfoGroup(broadcastableInfos, source.clusterId(), conf, partitioner, bridgeVersion);
    }

    private BroadcastableClusterInfoGroup(List<BroadcastableClusterInfo> clusterInfos,
                                          String clusterId,
                                          BulkSparkConf conf,
                                          Partitioner partitioner,
                                          String bridgeVersion)
    {
        this.clusterInfos = Collections.unmodifiableList(clusterInfos);
        this.conf = conf;
        this.clusterId = clusterId;
        this.partitioner = partitioner;
        this.bridgeVersion = bridgeVersion;
    }

    @Override
    @NotNull
    public BulkSparkConf getConf()
    {
        return conf;
    }

    @Override
    public String getBridgeVersion()
    {
        // Return pre-computed value from CassandraClusterInfoGroup
        // No need to duplicate aggregation/validation logic
        return bridgeVersion;
    }

    @Override
    public Partitioner getPartitioner()
    {
        // Return pre-computed value from CassandraClusterInfoGroup
        // No need to duplicate validation logic
        return partitioner;
    }

    @Override
    public String clusterId()
    {
        return clusterId;
    }

    // MultiClusterSupport methods
    @Override
    public int size()
    {
        return clusterInfos.size();
    }

    @Override
    public void forEach(BiConsumer<String, IBroadcastableClusterInfo> action)
    {
        clusterInfos.forEach(info -> action.accept(info.clusterId(), info));
    }

    @Nullable
    @Override
    public IBroadcastableClusterInfo getValueOrNull(@NotNull String clusterId)
    {
        throw new UnsupportedOperationException("getValueOrNull should not be called from BroadcastableClusterInfoGroup");
    }

    @Override
    public ClusterInfo reconstruct()
    {
        return CassandraClusterInfoGroup.from(this);
    }
}
