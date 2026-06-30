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

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal interface for cluster information that can be safely broadcast to Spark executors.
 * This interface contains only the essential methods that broadcastable cluster info implementations
 * ({@link BroadcastableClusterInfo}, {@link BroadcastableClusterInfoGroup}) need to provide.
 * <p>
 * Unlike {@link ClusterInfo}, this interface doesn't include methods that require fresh data
 * from Cassandra Sidecar or runtime operations. These implementations are designed to be broadcast
 * and then reconstructed to full {@link ClusterInfo} instances on executors.
 * <p>
 * Methods in this interface:
 * <ul>
 *   <li>{@link #getPartitioner()} - static cluster partitioner configuration</li>
 *   <li>{@link #getBridgeVersion()} - pre-computed version string</li>
 *   <li>{@link #clusterId()} - cluster identifier (optional)</li>
 *   <li>{@link #getConf()} - BulkSparkConf needed for reconstruction on executors</li>
 *   <li>{@link #reconstruct()} - reconstructs full ClusterInfo instance on executors</li>
 * </ul>
 */
public interface IBroadcastableClusterInfo extends Serializable
{
    /**
     * @return the partitioner used by the cluster
     */
    Partitioner getPartitioner();

    /**
     * @return the pre-computed Cassandra bridge version determined on the driver
     */
    CassandraVersion getBridgeVersion();

    /**
     * ID string that can uniquely identify a cluster.
     * When writing to a single cluster, this may be null.
     * When in coordinated write mode (writing to multiple clusters), this must return a unique string.
     *
     * @return cluster id string, null if absent
     */
    @Nullable
    String clusterId();

    /**
     * @return the BulkSparkConf configuration needed to reconstruct ClusterInfo on executors
     */
    @NotNull
    BulkSparkConf getConf();

    /**
     * Reconstructs a full ClusterInfo instance from this broadcastable data on executors.
     * Each implementation knows how to reconstruct itself into the appropriate ClusterInfo type.
     * This allows adding new broadcastable types without modifying the reconstruction logic
     * in {@link AbstractBulkWriterContext}.
     *
     * @return reconstructed ClusterInfo (CassandraClusterInfo or CassandraClusterInfoGroup)
     */
    ClusterInfo reconstruct();
}
