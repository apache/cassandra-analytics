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

import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Broadcastable implementation of BroadcastableClusterInfo with ZERO transient fields.
 * This class is designed for Spark broadcasting to avoid SizeEstimator overhead from inspecting transient fields.
 * Only essential fields are broadcast; executors reconstruct CassandraClusterInfo to fetch other data from Sidecar.
 * NO LOGGER - to avoid logger references in broadcast variable.
 */
public final class BroadcastableCluster implements BroadcastableClusterInfo
{
    private static final long serialVersionUID = 1L;

    // Essential fields broadcast to executors
    private final Partitioner partitioner;
    private final String cassandraVersion;
    private final String clusterId;
    private final BulkSparkConf conf;

    /**
     * Creates a BroadcastableCluster from a CassandraClusterInfo by extracting essential fields.
     * Executors will reconstruct CassandraClusterInfo to fetch other data from Sidecar.
     *
     * @param source the source ClusterInfo (typically CassandraClusterInfo)
     * @param conf   the BulkSparkConf needed to connect to Sidecar on executors
     */
    public static BroadcastableCluster from(@NotNull ClusterInfo source, @NotNull BulkSparkConf conf)
    {
        return new BroadcastableCluster(
            source.getPartitioner(),
            source.getLowestCassandraVersion(),
            source.clusterId(),
            conf
        );
    }

    private BroadcastableCluster(@NotNull Partitioner partitioner,
                                    @NotNull String cassandraVersion,
                                    @Nullable String clusterId,
                                    @NotNull BulkSparkConf conf)
    {
        this.partitioner = partitioner;
        this.cassandraVersion = cassandraVersion;
        this.clusterId = clusterId;
        this.conf = conf;
    }

    public BulkSparkConf getConf()
    {
        return conf;
    }

    @Override
    public String getLowestCassandraVersion()
    {
        return cassandraVersion;
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
}
