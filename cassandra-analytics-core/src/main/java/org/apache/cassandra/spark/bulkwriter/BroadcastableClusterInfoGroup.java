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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterSupport;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Broadcastable implementation of BroadcastableClusterInfo for coordinated writes with ZERO transient fields.
 * This class wraps multiple BroadcastableCluster instances for multi-cluster scenarios.
 * Only essential fields are broadcast; executors reconstruct CassandraClusterInfoGroup to fetch other data from Sidecar.
 * NO LOGGER - to avoid logger references in broadcast variable.
 */
public final class BroadcastableClusterInfoGroup implements BroadcastableClusterInfo, MultiClusterSupport<BroadcastableClusterInfo>
{
    private static final long serialVersionUID = 1L;

    // Essential fields broadcast to executors - list of BroadcastableCluster
    private final List<BroadcastableClusterInfo> clusterInfos;
    // Pre-compute and store the map
    private final Map<String, BroadcastableClusterInfo> clusterInfoById;
    // Cache for getConf() - all clusters share the same BulkSparkConf
    private final BulkSparkConf conf;

    /**
     * Creates a BroadcastableClusterInfoGroup from a source ClusterInfo group.
     * Executors will reconstruct CassandraClusterInfoGroup to fetch data from Sidecar.
     *
     * @param source the source ClusterInfo (typically CassandraClusterInfoGroup)
     * @param conf   the BulkSparkConf needed to connect to Sidecar on executors
     */
    public static BroadcastableClusterInfoGroup from(@NotNull MultiClusterSupport<ClusterInfo> source,
                                                    @NotNull BulkSparkConf conf)
    {
        List<BroadcastableClusterInfo> broadcastableInfos = new ArrayList<>();
        source.forEach((clusterId, clusterInfo) -> {
            broadcastableInfos.add(BroadcastableCluster.from(clusterInfo, conf));
        });
        return new BroadcastableClusterInfoGroup(broadcastableInfos, conf);
    }

    private BroadcastableClusterInfoGroup(List<BroadcastableClusterInfo> clusterInfos, BulkSparkConf conf)
    {
        this.clusterInfos = Collections.unmodifiableList(clusterInfos);
        this.conf = conf;
        // Pre-compute the map
        this.clusterInfoById = clusterInfos.stream()
                                          .collect(Collectors.toMap(BroadcastableClusterInfo::clusterId, Function.identity()));
    }

    @Override
    @NotNull
    public BulkSparkConf getConf()
    {
        return conf;
    }

    @Override
    public String getLowestCassandraVersion()
    {
        if (clusterInfos.size() == 1)
        {
            return clusterInfos.get(0).getLowestCassandraVersion();
        }

        Map<String, String> aggregated = applyOnEach(BroadcastableClusterInfo::getLowestCassandraVersion);
        List<CassandraVersionFeatures> versions = aggregated.values()
                                                            .stream()
                                                            .map(CassandraVersionFeatures::cassandraVersionFeaturesFromCassandraVersion)
                                                            .sorted()
                                                            .collect(Collectors.toList());
        CassandraVersionFeatures first = versions.get(0);
        CassandraVersionFeatures last = versions.get(versions.size() - 1);
        Preconditions.checkState(first.getMajorVersion() == last.getMajorVersion(),
                                 "Cluster versions are not compatible. lowest=%s and highest=%s",
                                 first.getRawVersionString(), last.getRawVersionString());

        return first.getRawVersionString();
    }

    @Override
    public Partitioner getPartitioner()
    {
        Map<String, Partitioner> aggregated = applyOnEach(BroadcastableClusterInfo::getPartitioner);
        Set<Partitioner> partitioners = EnumSet.copyOf(aggregated.values());
        if (partitioners.size() != 1)
        {
            throw new IllegalStateException("Clusters are not running with the same partitioner kind. Found partitioners: " + aggregated);
        }

        return partitioners.iterator().next();
    }

    @Override
    @Nullable
    public String clusterId()
    {
        return "BroadcastableClusterInfoGroup: [" + String.join(", ", applyOnEach(BroadcastableClusterInfo::clusterId).values()) + ']';
    }

    // MultiClusterSupport methods
    @Override
    public int size()
    {
        return clusterInfos.size();
    }

    @Override
    public void forEach(BiConsumer<String, BroadcastableClusterInfo> action)
    {
        clusterInfoById.forEach(action);
    }

    @Nullable
    @Override
    public BroadcastableClusterInfo getValueOrNull(@NotNull String clusterId)
    {
        return clusterInfoById.get(clusterId);
    }

    private <T> Map<String, T> applyOnEach(Function<BroadcastableClusterInfo, T> action)
    {
        Map<String, T> aggregated = new LinkedHashMap<>(clusterInfos.size());
        for (BroadcastableClusterInfo clusterInfo : clusterInfos)
        {
            try
            {
                aggregated.put(clusterInfo.clusterId(), action.apply(clusterInfo));
            }
            catch (Throwable cause)
            {
                throw toRuntimeException(clusterInfo, cause);
            }
        }
        return aggregated;
    }

    private RuntimeException toRuntimeException(BroadcastableClusterInfo clusterInfo, Throwable cause)
    {
        // No logger in broadcast variable - error will be propagated via exception
        return new RuntimeException("Failed to perform action on cluster: " + clusterInfo.clusterId(), cause);
    }
}
