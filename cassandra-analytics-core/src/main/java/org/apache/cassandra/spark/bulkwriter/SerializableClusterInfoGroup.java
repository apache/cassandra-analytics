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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterSupport;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Serializable implementation of ClusterInfo for coordinated writes with ZERO transient fields.
 * This class wraps multiple SerializableClusterInfo instances and delegates to them.
 * All caches are non-transient to avoid SizeEstimator overhead from inspecting transient fields.
 * NO LOGGER - to avoid logger references in broadcast variable.
 */
public final class SerializableClusterInfoGroup implements ClusterInfo, MultiClusterSupport<ClusterInfo>
{
    private static final long serialVersionUID = 1L;

    // All fields are non-transient - no lazy loading with transient fields
    private final List<ClusterInfo> clusterInfos;
    // Pre-compute and store instead of lazy loading
    private final Map<String, ClusterInfo> clusterInfoById;
    // Volatile for lazy initialization with double-checked locking
    private volatile TokenRangeMapping<RingInstance> consolidatedTokenRangeMapping;

    /**
     * Creates a SerializableClusterInfoGroup from a source ClusterInfo group
     *
     * @param source the source ClusterInfo (typically CassandraClusterInfoGroup)
     * @param conf   the BulkSparkConf needed to rebuild CassandraContext on executors
     */
    public static SerializableClusterInfoGroup from(@NotNull MultiClusterSupport<ClusterInfo> source,
                                                    @NotNull BulkSparkConf conf)
    {
        List<ClusterInfo> serializableInfos = new ArrayList<>();
        source.forEach((clusterId, clusterInfo) -> {
            serializableInfos.add(SerializableClusterInfo.from(clusterInfo, conf));
        });
        return new SerializableClusterInfoGroup(serializableInfos);
    }

    private SerializableClusterInfoGroup(List<ClusterInfo> clusterInfos)
    {
        this.clusterInfos = Collections.unmodifiableList(clusterInfos);
        // Pre-compute the map instead of lazy loading
        this.clusterInfoById = clusterInfos.stream()
                                          .collect(Collectors.toMap(ClusterInfo::clusterId, Function.identity()));
    }

    @Override
    public void refreshClusterInfo()
    {
        // No-op - this is an immutable snapshot
    }

    @Override
    public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
    {
        if (clusterInfos.size() == 1)
        {
            return clusterInfos.get(0).getTokenRangeMapping(cached);
        }

        if (!cached || consolidatedTokenRangeMapping == null)
        {
            synchronized (this)
            {
                if (cached && consolidatedTokenRangeMapping != null)
                {
                    return consolidatedTokenRangeMapping;
                }
                Map<String, TokenRangeMapping<RingInstance>> aggregated = applyOnEach(c -> c.getTokenRangeMapping(cached));
                consolidatedTokenRangeMapping = TokenRangeMapping.consolidate(new ArrayList<>(aggregated.values()));
            }
        }

        return consolidatedTokenRangeMapping;
    }

    @Override
    public String getLowestCassandraVersion()
    {
        if (clusterInfos.size() == 1)
        {
            return clusterInfos.get(0).getLowestCassandraVersion();
        }

        Map<String, String> aggregated = applyOnEach(ClusterInfo::getLowestCassandraVersion);
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
    public Map<RingInstance, WriteAvailability> clusterWriteAvailability()
    {
        if (clusterInfos.size() == 1)
        {
            return clusterInfos.get(0).clusterWriteAvailability();
        }

        Map<String, Map<RingInstance, WriteAvailability>> aggregated = applyOnEach(ClusterInfo::clusterWriteAvailability);
        Map<RingInstance, WriteAvailability> consolidated = new HashMap<>();
        aggregated.values().forEach(consolidated::putAll);
        return consolidated;
    }

    @Override
    public Partitioner getPartitioner()
    {
        Map<String, Partitioner> aggregated = applyOnEach(ClusterInfo::getPartitioner);
        Set<Partitioner> partitioners = EnumSet.copyOf(aggregated.values());
        if (partitioners.size() != 1)
        {
            throw new IllegalStateException("Clusters are not running with the same partitioner kind. Found partitioners: " + aggregated);
        }

        return partitioners.iterator().next();
    }

    @Override
    public void checkBulkWriterIsEnabledOrThrow()
    {
        // No-op - validation already done on driver
    }

    @Override
    public void validateTimeSkew(Range<BigInteger> range) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        // No-op - validation already done on driver
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        // All clusters should have the same keyspace schema
        return clusterInfos.get(0).getKeyspaceSchema(cached);
    }

    @Override
    public ReplicationFactor replicationFactor()
    {
        // Call the replicationFactor() method on the individual ClusterInfo
        throw new UnsupportedOperationException("Not implemented in SerializableClusterInfoGroup");
    }

    @Override
    public CassandraContext getCassandraContext()
    {
        // Call the getCassandraContext() method on the individual ClusterInfo
        throw new UnsupportedOperationException("Not implemented in SerializableClusterInfoGroup");
    }

    @Override
    public void startupValidate()
    {
        // No-op - validation already done on driver
    }

    @Override
    @Nullable
    public String clusterId()
    {
        return "SerializableClusterInfoGroup: [" + String.join(", ", applyOnEach(ClusterInfo::clusterId).values()) + ']';
    }

    @Override
    public int size()
    {
        return clusterInfos.size();
    }

    @Override
    public void forEach(BiConsumer<String, ClusterInfo> action)
    {
        clusterInfoById.forEach(action);
    }

    @Nullable
    @Override
    public ClusterInfo getValueOrNull(@NotNull String clusterId)
    {
        return clusterInfoById.get(clusterId);
    }

    private <T> Map<String, T> applyOnEach(Function<ClusterInfo, T> action)
    {
        Map<String, T> aggregated = new LinkedHashMap<>(clusterInfos.size());
        for (ClusterInfo clusterInfo : clusterInfos)
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

    private RuntimeException toRuntimeException(ClusterInfo clusterInfo, Throwable cause)
    {
        // No logger in broadcast variable - error will be propagated via exception
        return new RuntimeException("Failed to perform action on cluster: " + clusterInfo.clusterId(), cause);
    }

    @Override
    public void close()
    {
        // Delegate to all contained cluster infos
        for (ClusterInfo clusterInfo : clusterInfos)
        {
            clusterInfo.close();
        }
    }
}
