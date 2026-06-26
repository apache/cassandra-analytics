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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.CassandraClusterInfo;
import org.apache.cassandra.spark.bulkwriter.CassandraContext;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.BroadcastableClusterInfo;
import org.apache.cassandra.spark.bulkwriter.BroadcastableClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.IBroadcastableClusterInfo;
import org.apache.cassandra.spark.bulkwriter.WriteAvailability;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A group of ClusterInfo. One per cluster.
 * The class does the aggregation over all clusters for applicable operations.
 * <p>
 * This class is NOT serialized and does NOT have a serialVersionUID.
 * When broadcasting to executors, the driver extracts information from this class
 * and creates a {@link org.apache.cassandra.spark.bulkwriter.BroadcastableClusterInfoGroup} instance,
 * which is then included in the {@link org.apache.cassandra.spark.bulkwriter.BulkWriterConfig}
 * that gets broadcast.
 * <p>
 * This class implements Serializable only because the {@link org.apache.cassandra.spark.bulkwriter.ClusterInfo}
 * interface requires it (for use as a field type in broadcast classes), but instances of this
 * class are never directly serialized.
 */
public class CassandraClusterInfoGroup implements ClusterInfo, MultiClusterSupport<ClusterInfo>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfoGroup.class);

    // immutable
    private final List<ClusterInfo> clusterInfos;
    private final String clusterId;
    private volatile Map<String, ClusterInfo> clusterInfoById;
    private volatile TokenRangeMapping<RingInstance> consolidatedTokenRangeMapping;
    // Pre-computed values from BroadcastableClusterInfoGroup (only set when reconstructed on executors)
    private Partitioner cachedPartitioner;

    /**
     * Creates {@link CassandraClusterInfoGroup} with the list of {@link ClusterInfo} from {@link BulkSparkConf} and validation
     * The validation ensures non-empty list of {@link ClusterInfo}, where all objects have non-empty and unique clusterId
     * @param conf bulk write conf
     * @return new {@link CassandraClusterInfoGroup} instance
     */
    public static CassandraClusterInfoGroup fromBulkSparkConf(BulkSparkConf conf)
    {
        // bridgeVersion is null at construction time; the real version is applied later via setBridgeVersion(),
        // once the cluster's SSTable versions have been read from the preliminary group.
        return fromBulkSparkConf(conf, clusterId -> new CassandraClusterInfo(conf, clusterId, null));
    }


    /**
     * Reconstruct from BroadcastableClusterInfoGroup on executor.
     * Creates CassandraClusterInfo instances for each cluster that will fetch data from Sidecar.
     * Leverages pre-computed values (partitioner, bridgeVersion) from the broadcastable
     * to avoid re-validation and re-computation on executors.
     *
     * @param broadcastable the broadcastable cluster info group from broadcast
     * @return new {@link CassandraClusterInfoGroup} instance
     */
    public static CassandraClusterInfoGroup from(BroadcastableClusterInfoGroup broadcastable,
                                                 CassandraVersion bridgeVersion)
    {
        return new CassandraClusterInfoGroup(broadcastable, bridgeVersion);
    }

    /**
     * Similar to {@link #fromBulkSparkConf(BulkSparkConf)} but takes additional function to create {@link ClusterInfo}
     */
    public static CassandraClusterInfoGroup fromBulkSparkConf(BulkSparkConf conf, Function<String, ClusterInfo> clusterInfoFactory)
    {
        CoordinatedWriteConf coordinatedWriteConf = conf.coordinatedWriteConf();
        Preconditions.checkArgument(coordinatedWriteConf != null,
                                    "In order to create an instance of CassandraCoordinatedBulkWriterContext, " +
                                    "you must provide the appropriate coordinated write configuration by " +
                                    "setting the `" + WriterOptions.COORDINATED_WRITE_CONFIG + "` writer option.");
        for (String clusterId : coordinatedWriteConf.clusters().keySet())
        {
            Preconditions.checkState(!StringUtils.isEmpty(clusterId),
                                     "Found coordinatedWriteConf with empty or null clusterId. %s",
                                     coordinatedWriteConf);
        }
        List<ClusterInfo> clusterInfos = coordinatedWriteConf
                                         .clusters()
                                         .keySet()
                                         .stream()
                                         .map(clusterInfoFactory)
                                         .collect(Collectors.toList());
        Preconditions.checkState(!clusterInfos.isEmpty(), "No cluster info is built from %s", coordinatedWriteConf);
        return new CassandraClusterInfoGroup(clusterInfos);
    }

    /**
     * Creates a {@link CassandraClusterInfoGroup} from a pre-built list of {@link ClusterInfo} instances.
     * This factory is intended for custom {@link IBroadcastableClusterInfo} implementations that reconstruct
     * cluster infos individually and need to wrap them in a group.
     *
     * @param clusterInfos the list of already-reconstructed ClusterInfo instances
     * @return a new CassandraClusterInfoGroup
     */
    public static CassandraClusterInfoGroup createFrom(List<ClusterInfo> clusterInfos)
    {
        return new CassandraClusterInfoGroup(clusterInfos);
    }

    private CassandraClusterInfoGroup(List<ClusterInfo> clusterInfos)
    {
        this.clusterInfos = Collections.unmodifiableList(clusterInfos);
        clusterInfoById();
        this.clusterId = "ClusterInfoGroup: [" + String.join(", ", applyOnEach(ClusterInfo::clusterId).values()) + ']';
    }

    /**
     * Private constructor for executor-only reconstruction from broadcast data.
     * Accepts BroadcastableClusterInfoGroup and extracts pre-computed values to avoid
     * re-validation and re-computation on executors.
     *
     * @param broadcastable the broadcastable cluster info group from broadcast
     * @param bridgeVersion the bridge version from broadcast
     */
    private CassandraClusterInfoGroup(BroadcastableClusterInfoGroup broadcastable, CassandraVersion bridgeVersion)
    {
        // Build list of ClusterInfo from broadcastable data
        List<ClusterInfo> clusterInfosList = new ArrayList<>();
        broadcastable.forEach((clusterId, broadcastableInfo) -> clusterInfosList.add(
                new CassandraClusterInfo((BroadcastableClusterInfo) broadcastableInfo, bridgeVersion)));
        this.clusterInfos = Collections.unmodifiableList(clusterInfosList);

        // Extract pre-computed values from driver to avoid re-validation on executors
        this.cachedPartitioner = broadcastable.getPartitioner();
        this.clusterId = broadcastable.clusterId();
        clusterInfoById();
    }

    @Override
    public void refreshClusterInfo()
    {
        runOnEach(ClusterInfo::refreshClusterInfo);
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
                // return immediately if consolidatedTokenRangeMapping has been initialized and call-site asks for the cached value
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
        // Return cached value if available (executor-side reconstruction)
        if (cachedPartitioner != null)
        {
            return cachedPartitioner;
        }

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
        runOnEach(ClusterInfo::checkBulkWriterIsEnabledOrThrow);
    }

    @Override
    public void validateTimeSkew(Range<BigInteger> range) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        clusterInfos.forEach(ci -> ci.validateTimeSkew(range));
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        // All clusters that receive write should have the same keyspace schema. Therefore, return from the first cluster
        // Note that the keyspace replication options can vary among the clusters. It is/should not be used when the correct ReplicationFactor is wanted.
        // Instead, call the replicationFactor() method on the individual ClusterInfo
        return clusterInfos.get(0).getKeyspaceSchema(cached);
    }

    @Override
    public ReplicationFactor replicationFactor()
    {
        // Call the replicationFactor() method on the individual ClusterInfo
        throw new UnsupportedOperationException("Not implemented in CassandraClusterInfoGroup");
    }

    @Override
    public CassandraContext getCassandraContext()
    {
        // Call the getCassandraContext() method on the individual ClusterInfo
        throw new UnsupportedOperationException("Not implemented in CassandraClusterInfoGroup");
    }

    @Override
    public void startupValidate()
    {
        runOnEach(ClusterInfo::startupValidate);
    }

    @Override
    public String clusterId()
    {
        return clusterId;
    }

    @Override
    public int size()
    {
        return clusterInfos.size();
    }

    @Override
    public void forEach(BiConsumer<String, ClusterInfo> action)
    {
        clusterInfoById().forEach(action);
    }

    @Nullable
    @Override
    public ClusterInfo getValueOrNull(@NotNull String clusterId)
    {
        return clusterInfoById().get(clusterId);
    }

    private Map<String, ClusterInfo> clusterInfoById()
    {
        if (clusterInfoById == null)
        {
            synchronized (this)
            {
                if (clusterInfoById == null)
                {
                    clusterInfoById = clusterInfos.stream().collect(Collectors.toMap(ClusterInfo::clusterId, Function.identity()));
                }
            }
        }

        return clusterInfoById;
    }

    private void runOnEach(Consumer<ClusterInfo> action)
    {
        for (ClusterInfo clusterInfo : clusterInfos)
        {
            try
            {
                action.accept(clusterInfo);
            }
            catch (Throwable cause)
            {
                throw toRuntimeException(clusterInfo, cause);
            }
        }
    }

    private <T> Map<String, T> applyOnEach(Function<ClusterInfo, T> action)
    {
        // Preserve order with LinkedHashMap
        Map<String, T> aggregated = new LinkedHashMap<>(clusterInfos.size());
        for (ClusterInfo clusterInfo : clusterInfos)
        {
            try
            {
                // clusterId should not be null when there are multiple clusters
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
        LOGGER.error("Failed to perform action on cluster. cluster={}", clusterInfo.clusterId(), cause);
        return new RuntimeException("Failed to perform action on cluster: " + clusterInfo.clusterId(), cause);
    }

    /**
     * Sets the bridge version on all contained CassandraClusterInfo instances.
     *
     * @param bridgeVersion the determined Cassandra bridge version
     */
    public void setBridgeVersion(CassandraVersion bridgeVersion)
    {
        for (ClusterInfo ci : clusterInfos)
        {
            ((CassandraClusterInfo) ci).setBridgeVersion(bridgeVersion);
        }
    }

    /**
     * Determines the Cassandra bridge version for a coordinated write across all clusters.
     *
     * <p>Each cluster determines its own bridge version (see {@link CassandraClusterInfo#getBridgeVersion()}),
     * which is the lowest mutually-compatible SSTable version present on that cluster. Across clusters the
     * lowest of those is chosen so the produced SSTables remain importable by every cluster (a node can import
     * its own and older SSTable versions, but not newer ones).
     *
     * @return the determined Cassandra bridge version
     */
    public CassandraVersion getBridgeVersion()
    {
        // Single cluster: use its bridge version directly without aggregation
        if (clusterInfos.size() == 1)
        {
            return ((CassandraClusterInfo) clusterInfos.get(0)).getBridgeVersion();
        }

        // Write at the lowest so every cluster can import the produced SSTables
        return clusterInfos.stream()
                           .map(ci -> ((CassandraClusterInfo) ci).getBridgeVersion())
                           .min(Comparator.comparingInt(CassandraVersion::versionNumber))
                           .orElseThrow(() -> new IllegalStateException("No cluster bridge versions available"));
    }
}
