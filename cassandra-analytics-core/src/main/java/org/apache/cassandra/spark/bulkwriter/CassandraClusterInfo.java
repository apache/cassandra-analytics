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

import java.io.Closeable;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
import o.a.c.sidecar.client.shaded.common.response.SchemaResponse;
import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.clients.Sidecar;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.maybeQuotedIdentifier;

/**
 * Driver-only implementation of {@link ClusterInfo} for single cluster operations.
 * <p>
 * This class is NOT serialized. When broadcasting to executors, the driver extracts
 * broadcast-safe fields via {@link BroadcastableClusterInfo#from(ClusterInfo, BulkSparkConf)}
 * and includes the result in the {@link BulkWriterConfig} that gets broadcast.
 * <p>
 * On executors, a new instance is reconstructed from {@link BroadcastableClusterInfo}
 * using {@link #CassandraClusterInfo(BroadcastableClusterInfo, CassandraVersion)}, reusing broadcast-safe
 * fields and fetching other data fresh from Sidecar.
 *
 * @see BroadcastableClusterInfo for the broadcast-safe subset of fields
 */
public class CassandraClusterInfo implements ClusterInfo, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfo.class);

    // -- Broadcast-safe fields --
    // Extracted by BroadcastableClusterInfo.from() and sent to executors.
    // Changes here must be reflected in BroadcastableClusterInfo.
    protected final BulkSparkConf conf;
    protected final String clusterId;
    protected Partitioner partitioner;

    // -- Driver-only fields (not broadcast) --
    // NOT included in BroadcastableClusterInfo. Either expensive to serialize
    // (token mappings, schema) or non-serializable (CassandraContext, Futures).
    // Executors reconstruct these fresh from Sidecar via CassandraClusterInfo(BroadcastableClusterInfo).
    protected volatile TokenRangeMapping<RingInstance> tokenRangeReplicas;
    protected volatile String keyspaceSchema;
    protected volatile ReplicationFactor replicationFactor;
    protected volatile CassandraContext cassandraContext;
    protected volatile CassandraVersion bridgeVersion;
    private final List<CompletableFuture<NodeSettings>> allNodeSettingFutures;
    private List<NodeSettings> resolvedNodeSettings;

    public CassandraClusterInfo(BulkSparkConf conf, CassandraVersion bridgeVersion)
    {
        this(conf, null, bridgeVersion);
    }

    /**
     * Constructor with bridge version for driver-side usage.
     *
     * @param conf           Bulk Spark configuration
     * @param clusterId      Optional cluster identifier
     * @param bridgeVersion  Determined bridge version (nullable for preliminary construction)
     */
    public CassandraClusterInfo(BulkSparkConf conf, String clusterId, CassandraVersion bridgeVersion)
    {
        this.conf = conf;
        this.clusterId = clusterId;
        this.bridgeVersion = bridgeVersion;
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Getting Cassandra versions from all nodes");
        this.allNodeSettingFutures = Sidecar.allNodeSettings(cassandraContext.getSidecarClient(),
                                                             cassandraContext.getCluster());
    }

    /**
     * Reconstruct from BroadcastableCluster on executor.
     * Reuses partitioner and bridge version from broadcast,
     * fetches other data (tokenRangeMapping, replicationFactor, keyspaceSchema, writeAvailability) fresh from Sidecar.
     *
     * @param broadcastable the broadcastable cluster info from broadcast
     * @param bridgeVersion the bridge version from broadcast
     */
    public CassandraClusterInfo(BroadcastableClusterInfo broadcastable, CassandraVersion bridgeVersion)
    {
        this.conf = broadcastable.getConf();
        this.clusterId = broadcastable.clusterId();
        this.partitioner = broadcastable.getPartitioner();
        this.bridgeVersion = bridgeVersion;
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Reconstructing CassandraClusterInfo on executor from BroadcastableCluster. clusterId={}, bridgeVersion={}",
                    clusterId, bridgeVersion != null ? bridgeVersion.versionName() : "null");
        // Executors do not need to query all node settings since cassandraVersion is already set from broadcast
        this.allNodeSettingFutures = null;
    }

    @Override
    public void checkBulkWriterIsEnabledOrThrow()
    {
        // DO NOTHING
    }

    public String getVersion()
    {
        return CassandraClusterInfo.class.getPackage().getImplementationVersion();
    }

    @Override
    public CassandraContext getCassandraContext()
    {
        CassandraContext currentCassandraContext = cassandraContext;
        if (currentCassandraContext != null)
        {
            return currentCassandraContext;
        }

        synchronized (this)
        {
            if (cassandraContext == null)
            {
                cassandraContext = buildCassandraContext();
            }
            return cassandraContext;
        }
    }

    @Override
    public String clusterId()
    {
        return clusterId;
    }

    /**
     * Gets a Cassandra Context
     * <p>
     * NOTE: The caller of this method is required to call `shutdown` on the returned CassandraContext instance
     *
     * @return an instance of CassandraContext based on the configuration settings
     */
    protected CassandraContext buildCassandraContext()
    {
        return buildCassandraContext(conf, clusterId);
    }

    private static CassandraContext buildCassandraContext(BulkSparkConf conf, @Nullable String clusterId)
    {
        return CassandraContext.create(conf, clusterId);
    }

    @Override
    public void close()
    {
        LOGGER.info("Closing {}", this);
        getCassandraContext().close();
    }

    @Override
    public Partitioner getPartitioner()
    {
        Partitioner currentPartitioner = partitioner;
        if (currentPartitioner != null)
        {
            return currentPartitioner;
        }

        synchronized (this)
        {
            if (partitioner == null)
            {
                List<NodeSettings> settings = resolveAllNodeSettings();
                String partitionerString = settings.get(0).partitioner();
                partitioner = Partitioner.from(partitionerString);
            }
            return partitioner;
        }
    }

    @Override
    public void validateTimeSkew(Range<BigInteger> range) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        validateTimeSkewWithLocalNow(range, Instant.now());
    }

    @VisibleForTesting
    void validateTimeSkewWithLocalNow(Range<BigInteger> range, Instant localNow) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        TimeSkewResponse timeSkew;
        try
        {
            TokenRangeMapping<RingInstance> topology = getTokenRangeMapping(true);
            List<SidecarInstance> instances = topology.getSubRanges(range)
                                                      .asMapOfRanges()
                                                      .values()
                                                      .stream()
                                                      .flatMap(Collection::stream)
                                                      .distinct() // remove duplications
                                                      .map(replica -> new SidecarInstanceImpl(replica.nodeName(), getCassandraContext().sidecarPort()))
                                                      .collect(Collectors.toList());
            timeSkew = getCassandraContext().getSidecarClient().timeSkew(instances).get();
        }
        catch (InterruptedException | ExecutionException exception)
        {
            throw new SidecarApiCallException("Unable to retrieve time skew information. clusterId=" + clusterId(), exception);
        }

        Instant remoteNow = Instant.ofEpochMilli(timeSkew.currentTime);
        Duration allowedDuration = Duration.ofMinutes(timeSkew.allowableSkewInMinutes);
        if (localNow.isBefore(remoteNow.minus(allowedDuration)) || localNow.isAfter(remoteNow.plus(allowedDuration)))
        {
            throw new TimeSkewTooLargeException(timeSkew.allowableSkewInMinutes, localNow, remoteNow, clusterId());
        }
    }

    @Override
    public synchronized void refreshClusterInfo()
    {
        // Set backing stores to null and let them lazy-load on the next call
        keyspaceSchema = null;
        getCassandraContext().refreshClusterConfig();
    }

    protected String getCurrentKeyspaceSchema() throws Exception
    {
        SchemaResponse schemaResponse = getCassandraContext().getSidecarClient()
                                                             .schema(maybeQuotedIdentifier(bridge(), conf.quoteIdentifiers, conf.keyspace))
                                                             .get();
        return schemaResponse.schema();
    }

    private TokenRangeReplicasResponse getTokenRangesAndReplicaSets()
    {
        CassandraContext context = getCassandraContext();
        try
        {
            long start = System.nanoTime();
            TokenRangeReplicasResponse response = context.getSidecarClient()
                                                         .tokenRangeReplicas(new ArrayList<>(context.getCluster()), conf.keyspace)
                                                         .get();
            long elapsedTimeNanos = System.nanoTime() - start;
            LOGGER.info("Retrieved token ranges for {} instances in {} milliseconds",
                        response.writeReplicas().size(),
                        TimeUnit.NANOSECONDS.toMillis(elapsedTimeNanos));
            return response;
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.error("Failed to get token ranges for keyspace {}", conf.keyspace, exception);
            throw new SidecarApiCallException("Failed to get token ranges for keyspace" + conf.keyspace, exception);
        }
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        String currentKeyspaceSchema = keyspaceSchema;
        if (cached && currentKeyspaceSchema != null)
        {
            return currentKeyspaceSchema;
        }

        synchronized (this)
        {
            if (!cached || keyspaceSchema == null)
            {
                try
                {
                    keyspaceSchema = getCurrentKeyspaceSchema();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize schema information for keyspace " + conf.keyspace,
                                               exception);
                }
            }
            return keyspaceSchema;
        }
    }

    @Override
    public ReplicationFactor replicationFactor()
    {
        ReplicationFactor rf = replicationFactor;
        if (rf != null)
        {
            return rf;
        }

        String keyspaceSchema = getKeyspaceSchema(true);
        if (keyspaceSchema == null)
        {
            throw new RuntimeException("Could not retrieve keyspace schema information for keyspace " + conf.keyspace);
        }
        synchronized (this)
        {
            if (replicationFactor == null)
            {
                replicationFactor = CqlUtils.extractReplicationFactor(keyspaceSchema, conf.keyspace);
            }
            return replicationFactor;
        }
    }

    @Override
    public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
    {
        TokenRangeMapping<RingInstance> topology = this.tokenRangeReplicas;
        if (cached && topology != null)
        {
            return topology;
        }

        // Block only for the call-sites requesting the latest view of the ring
        // The other call-sites get the cached/stale view
        // We can avoid synchronization here
        if (topology != null)
        {
            topology = getTokenRangeReplicasFromSidecar();
            this.tokenRangeReplicas = topology;
            return topology;
        }

        // Only synchronize when it is the first time fetching the ring information
        synchronized (this)
        {
            try
            {
                this.tokenRangeReplicas = getTokenRangeReplicasFromSidecar();
            }
            catch (Exception exception)
            {
                throw new RuntimeException("Unable to initialize ring information", exception);
            }
            return this.tokenRangeReplicas;
        }
    }


    @Override
    public Map<RingInstance, WriteAvailability> clusterWriteAvailability()
    {
        Set<RingInstance> allInstances = getTokenRangeMapping(true).allInstances();
        Map<RingInstance, WriteAvailability> result = new HashMap<>(allInstances.size());
        for (RingInstance instance : allInstances)
        {
            result.put(instance, determineWriteAvailability(instance));
        }

        if (LOGGER.isDebugEnabled())
        {
            result.forEach((inst, avail) -> LOGGER.debug("Instance {} has availability {}", inst, avail));
        }
        return result;
    }

    protected WriteAvailability determineWriteAvailability(RingInstance instance)
    {
        return WriteAvailability.determineFromNodeState(instance.nodeState(), instance.nodeStatus());
    }

    private TokenRangeMapping<RingInstance> getTokenRangeReplicasFromSidecar()
    {
        return TokenRangeMapping.create(this::getTokenRangesAndReplicaSets,
                                        this::getPartitioner,
                                        metadata -> new RingInstance(metadata, clusterId()));
    }

    /**
     * Sets the bridge version after preliminary construction.
     * This allows constructing CassandraClusterInfo with a null bridgeVersion for early
     * context reuse, then setting the version once it has been determined.
     *
     * @param bridgeVersion the determined Cassandra bridge version
     */
    public void setBridgeVersion(CassandraVersion bridgeVersion)
    {
        this.bridgeVersion = bridgeVersion;
    }

    /**
     * Resolves the node settings futures on first call and caches the result.
     *
     * @return list of resolved NodeSettings from all nodes
     */
    private synchronized List<NodeSettings> resolveAllNodeSettings()
    {
        if (resolvedNodeSettings != null)
        {
            return resolvedNodeSettings;
        }

        if (allNodeSettingFutures == null)
        {
            throw new IllegalStateException("allNodeSettingFutures is null");
        }

        final long totalTimeout = conf.getSidecarRequestMaxRetryDelayMillis() *
                                  conf.getSidecarRequestRetries() *
                                  allNodeSettingFutures.size();
        List<NodeSettings> allNodeSettings = FutureUtils.bestEffortGet(allNodeSettingFutures,
                                                                       totalTimeout,
                                                                       TimeUnit.MILLISECONDS);

        if (allNodeSettings.isEmpty())
        {
            throw new RuntimeException(String.format("Unable to determine the node settings. 0/%d instances available.",
                                                     allNodeSettingFutures.size()));
        }
        else if (allNodeSettings.size() < allNodeSettingFutures.size())
        {
            LOGGER.warn("{}/{} instances were used to determine the node settings",
                        allNodeSettings.size(), allNodeSettingFutures.size());
        }

        resolvedNodeSettings = allNodeSettings;
        return resolvedNodeSettings;
    }

    /**
     * Retrieves the lowest Cassandra version using the already-fired allNodeSettingFutures.
     * Reuses the existing CassandraContext instead of creating a separate one.
     *
     * @return lowest Cassandra version string
     */
    public String getLowestCassandraVersion()
    {
        List<NodeSettings> allNodeSettings = resolveAllNodeSettings();

        NodeSettings ns = allNodeSettings
                          .stream()
                          .filter(settings -> !settings.releaseVersion().equalsIgnoreCase("unknown"))
                          .min(Comparator.comparing(settings ->
                                                    CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(settings.releaseVersion())))
                          .orElseThrow(() -> new RuntimeException("No valid Cassandra Versions were returned from Cassandra Sidecar"));

        return ns.releaseVersion();
    }

    /**
     * Retrieves SSTable versions using the existing cassandraContext.
     * Reuses the existing CassandraContext instead of creating a separate one.
     *
     * @return set of SSTable version strings present on the cluster
     */
    public Set<String> getSSTableVersionsOnCluster()
    {
        CassandraContext context = getCassandraContext();

        return Sidecar.getSSTableVersionsFromCluster(
            context.getSidecarClient(),
            context.getCluster(),
            conf.getSidecarRequestMaxRetryDelayMillis(),
            conf.getSidecarRequestRetries()
        );
    }

    protected CassandraBridge bridge()
    {
        // Use the pre-determined bridgeVersion if available
        if (bridgeVersion != null)
        {
            return CassandraBridgeFactory.get(bridgeVersion);
        }

        // Bridge version must be set before accessing bridge
        throw new IllegalStateException(
            "Bridge version must be set during construction before using bridge().");
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        getCassandraContext().startupValidate();
    }
}
