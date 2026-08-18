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
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.cassandra.bridge.SSTableVersionAnalyzer;
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
 * using {@link #CassandraClusterInfo(BroadcastableClusterInfo)}, reusing broadcast-safe
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
    // volatile for correct double-checked locking in getBridgeVersion() (lazy init on the driver)
    protected volatile CassandraVersion bridgeVersion;
    protected Partitioner partitioner;

    // -- Driver-only fields (not broadcast) --
    // NOT included in BroadcastableClusterInfo. Either expensive to serialize
    // (token mappings, schema) or non-serializable (CassandraContext, Futures).
    // Executors reconstruct these fresh from Sidecar via CassandraClusterInfo(BroadcastableClusterInfo).
    protected volatile TokenRangeMapping<RingInstance> tokenRangeReplicas;
    protected volatile String keyspaceSchema;
    protected volatile ReplicationFactor replicationFactor;
    protected volatile CassandraContext cassandraContext;
    protected final AtomicReference<NodeSettings> nodeSettings;
    protected final List<CompletableFuture<NodeSettings>> allNodeSettingFutures;

    public CassandraClusterInfo(BulkSparkConf conf)
    {
        this(conf, null);
    }

    // Used by CassandraClusterInfoGroup
    public CassandraClusterInfo(BulkSparkConf conf, String clusterId)
    {
        this.conf = conf;
        this.clusterId = clusterId;
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Getting Cassandra versions from all nodes");
        this.nodeSettings = new AtomicReference<>(null);
        this.allNodeSettingFutures = Sidecar.allNodeSettings(cassandraContext.getSidecarClient(),
                                                             cassandraContext.getCluster());
    }

    /**
     * Reconstruct from BroadcastableCluster on executor.
     * Reuses bridgeVersion and partitioner from broadcast,
     * fetches other data (tokenRangeMapping, replicationFactor, keyspaceSchema, writeAvailability) fresh from Sidecar.
     *
     * @param broadcastable the broadcastable cluster info from broadcast
     */
    public CassandraClusterInfo(BroadcastableClusterInfo broadcastable)
    {
        this.conf = broadcastable.getConf();
        this.clusterId = broadcastable.clusterId();
        this.partitioner = broadcastable.getPartitioner();
        this.bridgeVersion = broadcastable.getBridgeVersion();
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Reconstructing CassandraClusterInfo on executor from BroadcastableCluster. clusterId={}", clusterId);
        this.nodeSettings = new AtomicReference<>(null);
        // Executors do not need to query all node settings since the bridge version is already set from broadcast
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
                try
                {
                    String partitionerString;
                    NodeSettings currentNodeSettings = nodeSettings.get();
                    if (currentNodeSettings != null)
                    {
                        partitionerString = currentNodeSettings.partitioner();
                    }
                    else
                    {
                        partitionerString = getCassandraContext().getSidecarClient().nodeSettings().get().partitioner();
                    }
                    partitioner = Partitioner.from(partitionerString);
                }
                catch (ExecutionException | InterruptedException exception)
                {
                    throw new RuntimeException("Unable to retrieve partitioner information", exception);
                }
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
                                                      .map(replica -> new SidecarInstanceImpl(replica.nodeName(), getCassandraContext().sidecarPort(),
                                                                                              replica.sidecarInstanceId()))
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

    /**
     * Returns the Cassandra bridge version for this cluster, computed lazily on first use and cached.
     * Mirrors {@link #getVersionFromSidecar()}'s lazy pattern: it works on the driver (where the value is
     * computed from the cluster) and on executors (where the cached value is seeded from the broadcast
     * {@link BroadcastableClusterInfo}, so no re-query is needed). The determination itself is in
     * {@link #determineBridgeVersion()}.
     *
     * @return the determined Cassandra bridge version (e.g. {@link CassandraVersion#FIVEZERO})
     */
    @Override
    public CassandraVersion getBridgeVersion()
    {
        CassandraVersion currentBridgeVersion = bridgeVersion;
        if (currentBridgeVersion != null)
        {
            return currentBridgeVersion;
        }

        synchronized (this)
        {
            if (bridgeVersion == null)
            {
                bridgeVersion = determineBridgeVersion();
            }
            return bridgeVersion;
        }
    }

    /**
     * Determines the Cassandra bridge version for this cluster, in priority order:
     * <ol>
     *   <li>an explicit version override from {@link #getVersionFromFeature()} (an operator escape hatch) —
     *       takes precedence even when SSTable-version-based selection is enabled;</li>
     *   <li>otherwise, when SSTable version-based selection is enabled, the lowest version derived from the
     *       SSTable versions present, so the produced SSTables remain importable by every node;</li>
     *   <li>otherwise (feature disabled), the lowest Cassandra release version reported by the cluster.</li>
     * </ol>
     *
     * @return the determined bridge version
     */
    private CassandraVersion determineBridgeVersion()
    {
        String versionOverride = getVersionFromFeature();
        if (versionOverride != null)
        {
            // Forcing writer to use a particular version; validate it is a supported version up front
            return CassandraVersion.fromVersion(versionOverride)
                                   .orElseThrow(() -> new UnsupportedOperationException(
                                   "Unsupported Cassandra version override: " + versionOverride));
        }

        if (!conf.isSSTableVersionBasedBridgeDisabled())
        {
            return SSTableVersionAnalyzer.determineBridgeVersionForWrite(getSSTableVersionsOnCluster(),
                                                                         CassandraVersion.configuredSSTableFormat());
        }

        String releaseVersion = getVersionFromSidecar();
        CassandraVersion bridgeVersion = CassandraVersion.fromVersion(releaseVersion)
                                                         .orElseThrow(() -> new UnsupportedOperationException(
                                                         "Unsupported Cassandra version: " + releaseVersion));
        LOGGER.info("SSTable version-based bridge selection is disabled; determined bridge version {} for write "
                    + "from the cluster's Cassandra release version {} (legacy mode)",
                    bridgeVersion.versionName(), releaseVersion);
        return bridgeVersion;
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
        // Resolve per-instance ids from the contact points actually in effect for this cluster (getCluster()
        // already picks the right source: conf.sidecarContactPoints() for a plain job, or
        // conf.coordinatedWriteConf().cluster(clusterId).sidecarContactPoints() for a coordinated-write job).
        // Deriving this from conf.sidecarContactPoints() directly would silently miss (or NPE on) coordinated
        // writes, since their contact points don't live there.
        Map<String, Integer> instanceIdsByHostname = sidecarInstanceIdsByHostname(getCassandraContext().getCluster());
        TokenRangeMapping<RingInstance> topology =
        TokenRangeMapping.create(this::getTokenRangesAndReplicaSets,
                                 this::getPartitioner,
                                 metadata -> new RingInstance(metadata, clusterId(),
                                                              instanceIdsByHostname.get(metadata.fqdn())));
        validateSidecarInstanceIdCoverage(topology.allInstances());
        return topology;
    }

    /**
     * Builds a hostname (nodeName/fqdn) to per-instance Sidecar routing id lookup from the given contact points,
     * e.g. ones declared as {@code "host:port=<id>"} (see {@link SidecarInstanceFactory#createFromString}).
     *
     * <p><b>Limitation:</b> this lookup is keyed on the literal contact-point address string, so it can only
     * represent a 1:1 Sidecar-to-instance topology, where every instance has its own distinct address. If two
     * or more instances share the same address with different ids configured, building this map throws
     * {@code IllegalStateException: Duplicate key} — it cannot express that topology at all. The address must
     * also match exactly (IP vs hostname, case) what the ring reports for that instance ({@code
     * ReplicaMetadata#fqdn()}); a mismatch silently drops the configured id rather than resolving it.
     *
     * @param contactPoints the Sidecar contact points in effect for this cluster
     * @return a map of hostname to configured Sidecar instance id; entries with no configured id are omitted
     */
    @VisibleForTesting
    static Map<String, Integer> sidecarInstanceIdsByHostname(Set<SidecarInstance> contactPoints)
    {
        return contactPoints.stream()
                            .filter(instance -> instance.instanceId() != null)
                            .collect(Collectors.toMap(SidecarInstance::hostname, SidecarInstance::instanceId));
    }

    /**
     * Guards against the data-correctness risk of a single job-level {@code instanceId} (see
     * {@link BulkSparkConf#SIDECAR_INSTANCE_ID}) being stamped uniformly onto requests fanned out across more than
     * one real Cassandra instance. That is only correct when every instance in the ring resolves its own id (see
     * {@link #sidecarInstanceIdsByHostname}); otherwise requests to the unresolved instances would be silently
     * misrouted to whichever instance the global id happens to identify.
     *
     * <p><b>Scope:</b> this check — and the whole per-instance-id resolution mechanism above it — assumes a
     * <b>1:1 Sidecar-to-Cassandra-instance deployment where every instance is reachable at its own distinct
     * address</b> (directly, or via {@code host[:port]=<id>} contact points). In that shape a single job-level
     * id is always correct, since every Sidecar answering has exactly one local instance to route to.
     *
     * <p>It does <b>not</b> cover multiple distinct Cassandra instances reachable only through a
     * <b>shared</b> address (several instances behind the same load-balancer endpoint, or one Sidecar managing
     * more than one local instance on the same host:port). That topology cannot be expressed by the current
     * hostname-keyed lookup — configuring it crashes {@link #sidecarInstanceIdsByHostname} with a
     * "Duplicate key" error instead of resolving. Supporting it safely requires the Sidecar server to report
     * each replica's own instance id in the ring/token-range-replicas response, so the client resolves coverage
     * from the ring itself rather than from static, address-keyed config. Tracked as a follow-up in
     * apache/cassandra-sidecar.
     *
     * @param instances the distinct instances discovered from the live ring
     */
    @VisibleForTesting
    void validateSidecarInstanceIdCoverage(Set<RingInstance> instances)
    {
        Integer globalInstanceId = conf.getSidecarInstanceId();
        if (globalInstanceId == null || instances.size() <= 1)
        {
            return;
        }

        List<String> unresolvedInstances = instances.stream()
                                                     .filter(instance -> instance.sidecarInstanceId() == null)
                                                     .map(RingInstance::nodeName)
                                                     .sorted()
                                                     .collect(Collectors.toList());
        if (!unresolvedInstances.isEmpty())
        {
            throw new IllegalStateException(
            String.format("Ambiguous Sidecar instanceId configuration: Spark conf %s=%d would be applied uniformly "
                          + "to %d/%d ring instances that have no per-instance id configured (%s). This misroutes "
                          + "requests whenever a single Sidecar endpoint fronts more than one of these instances "
                          + "(for example, behind a load balancer). If every instance is reachable at its own "
                          + "distinct address, configure a per-instance id for each one using the host[:port]=<id> "
                          + "syntax in %s. If instead multiple instances share the same address (e.g. behind one "
                          + "load-balancer endpoint), per-instance ids cannot currently be expressed this way — "
                          + "that requires a Sidecar-side fix to report each instance's id in the ring response.",
                          BulkSparkConf.SIDECAR_INSTANCE_ID, globalInstanceId, unresolvedInstances.size(), instances.size(),
                          unresolvedInstances, WriterOptions.SIDECAR_CONTACT_POINTS.name()));
        }
    }

    public String getVersionFromFeature()
    {
        return null;
    }

    protected List<NodeSettings> getAllNodeSettings()
    {
        if (allNodeSettingFutures == null)
        {
            throw new IllegalStateException("getAllNodeSettings should not be called on executor. "
                                            + "Cassandra version is pre-computed on driver and broadcast to executors.");
        }

        // Each of the retry attempts can take up to the request timeout plus the max delay
        // before the next retry. Requests to all instances run in parallel, so the cluster-wide
        // wait is bounded by a single node's worst case.
        final long totalTimeout = (TimeUnit.SECONDS.toMillis(conf.getSidecarRequestTimeoutSeconds()) + conf.getSidecarRequestMaxRetryDelayMillis())
                                  * conf.getSidecarRequestRetries();
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

        return allNodeSettings;
    }

    public String getVersionFromSidecar()
    {
        NodeSettings nodeSettings = this.nodeSettings.get();
        if (nodeSettings != null)
        {
            return nodeSettings.releaseVersion();
        }

        return getLowestVersion(getAllNodeSettings());
    }

    @VisibleForTesting
    public String getLowestVersion(List<NodeSettings> allNodeSettings)
    {
        NodeSettings ns = this.nodeSettings.get();
        if (ns != null)
        {
            return ns.releaseVersion();
        }

        // It is possible to run the below computation multiple times. Since the computation is local-only, it is OK.
        ns = allNodeSettings
             .stream()
             .filter(settings -> !settings.releaseVersion().equalsIgnoreCase("unknown"))
             .min(Comparator.comparing(settings ->
                                       CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(settings.releaseVersion())))
             .orElseThrow(() -> new RuntimeException("No valid Cassandra Versions were returned from Cassandra Sidecar"));
        nodeSettings.compareAndSet(null, ns);
        return ns.releaseVersion();
    }

    /**
     * Retrieves SSTable versions using the existing CassandraContext.
     * Reuses the existing CassandraContext instead of creating a separate one.
     *
     * @return set of SSTable version strings present on the cluster
     */
    @VisibleForTesting
    public Set<String> getSSTableVersionsOnCluster()
    {
        CassandraContext context = getCassandraContext();

        return Sidecar.getSSTableVersionsFromCluster(context.getSidecarClient(),
                                                     context.getCluster(),
                                                     conf.getSidecarRequestMaxRetryDelayMillis(),
                                                     conf.getSidecarRequestRetries(),
                                                     conf.getSidecarRequestTimeoutSeconds()
        );
    }

    protected CassandraBridge bridge()
    {
        return CassandraBridgeFactory.get(getBridgeVersion());
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        getCassandraContext().startupValidate();
    }
}
