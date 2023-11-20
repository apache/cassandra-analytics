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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.common.client.InstanceStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;

public class CassandraClusterInfo implements ClusterInfo, Closeable
{
    private static final long serialVersionUID = -6944818863462956767L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfo.class);

    protected final BulkSparkConf conf;
    protected String cassandraVersion;
    protected Partitioner partitioner;
    protected transient CassandraRing<RingInstance> ring;
    protected transient String keyspaceSchema;
    protected transient volatile RingResponse ringResponse;
    protected transient GossipInfoResponse gossipInfo;
    protected transient CassandraContext cassandraContext;
    protected final transient AtomicReference<NodeSettings> nodeSettings;
    protected final transient List<CompletableFuture<NodeSettings>> allNodeSettingFutures;

    public CassandraClusterInfo(BulkSparkConf conf)
    {
        this.conf = conf;
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Getting Cassandra versions from all nodes");
        this.nodeSettings = new AtomicReference<>(null);
        this.allNodeSettingFutures = Sidecar.allNodeSettings(cassandraContext.getSidecarClient(),
                                                             cassandraContext.clusterConfig);
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
    public Map<RingInstance, InstanceAvailability> getInstanceAvailability()
    {
        RingResponse ringResponse = getRingResponse();
        Map<RingInstance, InstanceAvailability> result = ringResponse
                .stream()
                .collect(Collectors.toMap(CassandraClusterInfo::getCasInstanceMethodsImpl,
                                          this::determineInstanceAvailability));
        if (LOGGER.isDebugEnabled())
        {
            result.forEach((instance, availability) ->
                    LOGGER.debug("Instance {} has availability {}", instance, availability));
        }
        return result;
    }

    @Override
    public boolean instanceIsAvailable(RingInstance ringInstance)
    {
        return instanceIsUp(ringInstance.getRingInstance())
            && instanceIsNormal(ringInstance.getRingInstance())
            && !instanceIsBlocked(ringInstance);
    }

    @Override
    public InstanceState getInstanceState(RingInstance ringInstance)
    {
        return InstanceState.valueOf(ringInstance.getRingInstance().state().toUpperCase());
    }

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

    /**
     * Gets a Cassandra Context
     *
     * NOTE: The caller of this method is required to call `shutdown` on the returned CassandraContext instance
     *
     * @return an instance of CassandraContext based on the configuration settings
     */
    protected CassandraContext buildCassandraContext()
    {
        return buildCassandraContext(conf);
    }

    private static CassandraContext buildCassandraContext(BulkSparkConf conf)
    {
        return CassandraContext.create(conf);
    }

    @Override
    public void close()
    {
        synchronized (this)
        {
            LOGGER.info("Closing {}", this);
            getCassandraContext().close();
        }
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
    public TimeSkewResponse getTimeSkew(List<RingInstance> replicas)
    {
        try
        {
            List<SidecarInstance> instances = replicas
                    .stream()
                    .map(replica -> new SidecarInstanceImpl(replica.getNodeName(), getCassandraContext().sidecarPort()))
                    .collect(Collectors.toList());
            return getCassandraContext().getSidecarClient().timeSkew(instances).get();
        }
        catch (InterruptedException | ExecutionException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void refreshClusterInfo()
    {
        synchronized (this)
        {
            // Set backing stores to null and let them lazy-load on the next call
            ringResponse = null;
            gossipInfo = null;
            keyspaceSchema = null;
            getCassandraContext().refreshClusterConfig();
        }
    }

    protected String getCurrentKeyspaceSchema() throws Exception
    {
        SchemaResponse schemaResponse = getCassandraContext().getSidecarClient().schema(conf.keyspace).get();
        return schemaResponse.schema();
    }

    @NotNull
    protected CassandraRing<RingInstance> getCurrentRing() throws Exception
    {
        RingResponse ringResponse = getCurrentRingResponse();
        List<RingInstance> instances = getSerializableInstances(ringResponse);
        ReplicationFactor replicationFactor = getReplicationFactor();
        return new CassandraRing<>(getPartitioner(), conf.keyspace, replicationFactor, instances);
    }

    @NotNull
    protected ReplicationFactor getReplicationFactor()
    {
        String keyspaceSchema = getKeyspaceSchema(true);
        if (keyspaceSchema == null)
        {
            throw new RuntimeException(String.format("Could not retrieve keyspace schema information for keyspace %s",
                                                     conf.keyspace));
        }
        return CqlUtils.extractReplicationFactor(keyspaceSchema, conf.keyspace);
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
    public CassandraRing<RingInstance> getRing(boolean cached)
    {
        CassandraRing<RingInstance> currentRing = ring;
        if (cached && currentRing != null)
        {
            return currentRing;
        }

        synchronized (this)
        {
            if (!cached || ring == null)
            {
                try
                {
                    ring = getCurrentRing();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize ring information", exception);
                }
            }
            return ring;
        }
    }

    @Override
    public String getLowestCassandraVersion()
    {
        String currentCassandraVersion = cassandraVersion;
        if (currentCassandraVersion != null)
        {
            return currentCassandraVersion;
        }

        synchronized (this)
        {
            if (cassandraVersion == null)
            {
                String versionFromFeature = getVersionFromFeature();
                if (versionFromFeature != null)
                {
                    // Forcing writer to use a particular version
                    cassandraVersion = versionFromFeature;
                }
                else
                {
                    cassandraVersion = getVersionFromSidecar();
                }
            }
        }
        return cassandraVersion;
    }

    public String getVersionFromFeature()
    {
        return null;
    }

    protected List<NodeSettings> getAllNodeSettings()
    {
        List<NodeSettings> allNodeSettings = FutureUtils.bestEffortGet(allNodeSettingFutures,
                                                                       conf.getSidecarRequestMaxRetryDelayInSeconds(),
                                                                       TimeUnit.SECONDS);

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

    protected RingResponse getRingResponse()
    {
        RingResponse currentRingResponse = ringResponse;
        if (currentRingResponse != null)
        {
            return currentRingResponse;
        }

        synchronized (this)
        {
            if (ringResponse == null)
            {
                try
                {
                    ringResponse = getCurrentRingResponse();
                }
                catch (Exception exception)
                {
                    LOGGER.error("Failed to load Cassandra ring", exception);
                    throw new RuntimeException(exception);
                }
            }
            return ringResponse;
        }
    }

    private RingResponse getCurrentRingResponse() throws Exception
    {
        return getCassandraContext().getSidecarClient().ring(conf.keyspace).get();
    }

    private static List<RingInstance> getSerializableInstances(RingResponse ringResponse)
    {
        return ringResponse.stream()
                           .map(RingInstance::new)
                           .collect(Collectors.toList());
    }

    private static RingInstance getCasInstanceMethodsImpl(RingEntry ringEntry)
    {
        return new RingInstance(ringEntry);
    }

    protected GossipInfoResponse getGossipInfo(boolean forceRefresh)
    {
        GossipInfoResponse currentGossipInfo = gossipInfo;
        if (!forceRefresh && currentGossipInfo != null)
        {
            return currentGossipInfo;
        }

        synchronized (this)
        {
            if (forceRefresh || gossipInfo == null)
            {
                try
                {
                    gossipInfo = cassandraContext.getSidecarClient().gossipInfo().get(conf.getHttpResponseTimeoutMs(),
                                                                                      TimeUnit.MILLISECONDS);
                }
                catch (ExecutionException | InterruptedException exception)
                {
                    LOGGER.error("Failed to retrieve gossip information");
                    throw new RuntimeException("Failed to retrieve gossip information", exception);
                }
                catch (TimeoutException exception)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Failed to retrieve gossip information", exception);
                }
            }
            return gossipInfo;
        }
    }

    private InstanceAvailability determineInstanceAvailability(RingEntry ringEntry)
    {
        if (!instanceIsUp(ringEntry))
        {
            return InstanceAvailability.UNAVAILABLE_DOWN;
        }
        else if (instanceIsJoining(ringEntry) && isReplacement(ringEntry))
        {
            return InstanceAvailability.UNAVAILABLE_REPLACEMENT;
        }
        else if (instanceIsBlocked(getCasInstanceMethodsImpl(ringEntry)))
        {
            return InstanceAvailability.UNAVAILABLE_BLOCKED;
        }
        else if (instanceIsNormal(ringEntry))
        {
            return InstanceAvailability.AVAILABLE;
        }
        else
        {
            // If it's not one of the above, it's inherently INVALID
            return InstanceAvailability.INVALID_STATE;
        }
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

    protected boolean instanceIsBlocked(RingInstance ignored)
    {
        return false;
    }

    protected boolean instanceIsNormal(RingEntry ringEntry)
    {
        return InstanceState.NORMAL.name().equalsIgnoreCase(ringEntry.state());
    }

    protected boolean instanceIsUp(RingEntry ringEntry)
    {
        return InstanceStatus.UP.name().equalsIgnoreCase(ringEntry.status());
    }

    protected boolean instanceIsJoining(RingEntry ringEntry)
    {
        return InstanceState.JOINING.name().equalsIgnoreCase(ringEntry.state());
    }

    protected boolean isReplacement(RingEntry ringEntry)
    {
        GossipInfoResponse gossipInfo = getGossipInfo(true);
        LOGGER.debug("Gossip info={}", gossipInfo);

        GossipInfoResponse.GossipInfo hostInfo = gossipInfo.get(ringEntry.address());
        if (hostInfo != null)
        {
            LOGGER.info("Found hostinfo: {}", hostInfo);
            String hostStatus = hostInfo.status();
            if (hostStatus != null)
            {
                // If status has gone to NORMAL, we can't determine here if this was a host replacement or not.
                // CassandraRingManager will handle detecting the ring change if it's gone NORMAL after the job starts.
                return hostStatus.startsWith("BOOT_REPLACE,") || hostStatus.equals("NORMAL");
            }
        }
        return false;
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        getCassandraContext().startupValidate();
    }
}
