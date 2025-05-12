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
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.maybeQuotedIdentifier;

public class CassandraClusterInfo implements ClusterInfo, Closeable
{
    private static final long serialVersionUID = -6944818863462956767L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfo.class);

    protected final BulkSparkConf conf;
    protected final String clusterId;
    protected String cassandraVersion;
    protected Partitioner partitioner;

    protected transient volatile TokenRangeMapping<RingInstance> tokenRangeReplicas;
    protected transient volatile String keyspaceSchema;
    protected transient volatile ReplicationFactor replicationFactor;
    protected transient volatile CassandraContext cassandraContext;
    protected final transient AtomicReference<NodeSettings> nodeSettings;
    protected final transient List<CompletableFuture<NodeSettings>> allNodeSettingFutures;

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

    public String getVersionFromFeature()
    {
        return null;
    }

    protected List<NodeSettings> getAllNodeSettings()
    {
        // Worst-case, the http client is configured for 1 worker pool.
        // In that case, each future can take the full retry delay * number of retries,
        // and each instance will be processed serially.
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

    protected CassandraBridge bridge()
    {
        return CassandraBridgeFactory.get(getLowestCassandraVersion());
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        getCassandraContext().startupValidate();
    }
}
