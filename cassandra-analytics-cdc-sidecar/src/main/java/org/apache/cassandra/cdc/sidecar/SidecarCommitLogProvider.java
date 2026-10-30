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

package org.apache.cassandra.cdc.sidecar;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.Nullable;

/**
 * The SidecarCommitLogProvider implements a CommitLogProvider for listing and reading
 * CommitLog segments over the Sidecar HTTP APIs.
 * It only selects the CassandraInstance objects that replicate the TokenRange supplied.
 * <p>
 * The replicas of each token range are taken from Cassandra, through Sidecar's token range replicas endpoint, for
 * every CDC-enabled keyspace, so they follow Cassandra's rack-aware replica placement. Write replicas are used, as
 * pending replicas also receive the mutations. The replicas are derived locally from node tokens and the maximal
 * replication factor instead, which assumes racks are not in use, when no {@link SchemaSupplier} is supplied, when
 * {@link SidecarCdcOptions#deriveTokenRanges()} is set, or when they cannot be taken from Cassandra.
 */
public class SidecarCommitLogProvider implements CommitLogProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarCommitLogProvider.class);
    // How soon to try again after the replicas could not be taken from Cassandra
    @VisibleForTesting
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(30);
    // Bounds each call made to take the replicas from Cassandra, as they are made holding the lock
    private static final Duration FETCH_TIMEOUT = Duration.ofMinutes(1);

    private final ClusterConfigProvider clusterConfigProvider;
    private final SidecarCdcClient sidecarCdcClient;
    private final SidecarDownMonitor downMonitor;
    private final Predicate<CassandraInstance> dcFilter;
    private final ReplicationFactorSupplier replicationFactorSupplier;
    @Nullable
    private final SchemaSupplier schemaSupplier;
    private final SidecarCdcOptions sidecarCdcOptions;
    private final LongSupplier nanoTime;
    // guarded by this
    @Nullable
    private ReplicaPlacement placement;

    public SidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider,
                                    SidecarCdcClient sidecarCdcClient,
                                    SidecarDownMonitor downMonitor,
                                    ReplicationFactorSupplier replicationFactorSupplier)
    {
        this(clusterConfigProvider, sidecarCdcClient, downMonitor, replicationFactorSupplier, null, SidecarCdcOptions.DEFAULT);
    }

    public SidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider,
                                    SidecarCdcClient sidecarCdcClient,
                                    SidecarDownMonitor downMonitor,
                                    ReplicationFactorSupplier replicationFactorSupplier,
                                    @Nullable SchemaSupplier schemaSupplier,
                                    SidecarCdcOptions sidecarCdcOptions)
    {
        this(clusterConfigProvider, sidecarCdcClient, downMonitor, replicationFactorSupplier, schemaSupplier,
             sidecarCdcOptions, System::nanoTime);
    }

    @VisibleForTesting
    SidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider,
                             SidecarCdcClient sidecarCdcClient,
                             SidecarDownMonitor downMonitor,
                             ReplicationFactorSupplier replicationFactorSupplier,
                             @Nullable SchemaSupplier schemaSupplier,
                             SidecarCdcOptions sidecarCdcOptions,
                             LongSupplier nanoTime)
    {
        this.clusterConfigProvider = clusterConfigProvider;
        this.sidecarCdcClient = sidecarCdcClient;
        this.downMonitor = downMonitor;
        this.replicationFactorSupplier = replicationFactorSupplier;
        this.schemaSupplier = schemaSupplier;
        this.sidecarCdcOptions = sidecarCdcOptions;
        this.nanoTime = nanoTime;
        this.dcFilter = inst -> {
            String dc = clusterConfigProvider.dc();
            return dc == null || inst.dataCenter().equalsIgnoreCase(dc);
        };
    }

    /**
     * @return the ring derived locally from node tokens and the maximal replication factor
     */
    protected CassandraRing ring()
    {
        Set<CassandraInstance> cluster = clusterConfigProvider.getCluster()
                                         .stream()
                                         .filter(dcFilter)
                                         .collect(Collectors.toSet());
        Partitioner partitioner = clusterConfigProvider.partitioner();
        ReplicationFactor rf = replicationFactorSupplier.getMaximalReplicationFactor();
        return new CassandraRing(partitioner, "keyspace", rf, cluster);
    }

    protected static Range<BigInteger> toGuavaRange(TokenRange range)
    {
        return Range.openClosed(range.lowerEndpoint(), range.upperEndpoint());
    }

    /**
     * @param tokenRange the token range to read
     * @return the instances that replicate any part of the token range
     */
    protected List<CassandraInstance> replicas(TokenRange tokenRange)
    {
        Range<BigInteger> range = toGuavaRange(tokenRange);
        Map<Range<BigInteger>, Set<CassandraInstance>> cassandraReplicas = cassandraReplicas();
        if (cassandraReplicas != null)
        {
            List<CassandraInstance> instances = cassandraReplicas.entrySet()
                                                                 .stream()
                                                                 .filter(entry -> overlaps(entry.getKey(), range))
                                                                 .flatMap(entry -> entry.getValue().stream())
                                                                 .distinct()
                                                                 .collect(Collectors.toList());
            if (!instances.isEmpty())
            {
                return instances;
            }
            LOGGER.warn("Cassandra reported no replicas for token range {}, deriving them locally", range);
        }
        return ring().getSubRanges(range).asMapOfRanges().values().stream()
                     .flatMap(Collection::stream)
                     .collect(Collectors.toList());
    }

    private static boolean overlaps(Range<BigInteger> left, Range<BigInteger> right)
    {
        return left.isConnected(right) && !left.intersection(right).isEmpty();
    }

    /**
     * @return the replicas of each token range taken from Cassandra, or {@code null} to derive them locally
     */
    @Nullable
    private synchronized Map<Range<BigInteger>, Set<CassandraInstance>> cassandraReplicas()
    {
        if (schemaSupplier == null || sidecarCdcOptions.deriveTokenRanges())
        {
            return null;
        }

        Set<CassandraInstance> cluster = clusterConfigProvider.getCluster();
        long now = nanoTime.getAsLong();
        if (placement != null
            && placement.cluster.equals(cluster)
            && now - placement.fetchedAtNanos < placement.validForNanos)
        {
            return placement.replicas;
        }

        Duration refreshInterval = sidecarCdcOptions.tokenRangeReplicasRefreshInterval();
        Map<Range<BigInteger>, Set<CassandraInstance>> replicas;
        boolean fetched = false;
        try
        {
            replicas = fetchCassandraReplicas(cluster);
            fetched = replicas != null;
        }
        catch (Exception exception)
        {
            if (exception instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            // Reuse the previous replicas while the cluster is unchanged, otherwise derive them, which can miss
            // replicas on a cluster with uneven racks
            replicas = placement != null && placement.cluster.equals(cluster) ? placement.replicas : null;
            if (replicas == null)
            {
                LOGGER.error("Failed to take token range replicas from Cassandra, deriving them locally until the "
                             + "next attempt in {}", RETRY_INTERVAL, ThrowableUtils.rootCause(exception));
            }
            else
            {
                LOGGER.warn("Failed to take token range replicas from Cassandra, reusing the previous ones until the "
                            + "next attempt in {}", RETRY_INTERVAL, ThrowableUtils.rootCause(exception));
            }
        }
        // Retry sooner when nothing was fetched, rather than calling Sidecar for every micro-batch
        Duration validFor = fetched || RETRY_INTERVAL.compareTo(refreshInterval) > 0 ? refreshInterval : RETRY_INTERVAL;
        placement = new ReplicaPlacement(cluster, now, validFor.toNanos(), replicas);
        return replicas;
    }

    @Nullable
    private Map<Range<BigInteger>, Set<CassandraInstance>> fetchCassandraReplicas(Set<CassandraInstance> cluster)
    throws Exception
    {
        Set<String> keyspaces = schemaSupplier.getCDCEnabledTables()
                                              .get(FETCH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                              .stream()
                                              .map(CqlTable::keyspace)
                                              .collect(Collectors.toSet());
        if (keyspaces.isEmpty())
        {
            return null;
        }

        // Replicas are identified by "address:port", which the replica metadata translates to the fqdn the
        // instances are named by
        Map<String, CassandraInstance> instanceByName = new HashMap<>();
        cluster.stream().filter(dcFilter).forEach(instance -> instanceByName.put(instance.nodeName(), instance));
        List<CassandraInstance> sidecars = cluster.stream().filter(downMonitor::isUp).collect(Collectors.toList());
        if (sidecars.isEmpty())
        {
            sidecars = new ArrayList<>(cluster);
        }

        Map<Range<BigInteger>, Set<CassandraInstance>> replicas = new HashMap<>();
        for (String keyspace : keyspaces)
        {
            TokenRangeReplicasResponse response = sidecarCdcClient.tokenRangeReplicas(sidecars, keyspace)
                                                                  .get(FETCH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = response == null ? null
                                                                        : isNullOrEmpty(response.writeReplicas())
                                                                          ? response.readReplicas()
                                                                          : response.writeReplicas();
            if (isNullOrEmpty(replicaInfos) || response.replicaMetadata() == null)
            {
                throw new IllegalStateException("Sidecar returned an incomplete token range replicas response for keyspace " + keyspace);
            }
            for (TokenRangeReplicasResponse.ReplicaInfo replicaInfo : replicaInfos)
            {
                Set<CassandraInstance> rangeReplicas = new HashSet<>();
                for (Map.Entry<String, List<String>> byDatacenter : replicaInfo.replicasByDatacenter().entrySet())
                {
                    // Replicas outside the CDC datacenter are not read
                    String dc = clusterConfigProvider.dc();
                    if (dc != null && !dc.equalsIgnoreCase(byDatacenter.getKey()))
                    {
                        continue;
                    }
                    for (String replica : byDatacenter.getValue())
                    {
                        TokenRangeReplicasResponse.ReplicaMetadata metadata = response.replicaMetadata().get(replica);
                        CassandraInstance instance = metadata == null ? null : instanceByName.get(metadata.fqdn());
                        if (instance == null && metadata != null)
                        {
                            instance = instanceByName.get(metadata.address());
                        }
                        if (instance == null)
                        {
                            // Skipping the replica could miss its mutations, so the replicas are not taken from
                            // Cassandra until the cluster includes it
                            throw new IllegalStateException(String.format(
                            "Replica %s of range (%s, %s] in keyspace %s is not in the cluster",
                            metadata == null ? replica : metadata.fqdn(), replicaInfo.start(), replicaInfo.end(), keyspace));
                        }
                        rangeReplicas.add(instance);
                    }
                }
                if (!rangeReplicas.isEmpty())
                {
                    Range<BigInteger> range = Range.openClosed(new BigInteger(replicaInfo.start()),
                                                               new BigInteger(replicaInfo.end()));
                    replicas.computeIfAbsent(range, ignored -> new HashSet<>()).addAll(rangeReplicas);
                }
            }
        }
        LOGGER.info("Using token range replicas reported by Cassandra keyspaces={} numRanges={}", keyspaces, replicas.size());
        return replicas;
    }

    private static boolean isNullOrEmpty(@Nullable List<?> list)
    {
        return list == null || list.isEmpty();
    }

    @Override
    public Stream<CommitLog> logs(@Nullable TokenRange tokenRange)
    {
        //TODO: a future improvement could read CommitLog directly from disk if CassandraInstance is local
        List<CassandraInstance> instances;
        if (tokenRange != null)
        {
            // find only Cassandra instances that replicate the range filter
            instances = replicas(tokenRange);
        }
        else
        {
            instances = new ArrayList<>(ring().instances());
        }

        Set<CassandraInstance> replicas = instances.stream()
                                                   .filter(dcFilter)
                                                   .filter(downMonitor::isUp)
                                                   .collect(Collectors.toSet());

        List<CompletableFuture<List<CommitLog>>> futures = replicas
                                                           .stream()
                                                           .map(this::listInstance)
                                                           .collect(Collectors.toList());

        // block to list replicas
        List<List<CommitLog>> replicaLogs = FutureUtils.awaitAll(
        futures,
        true,
        (throwable -> LOGGER.warn("Failed to list CDC commit logs on instance", (ThrowableUtils.rootCause(throwable))))
        );

        return replicaLogs.stream().flatMap(Collection::stream);
    }

    protected CompletableFuture<List<CommitLog>> listInstance(CassandraInstance instance)
    {
        return sidecarCdcClient
               .listCdcCommitLogSegments(instance)
               .whenComplete((response, throwable) -> {
                   if (response == null || throwable != null)
                   {
                       LOGGER.warn("Failed to list commit log segments on instance nodeName={}", instance.nodeName(), throwable);
                       downMonitor.hintDown(instance);
                   }
               });
    }

    /**
     * The token range replicas taken from Cassandra, with the cluster and time they were fetched for
     */
    private static final class ReplicaPlacement
    {
        private final Set<CassandraInstance> cluster;
        private final long fetchedAtNanos;
        private final long validForNanos;
        @Nullable
        private final Map<Range<BigInteger>, Set<CassandraInstance>> replicas;

        private ReplicaPlacement(Set<CassandraInstance> cluster,
                                 long fetchedAtNanos,
                                 long validForNanos,
                                 @Nullable Map<Range<BigInteger>, Set<CassandraInstance>> replicas)
        {
            this.cluster = new HashSet<>(cluster);
            this.fetchedAtNanos = fetchedAtNanos;
            this.validForNanos = validForNanos;
            this.replicas = replicas;
        }
    }
}
