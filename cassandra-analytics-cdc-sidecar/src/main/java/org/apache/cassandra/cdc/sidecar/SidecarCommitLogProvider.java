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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogProvider;
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
 * This class uses the `CassandraRing` object to only select CassandraInstance objects
 * that overlap with the TokenRange supplied.
 */
public class SidecarCommitLogProvider implements CommitLogProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarCommitLogProvider.class);

    private final ClusterConfigProvider clusterConfigProvider;
    private final SidecarCdcClient sidecarCdcClient;
    private final SidecarDownMonitor downMonitor;
    private final Predicate<CassandraInstance> dcFilter;
    private final ReplicationFactorSupplier replicationFactorSupplier;

    public SidecarCommitLogProvider(ClusterConfigProvider clusterConfigProvider,
                                    SidecarCdcClient sidecarCdcClient,
                                    SidecarDownMonitor downMonitor,
                                    ReplicationFactorSupplier replicationFactorSupplier)
    {
        this.clusterConfigProvider = clusterConfigProvider;
        this.sidecarCdcClient = sidecarCdcClient;
        this.downMonitor = downMonitor;
        this.replicationFactorSupplier = replicationFactorSupplier;
        this.dcFilter = inst -> {
            final String dc = clusterConfigProvider.dc();
            return dc == null || inst.dataCenter().equalsIgnoreCase(dc);
        };
    }

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

    @Override
    public Stream<CommitLog> logs(@Nullable TokenRange tokenRange)
    {
        //TODO: a future improvement could read CommitLog directly from disk if CassandraInstance is local
        List<CassandraInstance> instances;
        if (tokenRange != null)
        {
            // find only Cassandra instances that overlap with range filter
            instances = ring().getSubRanges(toGuavaRange(tokenRange)).asMapOfRanges().values().stream()
                              .flatMap(Collection::stream)
                              .collect(Collectors.toList());
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
}
