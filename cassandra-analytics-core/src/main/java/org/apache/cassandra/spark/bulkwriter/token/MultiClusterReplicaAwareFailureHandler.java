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

package org.apache.cassandra.spark.bulkwriter.token;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.coordinatedwrite.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.coordinatedwrite.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.coordinatedwrite.CoordinatedWriteConf.ClusterConf;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

/**
 * A ReplicaAwareFailureHandler that can handle multiple clusters, including the case of single cluster.
 *
 * @param <I> CassandraInstance type
 */
public class MultiClusterReplicaAwareFailureHandler<I extends CassandraInstance> extends ReplicaAwareFailureHandler<I>
{
    // default failure handler for CassandraInstance w/o specifying the belonging clusterId
    private final SingleClusterReplicaAwareFailureHandler<I> defaultFailureHandler;
    private final Map<String, SingleClusterReplicaAwareFailureHandler<I>> failureHandlerPerCluster = new HashMap<>();

    private final Partitioner partitioner;

    public MultiClusterReplicaAwareFailureHandler(Partitioner partitioner)
    {
        this.partitioner = partitioner;
        this.defaultFailureHandler = new SingleClusterReplicaAwareFailureHandler<>(partitioner);
    }

    @Override
    public synchronized void addFailure(Range<BigInteger> tokenRange, I instance, String errMessage)
    {
        if (instance.hasClusterId())
        {
            String clusterId = instance.clusterId();
            ReplicaAwareFailureHandler<I> handler = failureHandlerPerCluster
                                                    .computeIfAbsent(clusterId, k -> new SingleClusterReplicaAwareFailureHandler<>(partitioner));
            handler.addFailure(tokenRange, instance, errMessage);
        }
        else
        {
            defaultFailureHandler.addFailure(tokenRange, instance, errMessage);
        }
    }

    @Override
    public Set<I> getFailedInstances()
    {
        if (failureHandlerPerCluster.isEmpty())
        {
            return defaultFailureHandler.getFailedInstances();
        }

        // failed instances merged from all clusters
        HashSet<I> failedInstances = new HashSet<>(defaultFailureHandler.getFailedInstances());
        failureHandlerPerCluster.values().forEach(handler -> failedInstances.addAll(handler.getFailedInstances()));
        return failedInstances;
    }

    @Override
    public synchronized List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange>
    getFailedRanges(TokenRangeMapping<I> tokenRangeMapping,
                    JobInfo job,
                    ClusterInfo cluster)
    {
        if (failureHandlerPerCluster.isEmpty())
        {
            return defaultFailureHandler.getFailedRanges(tokenRangeMapping, job, cluster);
        }

        List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange> failurePerRanges = new ArrayList<>();
        CoordinatedWriteConf coordinatedWriteConf = job.coordinatedWriteConf();
        Preconditions.checkState(coordinatedWriteConf != null,
                                 "CoordinatedWriteConf is absent for multi-cluster write");
        Preconditions.checkState(cluster instanceof CassandraClusterInfoGroup,
                                 "Not a CassandraClusterInfoGroup for multi-cluster write");
        CassandraClusterInfoGroup group = (CassandraClusterInfoGroup) cluster;
        failureHandlerPerCluster.forEach((clusterId, handler) -> {
            ClusterConf clusterConf = Objects.requireNonNull(coordinatedWriteConf.cluster(clusterId),
                                                             () -> "ClusterConf is absent for " + clusterId);
            ClusterInfo clusterInfo = Objects.requireNonNull(group.cluster(clusterId),
                                                             () -> "ClusterInfo is not found for " + clusterId);
            ReplicationFactor rf = clusterInfo.replicationFactor();
            failurePerRanges.addAll(handler.getFailedRangesInternal(tokenRangeMapping, job.getConsistencyLevel(), clusterConf.localDc(), rf));
        });
        return failurePerRanges;
    }

    @Override
    protected List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange>
    getFailedRangesInternal(TokenRangeMapping<I> tokenRangeMapping,
                            ConsistencyLevel cl,
                            @Nullable String localDC,
                            ReplicationFactor replicationFactor)
    {
        throw new NotImplementedException("Not implemented for MultiClusterReplicaAwareFailureHandler");
    }
}
