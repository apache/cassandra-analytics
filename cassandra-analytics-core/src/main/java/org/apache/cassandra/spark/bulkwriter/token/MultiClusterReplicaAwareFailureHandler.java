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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.ClusterConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
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
    private final MultiClusterContainer<SingleClusterReplicaAwareFailureHandler<I>> failureHandlers = new MultiClusterContainer<>();
    private final Partitioner partitioner;

    public MultiClusterReplicaAwareFailureHandler(Partitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    @Override
    public synchronized void addFailure(Range<BigInteger> tokenRange, I instance, String errMessage)
    {
        String clusterId = instance.clusterId();
        failureHandlers.updateValue(clusterId, handler -> {
            SingleClusterReplicaAwareFailureHandler<I> h = handler;
            if (handler == null)
            {
                h = new SingleClusterReplicaAwareFailureHandler<>(partitioner, clusterId);
            }
            h.addFailure(tokenRange, instance, errMessage);
            return h;
        });
    }

    @Override
    public synchronized Set<I> getFailedInstances()
    {
        // failed instances merged from all clusters
        HashSet<I> failedInstances = new HashSet<>();
        failureHandlers.forEach((ignored, handler) -> {
            failedInstances.addAll(handler.getFailedInstances());
        });
        return failedInstances;
    }

    @Override
    public synchronized List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange>
    getFailedRanges(TokenRangeMapping<I> tokenRangeMapping,
                    JobInfo job,
                    ClusterInfo cluster)
    {
        if (!job.isCoordinatedWriteEnabled())
        {
            // if the retrieved failure handler is null, it indicates no failure has been added, i.e. no failed ranges
            SingleClusterReplicaAwareFailureHandler<I> singleCluster = failureHandlers.getValueOrNull(null);
            return singleCluster == null ? Collections.emptyList() : singleCluster.getFailedRanges(tokenRangeMapping, job, cluster);
        }

        List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange> failurePerRanges = new ArrayList<>();
        CoordinatedWriteConf coordinatedWriteConf = job.coordinatedWriteConf();
        Preconditions.checkState(coordinatedWriteConf != null,
                                 "CoordinatedWriteConf is absent for multi-cluster write");
        Preconditions.checkState(cluster instanceof CassandraClusterInfoGroup,
                                 "Not a CassandraClusterInfoGroup for multi-cluster write");
        CassandraClusterInfoGroup group = (CassandraClusterInfoGroup) cluster;
        failureHandlers.forEach((clusterId, handler) -> {
            ClusterConf clusterConf = coordinatedWriteConf.cluster(clusterId);
            ClusterInfo clusterInfo = group.getValueOrThrow(clusterId);
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
        throw new UnsupportedOperationException("Not implemented for MultiClusterReplicaAwareFailureHandler");
    }
}
