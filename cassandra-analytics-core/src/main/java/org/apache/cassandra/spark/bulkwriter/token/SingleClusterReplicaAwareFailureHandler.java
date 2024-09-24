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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.model.NodeStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

/**
 * ReplicaAwareFailureHandler for a single cluster
 * The handler should be constructed by {@link MultiClusterReplicaAwareFailureHandler} only, hence package-private
 * @param <I> CassandraInstance type
 */
class SingleClusterReplicaAwareFailureHandler<I extends CassandraInstance> extends ReplicaAwareFailureHandler<I>
{
    // failures captures per each range; note that failures do not necessarily fail a range, as long as consistency level is considered
    @GuardedBy("this")
    private final RangeMap<BigInteger, FailuresPerInstance> rangeFailuresMap = TreeRangeMap.create();

    @GuardedBy("this")
    private boolean isEmpty = true;

    @Nullable
    private String clusterId;

    SingleClusterReplicaAwareFailureHandler(Partitioner partitioner, String clusterId)
    {
        this.clusterId = clusterId;
        rangeFailuresMap.put(Range.openClosed(partitioner.minToken(), partitioner.maxToken()), new FailuresPerInstance());
    }

    /**
     * Check whether the handler contains any failure
     * @return true if there is at least a failure; false otherwise.
     */
    public boolean isEmpty()
    {
        return isEmpty;
    }

    @Override
    public List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange>
    getFailedRanges(TokenRangeMapping<I> tokenRangeMapping, JobInfo job, ClusterInfo cluster)
    {
        return getFailedRangesInternal(tokenRangeMapping, job.getConsistencyLevel(), job.getLocalDC(), cluster.replicationFactor());
    }

    @Override
    public synchronized void addFailure(Range<BigInteger> tokenRange, I instance, String errMessage)
    {
        RangeMap<BigInteger, FailuresPerInstance> overlappingFailures = rangeFailuresMap.subRangeMap(tokenRange);
        RangeMap<BigInteger, FailuresPerInstance> mappingsToAdd = TreeRangeMap.create();

        for (Map.Entry<Range<BigInteger>, FailuresPerInstance> entry : overlappingFailures.asMapOfRanges().entrySet())
        {
            FailuresPerInstance newErrorMap = entry.getValue().copy();
            newErrorMap.addErrorForInstance(instance, errMessage);
            mappingsToAdd.put(entry.getKey(), newErrorMap);
        }
        rangeFailuresMap.putAll(mappingsToAdd);
        isEmpty = false;
    }

    @Override
    public synchronized Set<I> getFailedInstances()
    {
        if (isEmpty)
        {
            return Collections.emptySet();
        }

        return rangeFailuresMap.asMapOfRanges()
                               .values()
                               .stream()
                               .map(FailuresPerInstance::instances)
                               .flatMap(Collection::stream)
                               .collect(Collectors.toSet());
    }

    @Override
    protected synchronized List<ReplicaAwareFailureHandler<I>.ConsistencyFailurePerRange>
    getFailedRangesInternal(TokenRangeMapping<I> tokenRangeMapping,
                            ConsistencyLevel cl,
                            @Nullable String localDC,
                            ReplicationFactor replicationFactor)
    {
        Preconditions.checkArgument((cl.isLocal() && localDC != null) || (!cl.isLocal() && localDC == null),
                                    "Not a valid pair of consistency level configuration. " +
                                    "Consistency level: " + cl + " localDc: " + localDC);
        List<ConsistencyFailurePerRange> failedRanges = new ArrayList<>();

        if (isEmpty)
        {
            return failedRanges;
        }

        for (Map.Entry<Range<BigInteger>, FailuresPerInstance> failedRangeEntry : rangeFailuresMap.asMapOfRanges().entrySet())
        {
            Range<BigInteger> range = failedRangeEntry.getKey();
            FailuresPerInstance errorMap = failedRangeEntry.getValue();
            Set<I> failedReplicas = errorMap.instances()
                                            .stream()
                                            .filter(errorMap::hasError)
                                            .collect(Collectors.toSet());

            // no failures found for the range; skip consistency check on this one and move on
            if (failedReplicas.isEmpty())
            {
                continue;
            }

            tokenRangeMapping.getWriteReplicasOfRange(range, instance -> {
                                 boolean shouldKeep = instance.datacenter().equalsIgnoreCase(localDC);
                                 if (shouldKeep && clusterId != null)
                                 {
                                     shouldKeep = clusterId.equalsIgnoreCase(instance.clusterId());
                                 }
                                 return shouldKeep;
                             })
                             .forEach((subrange, liveAndDown) -> {
                                 if (!checkSubrange(cl, localDC, replicationFactor, liveAndDown, failedReplicas))
                                 {
                                     failedRanges.add(new ConsistencyFailurePerRange(subrange, errorMap));
                                 }
                             });
        }

        return failedRanges;
    }

    /**
     * Check whether a CL can be satisfied for each sub-range.
     * @return true if consistency is satisfied; false otherwise.
     */
    private boolean checkSubrange(ConsistencyLevel cl,
                                  @Nullable String localDC,
                                  ReplicationFactor replicationFactor,
                                  Set<I> liveAndDown,
                                  Set<I> failedReplicas)
    {
        Set<I> liveReplicas = liveAndDown.stream()
                                         .filter(instance -> instance.nodeStatus() == NodeStatus.UP)
                                         .collect(Collectors.toSet());
        Set<I> pendingReplicas = liveAndDown.stream()
                                            .filter(instance -> instance.nodeState().isPending)
                                            .collect(Collectors.toSet());
        // success is assumed if not failed
        Set<I> succeededReplicas = liveReplicas.stream()
                                               .filter(instance -> !failedReplicas.contains(instance))
                                               .collect(Collectors.toSet());

        return cl.canBeSatisfied(succeededReplicas, pendingReplicas, replicationFactor, localDC);
    }
}
