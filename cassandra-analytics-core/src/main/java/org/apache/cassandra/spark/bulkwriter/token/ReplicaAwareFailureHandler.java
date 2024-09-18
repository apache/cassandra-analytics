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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.model.NodeStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

public class ReplicaAwareFailureHandler<Instance extends CassandraInstance>
{
    public class FailuresPerInstance
    {
        private final Multimap<Instance, String> errorMessagesPerInstance;

        public FailuresPerInstance()
        {
            this.errorMessagesPerInstance = ArrayListMultimap.create();
        }

        public FailuresPerInstance(Multimap<Instance, String> errorMessagesPerInstance)
        {
            this.errorMessagesPerInstance = ArrayListMultimap.create(errorMessagesPerInstance);
        }

        public FailuresPerInstance copy()
        {
            return new FailuresPerInstance(this.errorMessagesPerInstance);
        }

        public Set<Instance> instances()
        {
            return errorMessagesPerInstance.keySet();
        }

        public void addErrorForInstance(Instance instance, String errorMessage)
        {
            errorMessagesPerInstance.put(instance, errorMessage);
        }

        public boolean hasError(Instance instance)
        {
            return errorMessagesPerInstance.containsKey(instance)
                   && !errorMessagesPerInstance.get(instance).isEmpty();
        }

        public void forEachInstance(BiConsumer<Instance, Collection<String>> instanceErrorsConsumer)
        {
            errorMessagesPerInstance.asMap().forEach(instanceErrorsConsumer);
        }
    }

    public class ConsistencyFailurePerRange
    {
        public final Range<BigInteger> range;
        public final FailuresPerInstance failuresPerInstance;

        public ConsistencyFailurePerRange(Range<BigInteger> range, FailuresPerInstance failuresPerInstance)
        {
            this.range = range;
            this.failuresPerInstance = failuresPerInstance;
        }
    }

    // failures captures per each range; note that failures do not necessarily fail a range, as long as consistency level is considered
    private final RangeMap<BigInteger, FailuresPerInstance> rangeFailuresMap = TreeRangeMap.create();

    public ReplicaAwareFailureHandler(Partitioner partitioner)
    {
        rangeFailuresMap.put(Range.openClosed(partitioner.minToken(), partitioner.maxToken()), new FailuresPerInstance());
    }

    /**
     * Adds a new token range as a failed token range, with errors on given instance.
     * <p>
     * It's guaranteed that failedRangesMap has overlapping ranges for the range we are trying to insert (Check
     * constructor, we are adding complete ring first).
     * <p>
     * So the scheme is to get list of overlapping ranges first. For each overlapping range get the failure map.
     * Make a copy of the map and add new failure to this map. It's important we make the copy and not use the
     * one returned from failedRangesMap map. As our range could be overlapping partially and the map could be used
     * by other range.
     *
     * @param tokenRange  the range which failed
     * @param casInstance the instance on which the range failed
     * @param errMessage  the error that occurred for this particular range/instance pair
     */
    public synchronized void addFailure(Range<BigInteger> tokenRange, Instance casInstance, String errMessage)
    {
        RangeMap<BigInteger, FailuresPerInstance> overlappingFailures = rangeFailuresMap.subRangeMap(tokenRange);
        RangeMap<BigInteger, FailuresPerInstance> mappingsToAdd = TreeRangeMap.create();

        for (Map.Entry<Range<BigInteger>, FailuresPerInstance> entry : overlappingFailures.asMapOfRanges().entrySet())
        {
            FailuresPerInstance newErrorMap = entry.getValue().copy();
            newErrorMap.addErrorForInstance(casInstance, errMessage);
            mappingsToAdd.put(entry.getKey(), newErrorMap);
        }
        rangeFailuresMap.putAll(mappingsToAdd);
    }

    public Set<Instance> getFailedInstances()
    {
        return rangeFailuresMap.asMapOfRanges().values()
                               .stream()
                               .map(FailuresPerInstance::instances)
                               .flatMap(Collection::stream)
                               .collect(Collectors.toSet());
    }

    /**
     * Given the number of failed instances for each token range, validates if the consistency guarantees are maintained
     * for the size of the ring and the consistency level.
     *
     * @param tokenRangeMapping the mapping of token ranges to a Cassandra instance
     * @param cl                the desired consistency level
     * @param localDC           the local datacenter
     * @return list of failed token ranges that break consistency. This should ideally be empty for a
     * successful operation.
     */
    public synchronized List<ConsistencyFailurePerRange>
    getFailedRanges(TokenRangeMapping<Instance> tokenRangeMapping,
                    ConsistencyLevel cl,
                    @Nullable String localDC,
                    ReplicationFactor replicationFactor)
    {
        Preconditions.checkArgument((cl.isLocal() && localDC != null) || (!cl.isLocal() && localDC == null),
                                    "Not a valid pair of consistency level configuration. " +
                                    "Consistency level: " + cl + " localDc: " + localDC);
        List<ConsistencyFailurePerRange> failedRanges = new ArrayList<>();

        for (Map.Entry<Range<BigInteger>, FailuresPerInstance> failedRangeEntry : rangeFailuresMap.asMapOfRanges()
                                                                                                  .entrySet())
        {
            Range<BigInteger> range = failedRangeEntry.getKey();
            FailuresPerInstance errorMap = failedRangeEntry.getValue();
            Set<Instance> failedReplicas = errorMap.instances()
                                                   .stream()
                                                   .filter(errorMap::hasError)
                                                   .collect(Collectors.toSet());

            // no failures found for the range; skip consistency check on this one and move on
            if (failedReplicas.isEmpty())
            {
                continue;
            }

            tokenRangeMapping.getWriteReplicasOfRange(range, localDC)
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
                                  Set<Instance> liveAndDown,
                                  Set<Instance> failedReplicas)
    {
        Set<Instance> liveReplicas = liveAndDown.stream()
                                                .filter(instance -> instance.nodeStatus() == NodeStatus.UP)
                                                .collect(Collectors.toSet());
        Set<Instance> pendingReplicas = liveAndDown.stream()
                                                   .filter(instance -> instance.nodeState().isPending)
                                                   .collect(Collectors.toSet());
        // success is assumed if not failed
        Set<Instance> succeededReplicas = liveReplicas.stream()
                                                      .filter(instance -> !failedReplicas.contains(instance))
                                                      .collect(Collectors.toSet());

        return cl.canBeSatisfied(succeededReplicas, pendingReplicas, replicationFactor, localDC);
    }
}
