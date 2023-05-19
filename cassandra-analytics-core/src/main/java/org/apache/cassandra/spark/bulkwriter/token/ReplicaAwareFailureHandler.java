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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import org.apache.cassandra.spark.common.model.CassandraInstance;

public class ReplicaAwareFailureHandler<Instance extends CassandraInstance>
{
    private final CassandraRing<Instance> ring;
    private final RangeMap<BigInteger, Multimap<Instance, String>> failedRangesMap = TreeRangeMap.create();

    public ReplicaAwareFailureHandler(CassandraRing<Instance> ring)
    {
        this.ring = ring;
        failedRangesMap.put(Range.closed(ring.getPartitioner().minToken(),
                                         ring.getPartitioner().maxToken()),
                            ArrayListMultimap.create());
    }

    /**
     * Adds a new token range as a failed token range, with errors on given instance.
     *
     * It's guaranteed that failedRangesMap has overlapping ranges for the range we are trying to insert (Check
     * constructor, we are adding complete ring first).
     *
     * So the scheme is to get list of overlapping ranges first. For each overlapping range get the failure map.
     * Make a copy of the map and add new failure to this map. It's important we make the copy and not use the
     * one returned from failedRangesMap map. As our range could be overlapping partially and the map could be used
     * by other range.
     *
     * @param tokenRange the range which failed
     * @param casInstance the instance on which the range failed
     * @param errMessage the error that occurred for this particular range/instance pair
     */
    public void addFailure(Range<BigInteger> tokenRange, Instance casInstance, String errMessage)
    {
        RangeMap<BigInteger, Multimap<Instance, String>> overlappingFailures = failedRangesMap.subRangeMap(tokenRange);
        RangeMap<BigInteger, Multimap<Instance, String>> mappingsToAdd = TreeRangeMap.create();

        for (Map.Entry<Range<BigInteger>, Multimap<Instance, String>> entry : overlappingFailures.asMapOfRanges().entrySet())
        {
            Multimap<Instance, String> newErrorMap = ArrayListMultimap.create(entry.getValue());

            newErrorMap.put(casInstance, errMessage);
            mappingsToAdd.put(entry.getKey(), newErrorMap);
        }
        failedRangesMap.putAll(mappingsToAdd);
    }

    public boolean hasFailed(ConsistencyLevel consistencyLevel, String localDC)
    {
        return !getFailedEntries(consistencyLevel, localDC).isEmpty();
    }

    @SuppressWarnings("unused")  // Convenience method can become useful in the future
    public Collection<Range<BigInteger>> getFailedRanges(ConsistencyLevel consistencyLevel, String localDC)
    {
        return getFailedEntries(consistencyLevel, localDC).stream()
                .map(AbstractMap.SimpleEntry::getKey)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unused")  // Convenience method can become useful in the future
    public Multimap<Instance, String> getFailedInstances(ConsistencyLevel consistencyLevel, String localDC)
    {
        Multimap<Instance, String> failedInstances = ArrayListMultimap.create();
        getFailedEntries(consistencyLevel, localDC).stream()
                .flatMap(failedMultiEntry -> failedMultiEntry.getValue().entries().stream())
                .forEach(failedEntry -> failedInstances.put(failedEntry.getKey(), failedEntry.getValue()));

        return failedInstances;
    }

    public Collection<AbstractMap.SimpleEntry<Range<BigInteger>, Multimap<Instance, String>>> getFailedEntries(
            ConsistencyLevel consistencyLevel,
            String localDC)
    {
        List<AbstractMap.SimpleEntry<Range<BigInteger>, Multimap<Instance, String>>> failedEntries = new ArrayList<>();

        for (Map.Entry<Range<BigInteger>, Multimap<Instance, String>> failedRangeEntry : failedRangesMap.asMapOfRanges().entrySet())
        {
            Multimap<Instance, String> errorMap = failedRangeEntry.getValue();
            Collection<Instance> failedInstances = errorMap.keySet().stream()
                    .filter(instance -> !errorMap.get(instance).isEmpty())
                    .collect(Collectors.toList());
            if (!consistencyLevel.checkConsistency(failedInstances, ring.getReplicationFactor(), localDC))
            {
                failedEntries.add(new AbstractMap.SimpleEntry<>(failedRangeEntry.getKey(), failedRangeEntry.getValue()));
            }
        }

        return failedEntries;
    }

    public String toString()
    {
        return "CassandraRing: Tokens: " + ring.getTokens().stream()
                                                           .map(BigInteger::toString)
                                                           .collect(Collectors.joining(",", "[", "]")) + ", "
                 + "ReplicationFactor: " + ring.getReplicationFactor().getReplicationStrategy().name() + " "
                                         + ring.getReplicationFactor().getOptions();
    }
}
