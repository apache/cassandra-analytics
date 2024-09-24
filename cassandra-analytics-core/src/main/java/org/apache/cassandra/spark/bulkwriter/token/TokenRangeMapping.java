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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TokenRangeMapping<I extends CassandraInstance> implements Serializable
{
    private static final long serialVersionUID = -7284933683815811160L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeMapping.class);

    private final Partitioner partitioner;
    private final transient Set<I> allInstances;
    private final transient Set<I> pendingInstances;
    private final transient RangeMap<BigInteger, List<I>> instancesByTokenRange;
    private final transient Multimap<I, Range<BigInteger>> tokenRangeMap;

    public static <I extends CassandraInstance>
    TokenRangeMapping<I> create(Supplier<TokenRangeReplicasResponse> topologySupplier,
                                Supplier<Partitioner> partitionerSupplier,
                                Function<ReplicaMetadata, I> instanceCreator)
    {
        TokenRangeReplicasResponse response = topologySupplier.get();
        Map<String, I> instanceByIpAddress = new HashMap<>(response.replicaMetadata().size());
        response.replicaMetadata()
                .forEach((ipAddress, metadata) -> instanceByIpAddress.put(ipAddress, instanceCreator.apply(metadata)));

        Multimap<I, Range<BigInteger>> tokenRangesByInstance = tokenRangesByInstance(response.writeReplicas(),
                                                                                     instanceByIpAddress);

        Set<I> allInstances = new HashSet<>(instanceByIpAddress.values());
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Fetched token-range replicas: {}", allInstances);
        }

        return new TokenRangeMapping<>(partitionerSupplier.get(),
                                       tokenRangesByInstance,
                                       allInstances);
    }

    /**
     * Consolidate all TokenRangeMapping and produce a new TokenRangeMapping instance
     * @param all list of TokenRangeMapping
     * @return a consolidated TokenRangeMapping
     * @param <I> CassandraInstance type
     */
    public static <I extends CassandraInstance>
    TokenRangeMapping<I> consolidate(@NotNull List<TokenRangeMapping<I>> all)
    {
        Preconditions.checkArgument(!all.isEmpty(), "Cannot consolidate TokenRangeMapping from none");

        if (all.size() == 1)
        {
            return all.get(0);
        }

        Set<Partitioner> partitioners = all.stream().map(t -> t.partitioner).collect(Collectors.toSet());
        Preconditions.checkArgument(partitioners.size() == 1, "Multiple Partitioners found: " + partitioners);
        Partitioner partitioner = all.get(0).partitioner;
        Set<I> allInstances = new HashSet<>();
        Multimap<I, Range<BigInteger>> tokenRangesByInstance = ArrayListMultimap.create();
        for (TokenRangeMapping<I> topology : all)
        {
            allInstances.addAll(topology.allInstances);
            tokenRangesByInstance.putAll(topology.getTokenRanges());
        }
        return new TokenRangeMapping<>(partitioner, tokenRangesByInstance, allInstances);
    }

    private static <I extends CassandraInstance>
    Multimap<I, Range<BigInteger>> tokenRangesByInstance(List<ReplicaInfo> writeReplicas,
                                                         Map<String, I> instanceByIpAddress)
    {
        Multimap<I, Range<BigInteger>> instanceToRangeMap = ArrayListMultimap.create();
        for (ReplicaInfo rInfo : writeReplicas)
        {
            Range<BigInteger> range = Range.openClosed(new BigInteger(rInfo.start()), new BigInteger(rInfo.end()));
            for (Map.Entry<String, List<String>> dcReplicaEntry : rInfo.replicasByDatacenter().entrySet())
            {
                // For each writeReplica, get metadata and update map to include range
                dcReplicaEntry.getValue().forEach(ipAddress -> {
                    if (!instanceByIpAddress.containsKey(ipAddress))
                    {
                        throw new RuntimeException(String.format("No metadata found for instance: %s", ipAddress));
                    }

                    instanceToRangeMap.put(instanceByIpAddress.get(ipAddress), range);
                });
            }
        }
        return instanceToRangeMap;
    }

    public TokenRangeMapping(Partitioner partitioner,
                             Multimap<I, Range<BigInteger>> tokenRanges,
                             Set<I> allInstances)
    {
        this.partitioner = partitioner;
        this.tokenRangeMap = tokenRanges;
        this.allInstances = allInstances;
        this.pendingInstances = allInstances.stream()
                                            .filter(i -> i.nodeState().isPending)
                                            .collect(Collectors.toSet());
        // Populate reverse mapping of ranges to instances
        this.instancesByTokenRange = populateRangeReplicas();
    }

    public Partitioner partitioner()
    {
        return partitioner;
    }

    /**
     * Add a replica with given range to replicaMap (RangeMap pointing to replicas).
     * <p>
     * replicaMap starts with full range (representing complete ring) with empty list of replicas. So, it is
     * guaranteed that range will match one or many ranges in replicaMap.
     * <p>
     * Scheme to add a new replica for a range
     * - Find overlapping rangeMap entries from replicaMap
     * - For each overlapping range, create new replica list by adding new replica to the existing list and add it
     * back to replicaMap
     */
    private static <I extends CassandraInstance> void addReplica(I instance,
                                                                 Range<BigInteger> range,
                                                                 RangeMap<BigInteger, List<I>> replicaMap)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "Range calculations assume range is not wrapped");

        RangeMap<BigInteger, List<I>> replicaRanges = replicaMap.subRangeMap(range);
        RangeMap<BigInteger, List<I>> mappingsToAdd = TreeRangeMap.create();

        replicaRanges.asMapOfRanges().forEach((key, value) -> {
            List<I> replicas = new ArrayList<>(value);
            replicas.add(instance);
            mappingsToAdd.put(key, replicas);
        });
        replicaMap.putAll(mappingsToAdd);
    }

    public Set<I> allInstances()
    {
        return allInstances;
    }

    public Set<I> pendingInstances()
    {
        return pendingInstances;
    }

    /**
     * Get write replica-sets of sub-ranges that overlap with the input range.
     *
     * @param range range to check. The range can potentially overlap with multiple ranges.
     *              For example, a down node adds one failure of a token range that covers multiple primary token ranges that replicate to it.
     * @param localDc local DC name to filter out non-local-DC instances. The parameter is optional. When not present, i.e. null, no filtering is applied
     * @return the write replicas of sub-ranges
     */
    @VisibleForTesting
    public Map<Range<BigInteger>, Set<I>> getWriteReplicasOfRange(Range<BigInteger> range, @Nullable String localDc)
    {
        return getWriteReplicasOfRange(range, instance -> instance.datacenter().equalsIgnoreCase(localDc));
    }

    /**
     * Get write replica-sets of sub-ranges that overlap with the input range.
     *
     * @param range range to check. The range can potentially overlap with multiple ranges.
     *              For example, a down node adds one failure of a token range that covers multiple primary token ranges that replicate to it.
     * @param instanceFilter predicate to filter the instances
     * @return the write replicas of sub-ranges
     */
    public Map<Range<BigInteger>, Set<I>> getWriteReplicasOfRange(Range<BigInteger> range, @Nullable Predicate<I> instanceFilter)
    {
        Map<Range<BigInteger>, List<I>> subRangeReplicas = instancesByTokenRange.subRangeMap(range).asMapOfRanges();
        Function<List<I>, Set<I>> filterAndTransform = instances -> {
            if (instanceFilter != null)
            {
                return instances.stream()
                                .filter(instanceFilter)
                                .collect(Collectors.toSet());
            }
            return new HashSet<>(instances);
        };
        return subRangeReplicas.entrySet()
                               .stream()
                               .collect(Collectors.toMap(Map.Entry::getKey, entry -> filterAndTransform.apply(entry.getValue())));
    }

    // Used for writes
    public RangeMap<BigInteger, List<I>> getRangeMap()
    {
        return this.instancesByTokenRange;
    }

    public RangeMap<BigInteger, List<I>> getSubRanges(Range<BigInteger> tokenRange)
    {
        return instancesByTokenRange.subRangeMap(tokenRange);
    }

    public Multimap<I, Range<BigInteger>> getTokenRanges()
    {
        return tokenRangeMap;
    }

    private RangeMap<BigInteger, List<I>> populateRangeReplicas()
    {
        RangeMap<BigInteger, List<I>> replicaRangeMap = TreeRangeMap.create();
        // Calculate token range to replica mapping
        replicaRangeMap.put(Range.openClosed(this.partitioner.minToken(),
                                             this.partitioner.maxToken()),
                            Collections.emptyList());
        tokenRangeMap.asMap().forEach((inst, ranges) -> ranges.forEach(range -> addReplica(inst, range, replicaRangeMap)));
        return replicaRangeMap;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }

        TokenRangeMapping<?> that = (TokenRangeMapping<?>) other;
        return partitioner == that.partitioner
               && allInstances.equals(that.allInstances)
               && pendingInstances.equals(that.pendingInstances);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitioner, tokenRangeMap, allInstances, pendingInstances, instancesByTokenRange);
    }
}
