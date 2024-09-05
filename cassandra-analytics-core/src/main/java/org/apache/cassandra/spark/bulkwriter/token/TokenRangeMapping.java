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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

public class TokenRangeMapping<I extends CassandraInstance> implements Serializable
{
    private static final long serialVersionUID = -7284933683815811160L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeMapping.class);

    private final Partitioner partitioner;
    private final ReplicationFactor replicationFactor;
    private final transient Set<I> allInstances;
    private final transient RangeMap<BigInteger, List<I>> replicasByTokenRange;
    private final transient Multimap<I, Range<BigInteger>> tokenRangeMap;
    private final transient Map<String, Set<I>> writeReplicasByDC;
    private final transient Map<String, Set<I>> pendingReplicasByDC;

    public static <I extends CassandraInstance>
    TokenRangeMapping<I> create(Supplier<TokenRangeReplicasResponse> topologySupplier,
                                Supplier<Partitioner> partitionerSupplier,
                                Supplier<ReplicationFactor> replicationFactorSupplier,
                                Function<ReplicaMetadata, I> instanceCreator)
    {
        TokenRangeReplicasResponse response = topologySupplier.get();
        Map<String, I> instanceByIpAddress = new HashMap<>(response.replicaMetadata().size());
        response.replicaMetadata()
                .forEach((ipAddress, metadata) -> instanceByIpAddress.put(ipAddress, instanceCreator.apply(metadata)));

        Multimap<I, Range<BigInteger>> tokenRangesByInstance = tokenRangesByInstance(response.writeReplicas(),
                                                                                     instanceByIpAddress);

        // Each token range has hosts by DC. We collate them across all ranges into all hosts by DC
        Map<String, Set<I>> writeReplicasByDC = new HashMap<>();
        Map<String, Set<I>> pendingReplicasByDC = new HashMap<>();
        Set<I> allInstances = new HashSet<>(instanceByIpAddress.values());
        for (I instance : allInstances)
        {
            Set<I> dc = writeReplicasByDC.computeIfAbsent(instance.datacenter(), k -> new HashSet<>());
            dc.add(instance);
            if (instance.nodeState().isPending)
            {
                Set<I> pendingInDc = pendingReplicasByDC.computeIfAbsent(instance.datacenter(), k -> new HashSet<>());
                pendingInDc.add(instance);
            }
        }

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Fetched token-ranges with dcs={}, write_replica_count={}, pending_replica_count={}",
                         writeReplicasByDC.keySet(),
                         writeReplicasByDC.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()).size(),
                         pendingReplicasByDC.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()).size());
        }

        Map<String, ReplicaMetadata> replicaMetadata = response.replicaMetadata();
        // Include availability info so CL checks can use it to exclude replacement hosts
        return new TokenRangeMapping<>(partitionerSupplier.get(),
                                       replicationFactorSupplier.get(),
                                       writeReplicasByDC,
                                       pendingReplicasByDC,
                                       tokenRangesByInstance,
                                       allInstances);
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
                             ReplicationFactor replicationFactor,
                             Map<String, Set<I>> writeReplicasByDC,
                             Map<String, Set<I>> pendingReplicasByDC,
                             Multimap<I, Range<BigInteger>> tokenRanges,
                             Set<I> allInstances)
    {
        this.partitioner = partitioner;
        this.replicationFactor = replicationFactor;
        this.tokenRangeMap = tokenRanges;
        this.pendingReplicasByDC = pendingReplicasByDC;
        this.writeReplicasByDC = writeReplicasByDC;
        this.allInstances = allInstances;
        // Populate reverse mapping of ranges to replicas
        this.replicasByTokenRange = populateReplicas();
    }

    public Partitioner partitioner()
    {
        return partitioner;
    }

    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
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

    public Set<I> getPendingReplicas()
    {
        return (pendingReplicasByDC == null || pendingReplicasByDC.isEmpty())
               ? Collections.emptySet() : pendingReplicasByDC.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    /**
     * Get the write replicas of sub-ranges that overlap with the input range.
     *
     * @param range range to check. The range can potentially overlap with multiple ranges.
     *              For example, a down node adds one failure of a token range that covers multiple primary token ranges that replicate to it.
     * @param localDc local DC name to filter out non-local-DC instances. The parameter is optional. When not present, i.e. null, no filtering is applied
     * @return the write replicas of sub-ranges
     */
    public Map<Range<BigInteger>, Set<I>> getWriteReplicasOfRange(Range<BigInteger> range, @Nullable String localDc)
    {
        Map<Range<BigInteger>, List<I>> subRangeReplicas = replicasByTokenRange.subRangeMap(range).asMapOfRanges();
        Function<List<I>, Set<I>> inDcInstances = instances -> {
            if (localDc != null)
            {
                return instances.stream()
                                .filter(instance -> instance.datacenter().equalsIgnoreCase(localDc))
                                .collect(Collectors.toSet());
            }
            return new HashSet<>(instances);
        };
        return subRangeReplicas.entrySet()
                               .stream()
                               .collect(Collectors.toMap(Map.Entry::getKey, entry -> inDcInstances.apply(entry.getValue())));
    }

    public Set<I> getWriteReplicas()
    {
        return (writeReplicasByDC == null || writeReplicasByDC.isEmpty())
               ? Collections.emptySet() : writeReplicasByDC.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    // Used for writes
    public RangeMap<BigInteger, List<I>> getRangeMap()
    {
        return this.replicasByTokenRange;
    }

    public RangeMap<BigInteger, List<I>> getSubRanges(Range<BigInteger> tokenRange)
    {
        return replicasByTokenRange.subRangeMap(tokenRange);
    }

    public Multimap<I, Range<BigInteger>> getTokenRanges()
    {
        return tokenRangeMap;
    }

    private RangeMap<BigInteger, List<I>> populateReplicas()
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
        return writeReplicasByDC.equals(that.writeReplicasByDC)
               && pendingReplicasByDC.equals(that.pendingReplicasByDC);
    }

    @Override
    public int hashCode()
    {
        int result = tokenRangeMap.hashCode();
        result = 31 * result + pendingReplicasByDC.hashCode();
        result = 31 * result + writeReplicasByDC.hashCode();
        result = 31 * result + replicasByTokenRange.hashCode();
        return result;
    }
}
