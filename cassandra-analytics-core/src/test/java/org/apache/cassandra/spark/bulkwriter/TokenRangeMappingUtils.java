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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.NodeStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.RangeUtils;

public final class TokenRangeMappingUtils
{
    private TokenRangeMappingUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static TokenRangeMapping<RingInstance> buildTokenRangeMapping(int initialToken, ImmutableMap<String, Integer> rfByDC, int instancesPerDC)
    {
        return buildTokenRangeMapping(initialToken, rfByDC, instancesPerDC, false, -1);
    }

    public static TokenRangeMapping<RingInstance> buildTokenRangeMappingWithFailures(int initialToken,
                                                                                     ImmutableMap<String, Integer> rfByDC,
                                                                                     int instancesPerDC)
    {
        List<RingInstance> instances = getInstances(initialToken, rfByDC, instancesPerDC);
        RingInstance instance = instances.remove(0);
        RingEntry entry = instance.ringEntry();
        RingEntry newEntry = new RingEntry.Builder()
                             .datacenter(entry.datacenter())
                             .port(entry.port())
                             .address(entry.address())
                             .status(NodeStatus.DOWN.name())
                             .state(entry.state())
                             .token(entry.token())
                             .fqdn(entry.fqdn())
                             .rack(entry.rack())
                             .owns(entry.owns())
                             .load(entry.load())
                             .hostId(entry.hostId())
                             .build();
        RingInstance newInstance = new RingInstance(newEntry);
        instances.add(0, newInstance);
        ReplicationFactor replicationFactor = getReplicationFactor(rfByDC);

        Multimap<RingInstance, Range<BigInteger>> tokenRanges = setupTokenRangeMap(Partitioner.Murmur3Partitioner, replicationFactor, instances);
        return new TokenRangeMapping<>(Partitioner.Murmur3Partitioner,
                                       replicationFactor,
                                       tokenRanges,
                                       new HashSet<>(instances));
    }

    public static TokenRangeMapping<RingInstance> buildTokenRangeMapping(int initialToken,
                                                                         ImmutableMap<String, Integer> rfByDC,
                                                                         int instancesPerDC,
                                                                         boolean shouldUpdateToken,
                                                                         int moveTargetToken)
    {

        List<RingInstance> instances = getInstances(initialToken, rfByDC, instancesPerDC);
        if (shouldUpdateToken)
        {
            RingInstance instance = instances.remove(0);
            RingEntry entry = instance.ringEntry();
            RingEntry newEntry = new RingEntry.Builder()
                                 .datacenter(entry.datacenter())
                                 .port(entry.port())
                                 .address(entry.address())
                                 .status(entry.status())
                                 .state(entry.state())
                                 .token(String.valueOf(moveTargetToken))
                                 .fqdn(entry.fqdn())
                                 .rack(entry.rack())
                                 .owns(entry.owns())
                                 .load(entry.load())
                                 .hostId(entry.hostId())
                                 .build();
            RingInstance newInstance = new RingInstance(newEntry);
            instances.add(0, newInstance);
        }

        ReplicationFactor replicationFactor = getReplicationFactor(rfByDC);
        Multimap<RingInstance, Range<BigInteger>> tokenRanges = setupTokenRangeMap(Partitioner.Murmur3Partitioner, replicationFactor, instances);
        return new TokenRangeMapping<>(Partitioner.Murmur3Partitioner,
                                       replicationFactor,
                                       tokenRanges,
                                       new HashSet<>(instances));
    }

    // Used only in tests
    public static Multimap<RingInstance, Range<BigInteger>> setupTokenRangeMap(Partitioner partitioner,
                                                                               ReplicationFactor replicationFactor,
                                                                               List<RingInstance> instances)
    {
        ArrayListMultimap<RingInstance, Range<BigInteger>> tokenRangeMap = ArrayListMultimap.create();

        if (replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.SimpleStrategy)
        {
            tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(instances,
                                                                 replicationFactor.getTotalReplicationFactor(),
                                                                 partitioner));
        }
        else if (replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            for (String dataCenter : replicationFactor.getOptions().keySet())
            {
                int rf = replicationFactor.getOptions().get(dataCenter);
                if (rf == 0)
                {
                    // Apparently, its valid to have zero replication factor in Cassandra
                    continue;
                }
                List<RingInstance> dcInstances = instances.stream()
                                                          .filter(instance -> instance.datacenter().matches(dataCenter))
                                                          .collect(Collectors.toList());
                tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(dcInstances,
                                                                     replicationFactor.getOptions().get(dataCenter),
                                                                     partitioner));
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported replication strategy");
        }
        return tokenRangeMap;
    }

    @NotNull
    private static ReplicationFactor getReplicationFactor(Map<String, Integer> rfByDC)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder()
                                                                          .put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        rfByDC.forEach((key, value) -> optionsBuilder.put(key, value.toString()));
        return new ReplicationFactor(optionsBuilder.build());
    }

    @NotNull
    public static List<RingInstance> getInstances(int initialToken, Map<String, Integer> rfByDC, int instancesPerDc)
    {
        ArrayList<RingInstance> instances = new ArrayList<>();
        int dcOffset = 0;
        for (Map.Entry<String, Integer> rfForDC : rfByDC.entrySet())
        {
            String datacenter = rfForDC.getKey();
            for (int i = 0; i < instancesPerDc; i++)
            {
                int instanceId = i + 1;
                RingEntry.Builder ringEntry = new RingEntry.Builder()
                                              .address("127.0." + dcOffset + "." + instanceId)
                                              .datacenter(datacenter)
                                              .load("0")
                                              // Single DC tokens will be in multiples of 100000
                                              .token(Integer.toString(initialToken + dcOffset + 100_000 * i))
                                              .fqdn(datacenter + "-i" + instanceId)
                                              .rack("Rack")
                                              .hostId("")
                                              .status("UP")
                                              .state("NORMAL")
                                              .owns("");
                instances.add(new RingInstance(ringEntry.build()));
            }
            dcOffset++;
        }
        return instances;
    }

    public static TokenRangeReplicasResponse mockSimpleTokenRangeReplicasResponse(int instancesCount, int replicationFactor)
    {
        long startToken = 0;
        long rangeLength = 100;
        List<ReplicaInfo> replicaInfoList = new ArrayList<>(instancesCount);
        Map<String, ReplicaMetadata> replicaMetadata = new HashMap<>(instancesCount);
        for (int i = 0; i < instancesCount; i++)
        {
            long endToken = startToken + rangeLength;
            List<String> replicas = new ArrayList<>(replicationFactor);
            for (int r = 0; r < replicationFactor; r++)
            {
                replicas.add("localhost" + (i + r) % instancesCount + ":9042");
            }
            Map<String, List<String>> replicasPerDc = new HashMap<>();
            replicasPerDc.put("ignored", replicas);
            ReplicaInfo ri = new ReplicaInfo(String.valueOf(startToken), String.valueOf(endToken), replicasPerDc);
            replicaInfoList.add(ri);
            String address = "localhost" + i;
            int port = 9042;
            String addressWithPort = address + ":" + port;
            ReplicaMetadata rm = new ReplicaMetadata("NORMAL", "UP", address, address, 9042, "ignored");
            replicaMetadata.put(addressWithPort, rm);
            startToken = endToken;
        }
        return new TokenRangeReplicasResponse(replicaInfoList, replicaInfoList, replicaMetadata);
    }
}
