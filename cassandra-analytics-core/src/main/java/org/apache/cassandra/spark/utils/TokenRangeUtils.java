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

package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

public class TokenRangeUtils
{
    public static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeUtils.class);

    private TokenRangeUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Converts a TokenRangeReplicasResponse from Cassandra Sidecar into a RangeMap data structure
     * that maps token ranges to their corresponding replica instances.
     *
     * @param tokenRangeReplicas the response from Cassandra Sidecar containing token range and replica information
     * @param instances          collection of available Cassandra instances to match against
     * @param datacenter         the datacenter to exclude from replica selection, or null to include all datacenters
     * @return a RangeMap where keys are token ranges (openClosed BigInteger ranges) and values are lists of
     * CassandraInstance objects that serve as replicas for that range
     */
    public static RangeMap<BigInteger, List<CassandraInstance>> convertTokenRangeReplicasToRangeMap(
    TokenRangeReplicasResponse tokenRangeReplicas,
    Collection<CassandraInstance> instances,
    String datacenter)
    {
        RangeMap<BigInteger, List<CassandraInstance>> replicas = TreeRangeMap.create();
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = tokenRangeReplicas.replicaMetadata();

        for (TokenRangeReplicasResponse.ReplicaInfo replicaInfo : tokenRangeReplicas.readReplicas())
        {
            Range<BigInteger> range = Range.openClosed(new BigInteger(replicaInfo.start()), new BigInteger(replicaInfo.end()));
            List<CassandraInstance> instanceListForRange = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : replicaInfo.replicasByDatacenter().entrySet())
            {
                if (datacenter != null && !datacenter.equals(entry.getKey()))
                {
                    continue;
                }
                instanceListForRange.addAll(entry.getValue().stream()
                                                 .map(ipPort -> metadata.get(ipPort).fqdn())
                                                 .map(fqdn -> getCassandraInstanceByFqdn(instances, fqdn))
                                                 .filter(Objects::nonNull)
                                                 .collect(Collectors.toList()));
            }
            replicas.put(range, instanceListForRange);
        }
        return replicas;
    }

    private static CassandraInstance getCassandraInstanceByFqdn(Collection<CassandraInstance> instances, String fqdn)
    {
        return instances.stream()
                        .filter(instance -> instance.nodeName().equals(fqdn))
                        .findFirst()
                        .orElse(null);
    }

    public static void validateTokenRangeReplicasResponse(TokenRangeReplicasResponse tokenRangeReplicas)
    {
        String msg = getTokenRangeReplicasValidationMessage(tokenRangeReplicas);
        if (msg != null)
        {
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    private static String getTokenRangeReplicasValidationMessage(TokenRangeReplicasResponse tokenRangeReplicas)
    {
        if (tokenRangeReplicas == null)
        {
            return "Null TokenRangeReplicasResponse from sidecar";
        }
        if (tokenRangeReplicas.replicaMetadata() == null)
        {
            return "Null replicaMetadata in TokenRangeReplicasResponse from sidecar";
        }
        if (tokenRangeReplicas.replicaMetadata().isEmpty())
        {
            return "Empty replicaMetadata in TokenRangeReplicasResponse from sidecar";
        }
        for (Map.Entry<String, TokenRangeReplicasResponse.ReplicaMetadata> entry : tokenRangeReplicas.replicaMetadata().entrySet())
        {
            if (entry.getValue().fqdn() == null || entry.getValue().fqdn().trim().isEmpty())
            {
                return String.format(
                "ReplicaMetadata entry '%s' has null or empty fqdn in TokenRangeReplicasResponse from sidecar", entry.getKey());
            }
        }
        if (tokenRangeReplicas.readReplicas() == null)
        {
            return "Null readReplicas in TokenRangeReplicasResponse from sidecar";
        }
        if (tokenRangeReplicas.readReplicas().isEmpty())
        {
            return "Empty readReplicas in TokenRangeReplicasResponse from sidecar";
        }
        return null;
    }
}
