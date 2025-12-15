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

package org.apache.cassandra.spark.data.partitioner;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.ReplicationFactor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum ConsistencyLevel
{
    ANY(0),
    ONE(1),
    TWO(2),
    THREE(3),
    QUORUM(4),
    ALL(5),
    LOCAL_QUORUM(6, true),
    EACH_QUORUM(7),
    SERIAL(8),
    LOCAL_SERIAL(9),
    LOCAL_ONE(10, true);

    public final int code;
    public final boolean isDCLocal;

    ConsistencyLevel(int code)
    {
        this(code, false);
    }

    ConsistencyLevel(int code, boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    private int quorumFor(ReplicationFactor replicationFactor)
    {
        return (replicationFactor.getTotalReplicationFactor() / 2) + 1;
    }

    private int localQuorumFor(@NotNull ReplicationFactor replicationFactor, @Nullable String dataCenter)
    {
        return replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy
               ? getNetworkTopologyRf(replicationFactor, dataCenter)
               : quorumFor(replicationFactor);
    }

    /**
     * Calculates the number of replicas that must acknowledge the operation in each data center
     * for EACH_QUORUM consistency level.
     *
     * @param replicationFactor the replication factor configuration containing data center information
     * @return a map where keys are data center names and values are the number of replicas
     * required to achieve local quorum in each data center
     * @throws IllegalArgumentException if the consistency level is not EACH_QUORUM or if the
     *                                  replication strategy is not NetworkTopologyStrategy
     */
    public Map<String, Integer> eachQuorumBlockFor(@NotNull ReplicationFactor replicationFactor)
    {
        if (this != EACH_QUORUM)
        {
            throw new IllegalArgumentException(String.format("Consistency level needed is EACH_QUORUM, provided is: %s",
                                                             this.name()));
        }
        if (replicationFactor.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            throw new IllegalArgumentException(String.format("Invalid Replication Strategy: %s for EACH_QUORUM consistency read.",
                                                             replicationFactor.getReplicationStrategy().name()));
        }
        return replicationFactor.getOptions().keySet().stream()
                                .collect(Collectors.toMap(
                                dataCenter -> dataCenter,
                                dataCenter -> localQuorumFor(replicationFactor, dataCenter)
                                ));
    }

    private int getNetworkTopologyRf(@NotNull ReplicationFactor replicationFactor, @Nullable String dataCenter)
    {
        int rf;
        // Single data center and not specified, so return the only data center replication factor
        if (dataCenter == null && replicationFactor.getOptions().size() == 1)
        {
            rf = replicationFactor.getOptions().values().iterator().next();
        }
        else
        {
            if (!replicationFactor.getOptions().containsKey(dataCenter))
            {
                throw new IllegalArgumentException(String.format("Data center %s not found in replication factor %s",
                                                                 dataCenter, replicationFactor.getOptions().keySet()));
            }
            rf = replicationFactor.getOptions().get(dataCenter);
        }
        return (rf / 2) + 1;
    }

    public int blockFor(@NotNull ReplicationFactor replicationFactor, @Nullable String dataCenter)
    {
        switch (this)
        {
            case ONE:
            case LOCAL_ONE:
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case SERIAL:
                return quorumFor(replicationFactor);
            case ALL:
                return replicationFactor.getTotalReplicationFactor();
            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                return localQuorumFor(replicationFactor, dataCenter);
            case EACH_QUORUM:
                if (replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
                {
                    int count = 0;
                    for (String datacenter : replicationFactor.getOptions().keySet())
                    {
                        count += localQuorumFor(replicationFactor, datacenter);
                    }
                    return count;
                }
                else
                {
                    return quorumFor(replicationFactor);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }
}
