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
