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

import java.util.Collection;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;

public interface ConsistencyLevel
{
    /**
     * Whether the consistency level only considers replicas in the local data center.
     *
     * @return true if only considering the local replicas; otherwise, return false
     */
    boolean isLocal();

    /**
     * Check write consistency can be satisfied with the collection of the succeeded instances
     * <p>
     * When pendingReplicas is non-empty, the minimum number of success is increased by the size of pendingReplicas,
     * keeping the same semantics defined in Cassandra.
     * See <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/ConsistencyLevel.java#L172">blockForWrite</a>
     * <p>
     * For example, say RF == 3, and there is 2 pending replicas.
     * <ul>
     *     <li>QUORUM write consistency requires at least 4 replicas to succeed, i.e. quorum(3) + 2, tolerating 1 failure</li>
     *     <li>ONE write consistency requires at least 3 replicas to succeed, i.e. 1 + 2, tolerating 2 failure</li>
     *     <li>TWO write consistency requires at least 4 replicas to succeed, i.e. 2 + 2, tolerating 1 failure</li>
     * </ul>
     *
     * @param succeededInstances the succeeded instances in the replica set
     * @param pendingInstances the pending instances, i.e. JOINING, LEAVING, MOVING
     * @param replicationFactor replication factor to check with
     * @param localDC the local data center name if required for the check
     * @return true means the consistency level is _definitively_ satisfied.
     *         Meanwhile, returning false means no conclusion can be drawn
     */
    boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                           Collection<? extends CassandraInstance> pendingInstances,
                           ReplicationFactor replicationFactor,
                           String localDC);

    default void ensureNetworkTopologyStrategy(ReplicationFactor replicationFactor, CL cl)
    {
        Preconditions.checkArgument(replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                    cl.name() + " only make sense for NetworkTopologyStrategy keyspaces");
    }

    enum CL implements ConsistencyLevel
    {
        ALL
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int blockedFor = replicationFactor.getTotalReplicationFactor() + pendingInstances.size();
                return succeededInstances.size() >= blockedFor;
            }
        },

        EACH_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC) // localDc is ignored for EACH_QUORUM
            {
                ensureNetworkTopologyStrategy(replicationFactor, EACH_QUORUM);

                for (String datacenter : replicationFactor.getOptions().keySet())
                {
                    int rf = replicationFactor.getOptions().get(datacenter);
                    int pendingCountOfDc = countInDc(pendingInstances, datacenter);
                    int succeededCountOfDc = countInDc(succeededInstances, datacenter);
                    int blockedFor = quorum(rf) + pendingCountOfDc;
                    if (succeededCountOfDc < blockedFor)
                    {
                        return false;
                    }
                }
                return true;
            }
        },
        QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                int blockedFor = quorum(rf) + pendingInstances.size();
                return succeededInstances.size() >= blockedFor;
            }
        },
        LOCAL_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int rf = replicationFactor.getOptions().get(localDC);
                int pendingCountInDc = countInDc(pendingInstances, localDC);
                int succeededCountInDc = countInDc(succeededInstances, localDC);
                int blockFor = quorum(rf) + pendingCountInDc;
                return succeededCountInDc >= blockFor;
            }
        },
        ONE
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int blockFor = 1 + pendingInstances.size();
                return succeededInstances.size() >= blockFor;
            }
        },
        TWO
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int blockFor = 2 + pendingInstances.size();
                return succeededInstances.size() >= blockFor;
            }
        },
        LOCAL_ONE
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          Collection<? extends CassandraInstance> pendingInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_ONE);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int pendingCountInDc = countInDc(pendingInstances, localDC);
                int succeededCountInDc = countInDc(succeededInstances, localDC);
                int blockFor = pendingCountInDc + 1;
                return succeededCountInDc >= blockFor;
            }
        };

        private static int quorum(int replicaSetSize)
        {
            return replicaSetSize / 2 + 1;
        }

        private static int countInDc(Collection<? extends CassandraInstance> instances, String localDC)
        {
            return (int) instances.stream().filter(i -> i.datacenter().equalsIgnoreCase(localDC)).count();
        }
    }
}
