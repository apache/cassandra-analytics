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
import java.util.Set;

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
     * Check consistency level with the collection of the succeeded instances
     *
     * @param succeededInstances the succeeded instances in the replica set
     * @param replicationFactor replication factor to check with
     * @param localDC the local data center name if required for the check
     * @return true means the consistency level is _definitively_ satisfied.
     *         Meanwhile, returning false means no conclusion can be drawn
     */
    boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                           ReplicationFactor replicationFactor,
                           String localDC);

    default void ensureNetworkTopologyStrategy(ReplicationFactor replicationFactor, CL cl)
    {
        Preconditions.checkArgument(replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                    cl.name() + " only make sense for NetworkTopologyStrategy keyspaces");
    }

    /**
     * Checks if the consistency guarantees are maintained, given the failed, blocked and replacing instances, consistency-level and the replication-factor.
     * <pre>
     * - QUORUM based consistency levels check for quorum using the write-replica-set (instead of RF) as they include healthy and pending nodes.
     *   This is done to ensure that writes go to a quorum of healthy nodes while accounting for potential failure in pending nodes becoming healthy.
     * - ONE and TWO consistency guarantees are maintained by ensuring that the failures leave us with at-least the corresponding healthy
     *   (and non-pending) nodes.
     *
     *   For both the above cases, blocked instances are also considered as failures while performing consistency checks.
     *   Write replicas are adjusted to exclude replacement nodes for consistency checks, if we have replacement nodes that are not among the failed instances.
     *   This is to ensure that we are writing to sufficient non-replacement nodes as replacements can potentially fail leaving us with fewer nodes.
     * </pre>
     *
     * @param writeReplicas        the set of replicas for write operations
     * @param pendingReplicas      the set of replicas pending status
     * @param replacementInstances the set of instances that are replacing the other instances
     * @param blockedInstances     the set of instances that have been blocked for the bulk operation
     * @param failedInstanceIps    the set of instances where there were failures
     * @param localDC              the local datacenter used for consistency level, or {@code null} if not provided
     * @return {@code true} if the consistency has been met, {@code false} otherwise
     */
    boolean checkConsistency(Set<String> writeReplicas,
                             Set<String> pendingReplicas,
                             Set<String> replacementInstances,
                             Set<String> blockedInstances,
                             Set<String> failedInstanceIps,
                             String localDC);

    // Check if successful writes forms quorum of non-replacing nodes - N/A as quorum is if there are no failures/blocked
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                int failedExcludingReplacements = failedInstanceIps.size() - replacementInstances.size();
                return failedExcludingReplacements <= 0 && blockedInstances.isEmpty();
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                // The effective RF during expansion could be larger than the defined RF
                // The check for CL satisfaction should consider the scenario and use >=
                return succeededInstances.size() >= rf;
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size()) <= (writeReplicas.size() - (writeReplicas.size() / 2 + 1));
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, EACH_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                for (String datacenter : replicationFactor.getOptions().keySet())
                {
                    int rf = replicationFactor.getOptions().get(datacenter);
                    int majority = rf / 2 + 1;
                    if (succeededInstances.stream()
                                          .filter(instance -> instance.datacenter().equalsIgnoreCase(datacenter))
                                          .count() < majority)
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size()) <= (writeReplicas.size() - (writeReplicas.size() / 2 + 1));
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return succeededInstances.size() > rf / 2;
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size()) <= (writeReplicas.size() - (writeReplicas.size() / 2 + 1));
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int rf = replicationFactor.getOptions().get(localDC);
                return succeededInstances.stream()
                                         .filter(instance -> instance.datacenter().equalsIgnoreCase(localDC))
                                         .count() > rf / 2;
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size())
                       <= (writeReplicas.size() - pendingReplicas.size() - 1);
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                return !succeededInstances.isEmpty();
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size())
                       <= (writeReplicas.size() - pendingReplicas.size() - 2);
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                return succeededInstances.size() >= 2;
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
            public boolean checkConsistency(Set<String> writeReplicas,
                                            Set<String> pendingReplicas,
                                            Set<String> replacementInstances,
                                            Set<String> blockedInstances,
                                            Set<String> failedInstanceIps,
                                            String localDC)
            {
                return (failedInstanceIps.size() + blockedInstances.size()) <= (writeReplicas.size() - pendingReplicas.size() - 1);
            }

            @Override
            public boolean canBeSatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                          ReplicationFactor replicationFactor,
                                          String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_ONE);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                return succeededInstances.stream().anyMatch(instance -> instance.datacenter().equalsIgnoreCase(localDC));
            }
        };
    }
}
