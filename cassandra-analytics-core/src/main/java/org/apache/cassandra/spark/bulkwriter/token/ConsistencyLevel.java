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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ConsistencyLevel
{
    boolean isLocal();

    Logger LOGGER = LoggerFactory.getLogger(ConsistencyLevel.class);

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
     * @param replacementInstances the instances being replaced
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
        };
    }
}
