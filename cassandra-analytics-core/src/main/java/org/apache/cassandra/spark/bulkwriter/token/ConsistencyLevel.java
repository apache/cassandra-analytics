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

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;

public interface ConsistencyLevel
{
    boolean isLocal();

    boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts, ReplicationFactor replicationFactor, String localDC);

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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                return failedInsts.isEmpty();
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                Preconditions.checkArgument(replicationFactor.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "EACH_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                for (String datacenter : replicationFactor.getOptions().keySet())
                {
                    int rf = replicationFactor.getOptions().get(datacenter);
                    if (failedInsts.stream()
                                   .filter(instance -> instance.getDataCenter().matches(datacenter))
                                   .count() > (rf - (rf / 2 + 1)))
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInsts.size() <= (rf - (rf / 2 + 1));
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                Preconditions.checkArgument(replicationFactor.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "LOCAL_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                int rf = replicationFactor.getOptions().get(localDC);
                return failedInsts.stream().filter(instance -> instance.getDataCenter().matches(localDC)).count() <= (rf - (rf / 2 + 1));
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInsts.size() <= rf - 1;
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInsts.size() <= rf - 2;
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
            public boolean checkConsistency(Collection<? extends CassandraInstance> failedInsts,
                                            ReplicationFactor replicationFactor,
                                            String localDC)
            {
                Preconditions.checkArgument(replicationFactor.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "LOCAL_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                int rf = replicationFactor.getOptions().get(localDC);
                return failedInsts.stream().filter(instance -> instance.getDataCenter().matches(localDC)).count() <= (rf - 1);
            }
        }
    }
}
