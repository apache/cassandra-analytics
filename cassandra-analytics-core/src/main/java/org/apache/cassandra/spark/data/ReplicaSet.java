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

package org.apache.cassandra.spark.data;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

public class ReplicaSet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSet.class);

    private final Set<CassandraInstance> primary;
    private final Set<CassandraInstance> backup;
    private final int minReplicas;
    private final int partitionId;
    CassandraInstance incrementalRepairPrimary;

    ReplicaSet(int minReplicas, int partitionId)
    {
        this.minReplicas = minReplicas;
        this.partitionId = partitionId;
        this.primary = new HashSet<>();
        this.backup = new HashSet<>();
    }

    public ReplicaSet add(CassandraInstance instance)
    {
        if (primary.size() < minReplicas)
        {
            LOGGER.info("Selecting instance as primary replica nodeName={} token={} dataCenter={} partitionId={}",
                        instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
            return addPrimary(instance);
        }
        return addBackup(instance);
    }

    public boolean isRepairPrimary(CassandraInstance instance)
    {
        return incrementalRepairPrimary == null || incrementalRepairPrimary.equals(instance);
    }

    public Set<CassandraInstance> primary()
    {
        return primary;
    }

    public ReplicaSet addPrimary(CassandraInstance instance)
    {
        if (incrementalRepairPrimary == null)
        {
            // Pick the first primary replica as a 'repair primary' to read repaired SSTables at CL ONE
            incrementalRepairPrimary = instance;
        }
        primary.add(instance);
        return this;
    }

    public Set<CassandraInstance> backup()
    {
        return backup;
    }

    public ReplicaSet addBackup(CassandraInstance instance)
    {
        LOGGER.info("Selecting instance as backup replica nodeName={} token={} dataCenter={} partitionId={}",
                    instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
        backup.add(instance);
        return this;
    }
}
