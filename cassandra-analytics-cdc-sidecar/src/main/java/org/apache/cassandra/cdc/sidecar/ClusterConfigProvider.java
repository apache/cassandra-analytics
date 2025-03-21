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

package org.apache.cassandra.cdc.sidecar;

import java.util.Set;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * Interface to supply cluster topology information for CDC:
 * the Cassandra partitioner,
 * the datacenter where CDC is enabled,
 * CassandraInstance objects describing the cluster topology.
 */
public interface ClusterConfigProvider
{
    /**
     * @return the datacenter name where CDC is enabled.
     */
    String dc();

    /**
     * @return set of CassandraInstance objects describing the entire ring topology, including other datacenters.
     */
    Set<CassandraInstance> getCluster();

    /**
     * @return the Cassandra partitioner configured on the cluster.
     */
    Partitioner partitioner();
}
