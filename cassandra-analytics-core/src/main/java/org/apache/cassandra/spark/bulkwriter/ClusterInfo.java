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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;

import com.google.common.collect.Range;

import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.validation.StartupValidatable;
import org.jetbrains.annotations.Nullable;

public interface ClusterInfo extends StartupValidatable, Serializable
{
    void refreshClusterInfo();

    TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached);

    String getLowestCassandraVersion();

    /**
     * @return WriteAvailability per RingInstance in the cluster
     */
    Map<RingInstance, WriteAvailability> clusterWriteAvailability();

    Partitioner getPartitioner();

    void checkBulkWriterIsEnabledOrThrow();

    /**
     * Get the time skew information from the replicas of the range
     * @param range token range used to look up the relevant replicas
     * @return {@link TimeSkewResponse} retrieved from Sidecar
     */
    TimeSkewResponse timeSkew(Range<BigInteger> range);

    /**
     * Return the keyspace schema string of the enclosing keyspace for bulk write in the cluster
     * @param cached whether using the cached schema information
     * @return keyspace schema string
     */
    String getKeyspaceSchema(boolean cached);

    /**
     * @return ReplicationFactor of the enclosing keyspace for bulk write in the cluster
     */
    ReplicationFactor replicationFactor();

    CassandraContext getCassandraContext();

    /**
     * ID string that can uniquely identify a cluster
     * <p>
     * Implementor note: the method is optional. When writing to a single cluster, there is no requirement of assigning an ID for bulk write to proceed.
     * When in the coordinated write mode, i.e. writing to multiple clusters, the method must be implemented and return unique string for clusters.
     *
     * @return cluster id string, null if absent
     */
    @Nullable
    default String clusterId()
    {
        return null;
    }

    default void close()
    {
    }
}
