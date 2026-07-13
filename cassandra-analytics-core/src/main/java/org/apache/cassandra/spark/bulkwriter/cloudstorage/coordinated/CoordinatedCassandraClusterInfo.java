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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Range;

import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.BroadcastableClusterInfo;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.CassandraClusterInfo;

/**
 * Variant of {@link CassandraClusterInfo} used when Sidecars are fronted by a load balancer.
 * Replica FQDNs from the token map are not routable from Spark executors in that topology,
 * so time-skew validation must query the configured contact points (load balancer endpoints)
 * rather than per-range replicas. Selected when
 * {@link org.apache.cassandra.spark.bulkwriter.WriterOptions#SIDECAR_BEHIND_LOAD_BALANCER}
 * is set.
 */
public class CoordinatedCassandraClusterInfo extends CassandraClusterInfo
{
    public CoordinatedCassandraClusterInfo(BulkSparkConf conf, String clusterId)
    {
        super(conf, clusterId);
    }

    public CoordinatedCassandraClusterInfo(BroadcastableClusterInfo broadcastable)
    {
        super(broadcastable);
    }

    @Override
    protected CompletableFuture<TimeSkewResponse> fetchTimeSkew(Range<BigInteger> range)
    {
        return getCassandraContext().getSidecarClient().timeSkew();
    }
}
