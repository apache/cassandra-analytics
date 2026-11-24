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

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Range;

import o.a.c.sidecar.client.shaded.common.response.TimeSkewResponse;

/**
 * Variant of {@link CassandraClusterInfo} used when Sidecars are fronted by a load balancer.
 * Replica FQDNs from the token map are not routable from Spark executors in that topology,
 * so requests that would otherwise fan out to per-replica Sidecar addresses are routed through
 * the configured contact points (load balancer endpoints) instead.
 * <p>
 * This applies to both single-cluster and coordinated writes. It is selected by
 * {@link CassandraClusterInfo#create(BulkSparkConf, String)} and
 * {@link CassandraClusterInfo#create(BroadcastableClusterInfo)} when
 * {@link org.apache.cassandra.spark.bulkwriter.WriterOptions#SIDECAR_BEHIND_LOAD_BALANCER}
 * is set.
 */
public class LoadBalancedCassandraClusterInfo extends CassandraClusterInfo
{
    public LoadBalancedCassandraClusterInfo(BulkSparkConf conf, String clusterId)
    {
        super(conf, clusterId);
    }

    public LoadBalancedCassandraClusterInfo(BroadcastableClusterInfo broadcastable)
    {
        super(broadcastable);
    }

    @Override
    protected CompletableFuture<TimeSkewResponse> fetchTimeSkew(Range<BigInteger> range)
    {
        // range is irrelevant; the load balancer contact points are queried directly rather
        // than the per-range replicas, whose FQDNs are not routable from Spark executors.
        return getCassandraContext().getSidecarClient().timeSkew();
    }
}
