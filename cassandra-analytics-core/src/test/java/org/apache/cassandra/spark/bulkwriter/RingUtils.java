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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

public final class RingUtils
{
    private RingUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @NotNull
    static CassandraRing<RingInstance> buildRing(int initialToken,
                                                 String app,
                                                 String cluster,
                                                 String dataCenter,
                                                 String keyspace)
    {
        return buildRing(initialToken, app, cluster, dataCenter, keyspace, 3);
    }

    @NotNull
    public static CassandraRing<RingInstance> buildRing(int initialToken,
                                                        String app,
                                                        String cluster,
                                                        String dataCenter,
                                                        String keyspace,
                                                        int instancesPerDC)
    {
        ImmutableMap<String, Integer> rfByDC = ImmutableMap.of(dataCenter, 3);
        return buildRing(initialToken, app, cluster, rfByDC, keyspace, instancesPerDC);
    }

    @NotNull
    static CassandraRing<RingInstance> buildRing(int initialToken,
                                                 String app,
                                                 String cluster,
                                                 ImmutableMap<String, Integer> rfByDC,
                                                 String keyspace,
                                                 int instancesPerDC)
    {
        List<RingInstance> instances = getInstances(initialToken, rfByDC, instancesPerDC);
        ReplicationFactor replicationFactor = getReplicationFactor(rfByDC);
        return new CassandraRing<>(Partitioner.Murmur3Partitioner, keyspace, replicationFactor, instances);
    }

    @NotNull
    private static ReplicationFactor getReplicationFactor(Map<String, Integer> rfByDC)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder()
                .put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        rfByDC.forEach((key, value) -> optionsBuilder.put(key, value.toString()));
        return new ReplicationFactor(optionsBuilder.build());
    }

    @NotNull
    public static List<RingInstance> getInstances(int initialToken, Map<String, Integer> rfByDC, int instancesPerDc)
    {
        ArrayList<RingInstance> instances = new ArrayList<>();
        int dcOffset = 0;
        for (Map.Entry<String, Integer> rfForDC : rfByDC.entrySet())
        {
            String datacenter = rfForDC.getKey();
            for (int instance = 0; instance < instancesPerDc; instance++)
            {
                instances.add(new RingInstance(new RingEntry.Builder()
                        .address("127.0." + dcOffset + "." + instance)
                        .datacenter(datacenter)
                        .load("0")
                        .token(Integer.toString(initialToken + dcOffset + 100_000 * instance))
                        .fqdn(datacenter + "-i" + instance)
                        .rack("Rack")
                        .hostId("")
                        .status("UP")
                        .state("NORMAL")
                        .owns("")
                        .build()));
            }
            dcOffset++;
        }
        return instances;
    }
}
