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

package org.apache.cassandra.spark.common;

import java.util.concurrent.CompletableFuture;

import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import org.apache.cassandra.sidecar.analyticsclient.SidecarClient;
import org.apache.cassandra.spark.data.ClientConfig;
import org.apache.cassandra.spark.data.DefaultSizing;
import org.apache.cassandra.spark.data.DynamicSizing;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SidecarTableSizeProvider;
import org.apache.cassandra.spark.data.Sizing;
import org.apache.cassandra.spark.data.TableSizeProvider;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;

import static org.apache.cassandra.spark.data.ClientConfig.SIZING_DEFAULT;
import static org.apache.cassandra.spark.data.ClientConfig.SIZING_DYNAMIC;

/**
 * A factory class that creates {@link Sizing} based on the client-supplied configuration
 */
public class SizingFactory
{
    /**
     * Private constructor that prevents unnecessary instantiation
     *
     * @throws IllegalStateException when called
     */
    private SizingFactory()
    {
        throw new IllegalStateException(getClass() + " is a static utility class and shall not be instantiated");
    }

    /**
     * Returns the {@link Sizing} object based on the {@code sizing} option provided by the user,
     * or {@link DefaultSizing} as the default sizing
     *
     * @param replicationFactor   the replication factor
     * @param options             the {@link ClientConfig} options
     * @param consistencyLevel    the ConsistencyLevel to use
     * @param keyspace            the keyspace
     * @param table               the table
     * @param datacenter          the DataCenter to use
     * @param sidecarClient       the sidecar client instance to use
     * @param sidecarPort         the port for the sidecar service
     * @param ringFuture          a future representing the result of getting the current ring from the sidecar
     * @return the {@link Sizing} object based on the {@code sizing} option provided by the user
     */
    public static Sizing create(ReplicationFactor replicationFactor,
                                ClientConfig options,
                                ConsistencyLevel consistencyLevel,
                                String keyspace,
                                String table,
                                String datacenter,
                                SidecarClient sidecarClient,
                                int sidecarPort,
                                CompletableFuture<RingResponse> ringFuture)
    {
        if (SIZING_DYNAMIC.equalsIgnoreCase(options.sizing()))
        {
            TableSizeProvider tableSizeProvider = getTableSizeProvider(sidecarClient, sidecarPort, ringFuture);
            return new DynamicSizing(tableSizeProvider, consistencyLevel, replicationFactor,
                                     keyspace, table, datacenter,
                                     options.maxPartitionSize(), options.numCores());
        }
        else if (options.sizing() == null || options.sizing().isEmpty() || SIZING_DEFAULT.equalsIgnoreCase(options.sizing()))
        {
            return new DefaultSizing(options.numCores());
        }
        throw new RuntimeException(String.format("Invalid sizing option provided '%s'", options.sizing()));
    }

    protected static TableSizeProvider getTableSizeProvider(SidecarClient sidecarClient,
                                                            int sidecarPort,
                                                            CompletableFuture<RingResponse> ringFuture)
    {
        return new SidecarTableSizeProvider(sidecarClient, sidecarPort, ringFuture);
    }
}
