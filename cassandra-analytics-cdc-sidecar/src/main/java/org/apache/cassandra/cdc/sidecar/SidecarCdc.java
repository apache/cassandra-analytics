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

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.cdc.Cdc;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;

/**
 * SidecarCdc implementation that uses the Sidecar HTTP APIs to list and stream commit log segments.
 */
public class SidecarCdc extends Cdc
{
    protected final ClusterConfigProvider clusterConfigProvider;

    protected SidecarCdc(@NotNull SidecarCdcBuilder builder)
    {
        super(builder);
        this.clusterConfigProvider = builder.clusterConfigProvider;
        initSchema();
    }

    /**
     * Creates a new {@link SidecarCdcBuilder} pre-configured with the supplied parameters.
     *
     * <p><b>Lifecycle of {@code sidecarCdcClient}:</b> the supplied {@link SidecarCdcClient} is treated as
     * an externally managed singleton. Neither the returned builder nor the {@link SidecarCdc} instance it
     * produces will close the client. The caller is solely responsible for closing the
     * {@code SidecarCdcClient} (e.g. during application shutdown) to release underlying resources such as
     * thread pools and HTTP connections.
     *
     * @param jobId                  unique identifier for the CDC job
     * @param partitionId            partition index within the job
     * @param cdcOptions             CDC processing options
     * @param clusterConfigProvider  provider for cluster configuration (e.g. datacenter, hosts)
     * @param eventConsumer          consumer that receives CDC change events
     * @param schemaSupplier         supplier for all table schemas (CDC-enabled and disabled); see {@link SchemaSupplier}
     * @param tokenRangeSupplier     supplier for the token ranges assigned to this partition
     * @param sidecarCdcClient       externally managed Sidecar HTTP client; <em>not</em> closed by
     *                               {@code SidecarCdc} or {@code SidecarCdcBuilder}
     * @param cdcStats               CDC statistics collector
     * @return a new {@link SidecarCdcBuilder}
     */
    public static SidecarCdcBuilder builder(@NotNull String jobId,
                                            int partitionId,
                                            CdcOptions cdcOptions,
                                            ClusterConfigProvider clusterConfigProvider,
                                            EventConsumer eventConsumer,
                                            SchemaSupplier schemaSupplier,
                                            TokenRangeSupplier tokenRangeSupplier,
                                            SidecarCdcClient sidecarCdcClient,
                                            ICdcStats cdcStats)
    {
        return new SidecarCdcBuilder(jobId,
                                     partitionId,
                                     cdcOptions,
                                     clusterConfigProvider,
                                     eventConsumer,
                                     schemaSupplier,
                                     tokenRangeSupplier,
                                     sidecarCdcClient,
                                     cdcStats);
    }

    public void initSchema()
    {
        Set<CqlTable> allTables = FutureUtils.get(schemaSupplier.getTables());
        Optional<ReplicationFactor> rfOp = allTables.stream()
                                                    .filter(CqlTable::cdc)
                                                    .map(CqlTable::replicationFactor)
                                                    .filter(rf -> rf.getOptions().containsKey(dc()))
                                                    .max(Comparator.comparingInt(rf -> rf.getOptions().get(dc())));

        if (!rfOp.isPresent())
        {
            throw new RuntimeException("Could not find replication factor for any keyspace");
        }
    }

    public String dc()
    {
        return clusterConfigProvider.dc();
    }
}
