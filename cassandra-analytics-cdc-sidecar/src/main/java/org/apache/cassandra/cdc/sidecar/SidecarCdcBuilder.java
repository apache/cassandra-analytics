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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cdc.CdcBuilder;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import javax.annotation.Nonnull;

@SuppressWarnings("unused")
public class SidecarCdcBuilder extends CdcBuilder
{
    protected ClusterConfigProvider clusterConfigProvider;
    protected SidecarCdcClient sidecarCdcClient;
    protected SidecarDownMonitor downMonitor = SidecarDownMonitor.STUB;
    protected ReplicationFactorSupplier replicationFactorSupplier = ReplicationFactorSupplier.DEFAULT;
    protected SidecarCdcStats sidecarCdcStats = SidecarCdcStats.STUB;
    protected SidecarCdcCassandraClient cassandraClient = SidecarCdcCassandraClient.STUB;
    protected SidecarCdcOptions sidecarCdcOptions = SidecarCdcOptions.DEFAULT;

    SidecarCdcBuilder(@Nonnull String jobId,
                      int partitionId,
                      CdcOptions cdcOptions,
                      ClusterConfigProvider clusterConfigProvider,
                      EventConsumer eventConsumer,
                      SchemaSupplier schemaSupplier,
                      TokenRangeSupplier tokenRangeSupplier,
                      SidecarInstancesProvider sidecarInstancesProvider,
                      Sidecar.ClientConfig clientConfig,
                      SecretsProvider secretsProvider,
                      ICdcStats cdcStats) throws IOException
    {
        this(
        jobId,
        partitionId,
        cdcOptions,
        clusterConfigProvider,
        eventConsumer,
        schemaSupplier,
        tokenRangeSupplier,
        clientConfig,
        Sidecar.from(sidecarInstancesProvider, clientConfig, secretsProvider),
        cdcStats
        );
    }

    SidecarCdcBuilder(@Nonnull String jobId,
                      int partitionId,
                      CdcOptions cdcOptions,
                      ClusterConfigProvider clusterConfigProvider,
                      EventConsumer eventConsumer,
                      SchemaSupplier schemaSupplier,
                      TokenRangeSupplier tokenRangeSupplier,
                      Sidecar.ClientConfig clientConfig,
                      SidecarClient sidecarClient,
                      ICdcStats cdcStats)
    {
        super(jobId, partitionId, eventConsumer, schemaSupplier);
        this.clusterConfigProvider = clusterConfigProvider;
        this.sidecarCdcClient = new SidecarCdcClient(clientConfig, sidecarClient, cdcStats);
        withCdcOptions(cdcOptions);
        withTokenRangeSupplier(tokenRangeSupplier);
    }

    public SidecarCdcBuilder withClusterConfigProvider(ClusterConfigProvider clusterConfigProvider)
    {
        this.clusterConfigProvider = clusterConfigProvider;
        return this;
    }

    public SidecarCdcBuilder withDownMonitor(SidecarDownMonitor downMonitor)
    {
        this.downMonitor = downMonitor;
        return this;
    }

    public SidecarCdcBuilder withSidecarClient(Sidecar.ClientConfig clientConfig,
                                               SidecarClient sidecarClient,
                                               ICdcStats cdcStats)
    {
        this.sidecarCdcClient = new SidecarCdcClient(clientConfig, sidecarClient, cdcStats);
        return this;
    }

    public SidecarCdcBuilder withReplicationFactorSupplier(ReplicationFactorSupplier replicationFactorSupplier)
    {
        this.replicationFactorSupplier = replicationFactorSupplier;
        return this;
    }

    @Override
    public CdcBuilder withCdcOptions(@Nonnull CdcOptions cdcOptions)
    {
        super.withCdcOptions(cdcOptions);
        return withSidecarCdcCassandraClient(cassandraClient); // rebuild SidecarStatePersister with new cdcOptions
    }

    public SidecarCdcBuilder withSidecarCdcStats(SidecarCdcStats sidecarCdcStats)
    {
        this.sidecarCdcStats = sidecarCdcStats;
        return withSidecarCdcCassandraClient(cassandraClient); // rebuild SidecarStatePersister with new SidecarCdcStats
    }

    public SidecarCdcBuilder withSidecarCdcOptions(SidecarCdcOptions sidecarCdcOptions)
    {
        this.sidecarCdcOptions = sidecarCdcOptions;
        return withSidecarCdcCassandraClient(cassandraClient); // rebuild SidecarStatePersister with new SidecarCdcOptions
    }

    @Override
    public SidecarCdcBuilder withExecutor(AsyncExecutor asyncExecutor)
    {
        super.withExecutor(asyncExecutor);
        return withSidecarCdcCassandraClient(cassandraClient); // rebuild SidecarStatePersister with new AsyncExecutor
    }

    public SidecarCdcBuilder withSidecarCdcCassandraClient(SidecarCdcCassandraClient cassandraClient)
    {
        this.cassandraClient = cassandraClient;
        return withSidecarStatePersister(new SidecarStatePersister(sidecarCdcOptions, cdcOptions, sidecarCdcStats, cassandraClient, asyncExecutor));
    }

    public SidecarCdcBuilder withSidecarStatePersister(SidecarStatePersister statePersister)
    {
        withStatePersister(statePersister);
        return this;
    }

    @Override
    public SidecarCdc build()
    {
        Preconditions.checkNotNull(clusterConfigProvider, "A ClusterConfigProvider must be supplied");
        Preconditions.checkNotNull(asyncExecutor, "An AsyncExecutor must be supplied");
        Preconditions.checkNotNull(eventConsumer, "An event consumer supplier must be supplied");
        Preconditions.checkNotNull(schemaSupplier, "An schema supplier must be supplied");

        if (this.commitLogProvider == null)
        {
            // SidecarCdc should generally use SidecarCommitLogProvider to list and stream commit log segments over Sidecar http API
            // but the builder is left open if user wishes to provide own implementation
            this.commitLogProvider = new SidecarCommitLogProvider(clusterConfigProvider, sidecarCdcClient, downMonitor, replicationFactorSupplier);
        }

        return new SidecarCdc(this);
    }
}
