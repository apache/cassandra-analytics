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
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class SidecarCdcBuilder extends CdcBuilder
{
    protected ClusterConfigProvider clusterConfigProvider;
    protected SidecarCdcClient sidecarCdcClient;
    protected SidecarDownMonitor downMonitor = SidecarDownMonitor.STUB;
    protected ReplicationFactorSupplier replicationFactorSupplier = ReplicationFactorSupplier.DEFAULT;

    SidecarCdcBuilder(@NotNull String jobId,
                      int partitionId,
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
        clusterConfigProvider,
        eventConsumer,
        schemaSupplier,
        tokenRangeSupplier,
        clientConfig,
        Sidecar.from(sidecarInstancesProvider, clientConfig, secretsProvider),
        cdcStats
        );
    }

    SidecarCdcBuilder(@NotNull String jobId,
                      int partitionId,
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
        withTokenRangeSupplier(tokenRangeSupplier);
        rebuildCommitLogProvider();
    }

    public SidecarCdcBuilder withClusterConfigProvider(ClusterConfigProvider clusterConfigProvider)
    {
        this.clusterConfigProvider = clusterConfigProvider;
        rebuildCommitLogProvider();
        return this;
    }

    public SidecarCdcBuilder withDownMonitor(SidecarDownMonitor downMonitor)
    {
        this.downMonitor = downMonitor;
        rebuildCommitLogProvider();
        return this;
    }

    public SidecarCdcBuilder withSidecarClient(Sidecar.ClientConfig clientConfig,
                                               SidecarClient sidecarClient,
                                               ICdcStats cdcStats)
    {
        this.sidecarCdcClient = new SidecarCdcClient(clientConfig, sidecarClient, cdcStats);
        rebuildCommitLogProvider();
        return this;
    }

    protected void rebuildCommitLogProvider()
    {
        this.commitLogProvider = new SidecarCommitLogProvider(clusterConfigProvider, sidecarCdcClient, downMonitor, replicationFactorSupplier);
    }

    protected SidecarCdcBuilder withReplicationFactorSupplier(ReplicationFactorSupplier replicationFactorSupplier)
    {
        this.replicationFactorSupplier = replicationFactorSupplier;
        rebuildCommitLogProvider();
        return this;
    }

    @Override
    public SidecarCdc build()
    {
        Preconditions.checkNotNull(clusterConfigProvider, "A ClusterConfigProvider must be supplied");
        Preconditions.checkNotNull(commitLogProvider, "A CommitLogProvider must be supplied");
        Preconditions.checkNotNull(asyncExecutor, "An AsyncExecutor must be supplied");
        Preconditions.checkNotNull(eventConsumer, "An event consumer supplier must be supplied");
        Preconditions.checkNotNull(schemaSupplier, "An schema supplier must be supplied");
        return new SidecarCdc(this);
    }
}
