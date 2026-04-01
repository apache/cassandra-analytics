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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for SidecarCdc class
 */
public class SidecarCdcTest
{
    private SidecarClient mockSidecarClient;
    private ICdcStats cdcStats;

    @BeforeEach
    public void setup()
    {
        String jobId = "test-job-123";
        int partitionId = 0;
        CdcOptions cdcOptions = mock(CdcOptions.class);
        ClusterConfigProvider clusterConfigProvider = mock(ClusterConfigProvider.class);
        EventConsumer eventConsumer = mock(EventConsumer.class);
        SchemaSupplier schemaSupplier = mock(SchemaSupplier.class);
        TokenRangeSupplier tokenRangeSupplier = mock(TokenRangeSupplier.class);
        SidecarCdcClient mockSidecarCdcClient = mock(SidecarCdcClient.class);
        ICdcStats cdcStats = mock(ICdcStats.class);

        SidecarCdcBuilder builder = new SidecarCdcBuilder(
            jobId,
            partitionId,
            cdcOptions,
            clusterConfigProvider,
            eventConsumer,
            schemaSupplier,
            tokenRangeSupplier,
            mockSidecarCdcClient,
            cdcStats
        );

        assertThat(builder).isNotNull();
        assertThat(builder).isInstanceOf(SidecarCdcBuilder.class);
        assertThat(builder.clusterConfigProvider).isEqualTo(clusterConfigProvider);
        assertThat(builder.sidecarCdcClient).isEqualTo(mockSidecarCdcClient);
        mockSidecarClient = mock(SidecarClient.class);
        cdcStats = mock(ICdcStats.class);
    }


    @Test
    public void testPerInstancePortResolution()
    {
        Map<String, Integer> portMapping = Map.of("host1", 9043, "host2", 9044, "host3", 9045);
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create();

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats,
                                                       instance -> portMapping.getOrDefault(instance.nodeName(), 9043));

        SidecarInstance si1 = client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1"));
        assertThat(si1.hostname()).isEqualTo("host1");
        assertThat(si1.port()).isEqualTo(9043);

        SidecarInstance si2 = client.toSidecarInstance(new CassandraInstance("100", "host2", "DC1"));
        assertThat(si2.hostname()).isEqualTo("host2");
        assertThat(si2.port()).isEqualTo(9044);

        SidecarInstance si3 = client.toSidecarInstance(new CassandraInstance("200", "host3", "DC1"));
        assertThat(si3.hostname()).isEqualTo("host3");
        assertThat(si3.port()).isEqualTo(9045);
    }

    @Test
    public void testFallbackToEffectivePortWhenHostNotFound()
    {
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(8888, 3, 100L);
        Map<String, Integer> portMapping = Map.of("host1", 9043);

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats,
                                                       instance -> portMapping.getOrDefault(instance.nodeName(),
                                                                                            clientConfig.effectivePort()));

        assertThat(client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1")).port()).isEqualTo(9043);
        assertThat(client.toSidecarInstance(new CassandraInstance("100", "unknown-host", "DC1")).port()).isEqualTo(8888);
    }

    @Test
    public void testDefaultPortResolverUsesEffectivePort()
    {
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(7777, 3, 100L);

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats);

        assertThat(client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1")).port()).isEqualTo(7777);
    }

    @Test
    public void testWithPortResolverOverridesDefault()
    {
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(9042, 3, 100L);

        SidecarCdcBuilder builder = new SidecarCdcBuilder(
            "test-job",
            0,
            mock(CdcOptions.class),
            mock(ClusterConfigProvider.class),
            mock(EventConsumer.class),
            mock(SchemaSupplier.class),
            mock(TokenRangeSupplier.class),
            null,  // use default portResolver: ignored -> clientConfig.effectivePort()
            clientConfig,
            mockSidecarClient,
            cdcStats
        );

        // Before override: default resolver returns effectivePort for every host
        assertThat(builder.sidecarCdcClient.toSidecarInstance(new CassandraInstance("0", "host1", "DC1")).port())
            .isEqualTo(9042);

        // Apply custom resolver then rebuild the client so it picks up the new resolver
        Map<String, Integer> portMapping = Map.of("host1", 9100, "host2", 9200);
        builder.withPortResolver(instance -> portMapping.getOrDefault(instance.nodeName(), clientConfig.effectivePort()))
               .withSidecarClient(clientConfig, mockSidecarClient, cdcStats);

        // Known hosts now resolve to their mapped ports
        assertThat(builder.sidecarCdcClient.toSidecarInstance(new CassandraInstance("0", "host1", "DC1")).port())
            .isEqualTo(9100);
        assertThat(builder.sidecarCdcClient.toSidecarInstance(new CassandraInstance("100", "host2", "DC1")).port())
            .isEqualTo(9200);

        // Unknown host still falls back to effectivePort
        assertThat(builder.sidecarCdcClient.toSidecarInstance(new CassandraInstance("200", "unknown-host", "DC1")).port())
            .isEqualTo(9042);
    }
}
