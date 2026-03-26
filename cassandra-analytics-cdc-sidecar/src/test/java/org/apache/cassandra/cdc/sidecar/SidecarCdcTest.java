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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    @Test
    public void testBuilderMethodCreatesValidBuilder()
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
    }

    @Test
    public void testPerInstancePortResolution()
    {
        Map<String, Integer> portMapping = new HashMap<>();
        portMapping.put("host1", 9043);
        portMapping.put("host2", 9044);
        portMapping.put("host3", 9045);
        Function<String, Integer> portResolver = hostname -> portMapping.getOrDefault(hostname, 9043);

        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create();
        SidecarClient mockSidecarClient = mock(SidecarClient.class);
        ICdcStats cdcStats = mock(ICdcStats.class);

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats, portResolver);

        SidecarInstance si1 = client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1"));
        assertThat(si1.port()).isEqualTo(9043);
        assertThat(si1.hostname()).isEqualTo("host1");

        SidecarInstance si2 = client.toSidecarInstance(new CassandraInstance("100", "host2", "DC1"));
        assertThat(si2.port()).isEqualTo(9044);
        assertThat(si2.hostname()).isEqualTo("host2");

        SidecarInstance si3 = client.toSidecarInstance(new CassandraInstance("200", "host3", "DC1"));
        assertThat(si3.port()).isEqualTo(9045);
        assertThat(si3.hostname()).isEqualTo("host3");
    }

    @Test
    public void testFallbackToEffectivePortWhenHostNotFound()
    {
        Map<String, Integer> portMapping = new HashMap<>();
        portMapping.put("host1", 9043);
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(8888, 3, 100L);
        Function<String, Integer> portResolver = hostname -> portMapping.getOrDefault(hostname, clientConfig.effectivePort());

        SidecarClient mockSidecarClient = mock(SidecarClient.class);
        ICdcStats cdcStats = mock(ICdcStats.class);

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats, portResolver);

        SidecarInstance si1 = client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1"));
        assertThat(si1.port()).isEqualTo(9043);

        SidecarInstance si2 = client.toSidecarInstance(new CassandraInstance("100", "unknown-host", "DC1"));
        assertThat(si2.port()).isEqualTo(8888);
    }

    @Test
    public void testDefaultPortResolverUsesEffectivePort()
    {
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(7777, 3, 100L);
        SidecarClient mockSidecarClient = mock(SidecarClient.class);
        ICdcStats cdcStats = mock(ICdcStats.class);

        SidecarCdcClient client = new SidecarCdcClient(clientConfig, mockSidecarClient, cdcStats);

        SidecarInstance si = client.toSidecarInstance(new CassandraInstance("0", "host1", "DC1"));
        assertThat(si.port()).isEqualTo(7777);
    }

    @Test
    public void testBuildPortResolverFromProvider()
    {
        List<CdcSidecarInstance> instances = Arrays.asList(
            cdcSidecarInstance("host1", 9043),
            cdcSidecarInstance("host2", 9044),
            cdcSidecarInstance("host3", 9045)
        );
        CdcSidecarInstancesProvider provider = () -> instances;
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create(5555, 3, 100L);

        Function<String, Integer> resolver = SidecarCdcBuilder.buildPortResolver(provider, clientConfig);

        assertThat(resolver.apply("host1")).isEqualTo(9043);
        assertThat(resolver.apply("host2")).isEqualTo(9044);
        assertThat(resolver.apply("host3")).isEqualTo(9045);
        // Unknown host falls back to clientConfig.effectivePort()
        assertThat(resolver.apply("unknown-host")).isEqualTo(5555);
    }

    private static CdcSidecarInstance cdcSidecarInstance(String hostname, int port)
    {
        return new CdcSidecarInstance()
        {
            @Override
            public int port()
            {
                return port;
            }

            @Override
            public String hostname()
            {
                return hostname;
            }
        };
    }
}
