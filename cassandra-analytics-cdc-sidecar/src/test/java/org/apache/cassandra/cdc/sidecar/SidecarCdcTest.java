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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.ICdcStats;

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
}
