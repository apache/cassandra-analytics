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

package org.apache.cassandra.clients;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.DataTransport;

import static org.apache.cassandra.spark.bulkwriter.BulkSparkConf.DEFAULT_SIDECAR_PORT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link Sidecar.ClientConfig} inner class
 */
public class SidecarClientConfigTest
{
    @Test
    public void testDefaults()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of());
        assertThat(clientConfig.userProvidedPort()).isEqualTo(-1);
        assertThat(clientConfig.effectivePort()).isEqualTo(DEFAULT_SIDECAR_PORT);
        assertThat(clientConfig.maxRetries()).isEqualTo(10);
        assertThat(clientConfig.millisToSleep()).isEqualTo(500);
        assertThat(clientConfig.maxMillisToSleep()).isEqualTo(60_000);
        assertThat(clientConfig.maxBufferSize()).isEqualTo(6L * 1024L * 1024L);
        assertThat(clientConfig.chunkBufferSize()).isEqualTo(4L * 1024L * 1024L);
        assertThat(clientConfig.maxPoolSize()).isEqualTo(64);
        assertThat(clientConfig.timeoutSeconds()).isEqualTo(600);
    }

    @Test
    public void testSidecarPort()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("sidecar_port", "9999"));
        assertThat(clientConfig.userProvidedPort()).isEqualTo(9999);
        assertThat(clientConfig.effectivePort()).isEqualTo(9999);
    }

    @Test
    public void testMaxRetries()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxretries", "5"));
        assertThat(clientConfig.maxRetries()).isEqualTo(5);
    }

    @Test
    public void testMillisToSleep()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("defaultmillistosleep", "5000"));
        assertThat(clientConfig.millisToSleep()).isEqualTo(5000);
    }

    @Test
    public void testMaxMillisToSleep()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxmillistosleep", "30000"));
        assertThat(clientConfig.maxMillisToSleep()).isEqualTo(30_000);
    }

    @Test
    public void testMaxBufferSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxbuffersizebytes", "8"));
        assertThat(clientConfig.maxBufferSize()).isEqualTo(8);
    }

    @Test
    public void testChunkBufferSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("chunkbuffersizebytes", "24"));
        assertThat(clientConfig.chunkBufferSize()).isEqualTo(24);
    }

    @Test
    public void testMaxPoolSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxpoolsize", "150"));
        assertThat(clientConfig.maxPoolSize()).isEqualTo(150);
    }

    @Test
    public void testTimeoutSeconds()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("timeoutseconds", "2"));
        assertThat(clientConfig.timeoutSeconds()).isEqualTo(2);
    }

    @Test
    public void testTransportModeBasedWriterUserAgent()
    {
        String userAgentStr = AnalyticsSidecarClient.transportModeBasedWriterUserAgent(DataTransport.DIRECT);
        assertThat(userAgentStr.endsWith(" writer")).isTrue();

        userAgentStr = AnalyticsSidecarClient.transportModeBasedWriterUserAgent(DataTransport.S3_COMPAT);
        assertThat(userAgentStr.endsWith(" writer-s3")).isTrue();
    }
}
