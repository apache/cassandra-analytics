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

import static org.apache.cassandra.spark.data.CassandraDataLayer.ClientConfig.DEFAULT_SIDECAR_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link Sidecar.ClientConfig} inner class
 */
public class SidecarClientConfigTest
{
    @Test
    public void testDefaults()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of());
        assertEquals(-1, clientConfig.userProvidedPort());
        assertEquals(DEFAULT_SIDECAR_PORT, clientConfig.effectivePort());
        assertEquals(10, clientConfig.maxRetries());
        assertEquals(500, clientConfig.millisToSleep());
        assertEquals(60_000, clientConfig.maxMillisToSleep());
        assertEquals(6L * 1024L * 1024L, clientConfig.maxBufferSize());
        assertEquals(4L * 1024L * 1024L, clientConfig.chunkBufferSize());
        assertEquals(64, clientConfig.maxPoolSize());
        assertEquals(600, clientConfig.timeoutSeconds());
    }

    @Test
    public void testSidecarPort()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("sidecar_port", "9999"));
        assertEquals(9999, clientConfig.userProvidedPort());
        assertEquals(9999, clientConfig.effectivePort());
    }

    @Test
    public void testMaxRetries()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxretries", "5"));
        assertEquals(5, clientConfig.maxRetries());
    }

    @Test
    public void testMillisToSleep()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("defaultmillistosleep", "5000"));
        assertEquals(5000, clientConfig.millisToSleep());
    }

    @Test
    public void testMaxMillisToSleep()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxmillistosleep", "30000"));
        assertEquals(30_000, clientConfig.maxMillisToSleep());
    }

    @Test
    public void testMaxBufferSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxbuffersizebytes", "8"));
        assertEquals(8, clientConfig.maxBufferSize());
    }

    @Test
    public void testChunkBufferSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("chunkbuffersizebytes", "24"));
        assertEquals(24, clientConfig.chunkBufferSize());
    }

    @Test
    public void testMaxPoolSize()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("maxpoolsize", "150"));
        assertEquals(150, clientConfig.maxPoolSize());
    }

    @Test
    public void testTimeoutSeconds()
    {
        Sidecar.ClientConfig clientConfig = Sidecar.ClientConfig.create(ImmutableMap.of("timeoutseconds", "2"));
        assertEquals(2, clientConfig.timeoutSeconds());
    }
}
