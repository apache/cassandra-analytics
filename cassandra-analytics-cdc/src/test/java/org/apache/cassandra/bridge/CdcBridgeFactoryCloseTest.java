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

package org.apache.cassandra.bridge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link CdcBridgeFactory#close()} releases cached bridges.
 * Relies on {@code forkEvery = 1} so each test class gets a fresh JVM with clean static state.
 */
public class CdcBridgeFactoryCloseTest
{
    @AfterEach
    public void tearDown()
    {
        CdcBridgeFactory.close();
    }

    @Test
    public void testCloseReleasesBridges()
    {
        assertThat(CdcBridgeFactory.areBridgesClosed()).isTrue();

        // Loading a bridge populates the cache
        CassandraVersion version = CassandraVersion.implementedVersions()[0];
        CdcBridgeFactory.get(version);
        assertThat(CdcBridgeFactory.areBridgesClosed()).isFalse();

        // close() clears the cache and closes classloaders
        CdcBridgeFactory.close();
        assertThat(CdcBridgeFactory.areBridgesClosed()).isTrue();
    }
}
