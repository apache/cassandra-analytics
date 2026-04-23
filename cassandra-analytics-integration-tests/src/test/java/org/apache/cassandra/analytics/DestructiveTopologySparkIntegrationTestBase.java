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

package org.apache.cassandra.analytics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Spark integration test base for tests that permanently mutate cluster topology (stop/replace/move/leave
 * nodes, kill sidecar instances, etc.). Each {@code @Test} method gets a fresh cluster + sidecar via a
 * {@link Lifecycle#PER_METHOD} lifecycle so tests cannot bleed ring/sidecar state into one another.
 *
 * <p>Non-destructive Spark tests should stay on {@link SharedClusterSparkIntegrationTestBase} to
 * amortize provisioning across the class.
 */
@TestInstance(Lifecycle.PER_METHOD)
public abstract class DestructiveTopologySparkIntegrationTestBase extends SharedClusterSparkIntegrationTestBase
{
    @BeforeEach
    @Override
    protected void setup() throws Exception
    {
        provisionClusterAndSidecar();
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception
    {
        shutdownClusterAndSidecar();
    }

    /**
     * No-op: the cluster is torn down after every method anyway, so there is no shared state to reset.
     */
    @Override
    protected void resetClusterState()
    {
    }
}
