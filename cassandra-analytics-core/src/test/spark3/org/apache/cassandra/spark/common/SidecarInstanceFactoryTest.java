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

package org.apache.cassandra.spark.common;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.SidecarInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SidecarInstanceFactoryTest
{
    @Test
    void testCreateSidecarInstance()
    {
        assertThatThrownBy(() -> SidecarInstanceFactory.createFromString("", 9999))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to create sidecar instance from empty input");

        assertSidecarInstance(SidecarInstanceFactory.createFromString("localhost", 9999),
                              "localhost", 9999);
        assertSidecarInstance(SidecarInstanceFactory.createFromString("[2024:a::1]", 9999),
                              "[2024:a::1]", 9999);
        assertSidecarInstance(SidecarInstanceFactory.createFromString("localhost:8888", 9999),
                              "localhost", 8888);
        assertSidecarInstance(SidecarInstanceFactory.createFromString("127.0.0.1:8888", 9999),
                              "127.0.0.1", 8888);
        assertSidecarInstance(SidecarInstanceFactory.createFromString("[2024:a::1]:8888", 9999),
                              "[2024:a::1]", 8888);
    }

    @Test
    void testCreateSidecarInstanceWithInstanceId()
    {
        assertSidecarInstance(SidecarInstanceFactory.createFromString("localhost:8888=2", 9999),
                              "localhost", 8888, 2);
        assertSidecarInstance(SidecarInstanceFactory.createFromString("127.0.0.1:8888=0", 9999),
                              "127.0.0.1", 8888, 0);
        // no explicit port: default port applies, id still parsed
        assertSidecarInstance(SidecarInstanceFactory.createFromString("localhost=3", 9999),
                              "localhost", 9999, 3);
        // ipv6 with port and id
        assertSidecarInstance(SidecarInstanceFactory.createFromString("[2024:a::1]:8888=7", 9999),
                              "[2024:a::1]", 8888, 7);
        // no id: instanceId is null (falls back to the job-level value)
        assertSidecarInstance(SidecarInstanceFactory.createFromString("localhost:8888", 9999),
                              "localhost", 8888, null);
    }

    @Test
    void testCreateSidecarInstanceWithInvalidInstanceId()
    {
        assertThatThrownBy(() -> SidecarInstanceFactory.createFromString("localhost:8888=abc", 9999))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid sidecar instanceId");

        assertThatThrownBy(() -> SidecarInstanceFactory.createFromString("localhost:8888=-1", 9999))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
    }

    private void assertSidecarInstance(SidecarInstance sidecarInstance, String expectedHostname, int expectedPort)
    {
        assertThat(sidecarInstance.hostname()).isEqualTo(expectedHostname);
        assertThat(sidecarInstance.port()).isEqualTo(expectedPort);
    }

    private void assertSidecarInstance(SidecarInstance sidecarInstance, String expectedHostname, int expectedPort,
                                       Integer expectedInstanceId)
    {
        assertSidecarInstance(sidecarInstance, expectedHostname, expectedPort);
        assertThat(sidecarInstance.instanceId()).isEqualTo(expectedInstanceId);
    }
}
