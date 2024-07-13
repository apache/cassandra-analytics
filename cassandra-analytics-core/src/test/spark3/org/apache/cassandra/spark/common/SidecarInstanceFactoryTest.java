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

import org.apache.cassandra.sidecar.client.SidecarInstance;

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

    private void assertSidecarInstance(SidecarInstance sidecarInstance, String expectedHostname, int expectedPort)
    {
        assertThat(sidecarInstance.hostname()).isEqualTo(expectedHostname);
        assertThat(sidecarInstance.port()).isEqualTo(expectedPort);
    }
}
