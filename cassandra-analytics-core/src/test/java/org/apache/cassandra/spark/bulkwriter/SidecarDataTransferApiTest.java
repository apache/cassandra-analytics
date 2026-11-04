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

package org.apache.cassandra.spark.bulkwriter;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.bridge.CassandraBridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SidecarDataTransferApi}
 */
class SidecarDataTransferApiTest
{
    @Test
    void testToSidecarInstanceCarriesPerInstanceId()
    {
        SidecarDataTransferApi api = api();
        RingInstance instance = ringInstance("dc1-i0", 2);

        SidecarInstanceImpl sidecarInstance = api.toSidecarInstance(instance);

        assertThat(sidecarInstance.hostname()).isEqualTo("dc1-i0");
        assertThat(sidecarInstance.port()).isEqualTo(9043);
        assertThat(sidecarInstance.instanceId())
        .describedAs("upload/commit/cleanup requests must carry the target instance's own id, "
                    + "not a single job-wide value, or a multi-node write silently misroutes")
        .isEqualTo(2);
    }

    @Test
    void testToSidecarInstanceFallsBackToNullWhenNoPerInstanceIdConfigured()
    {
        SidecarDataTransferApi api = api();
        RingInstance instance = ringInstance("dc1-i1", null);

        SidecarInstanceImpl sidecarInstance = api.toSidecarInstance(instance);

        assertThat(sidecarInstance.instanceId()).isNull();
    }

    private static SidecarDataTransferApi api()
    {
        CassandraContext context = mock(CassandraContext.class, RETURNS_DEEP_STUBS);
        when(context.sidecarPort()).thenReturn(9043);
        return new SidecarDataTransferApi(context, mock(CassandraBridge.class), mock(JobInfo.class));
    }

    private static RingInstance ringInstance(String fqdn, Integer sidecarInstanceId)
    {
        return new RingInstance(new RingEntry.Builder()
                                .datacenter("dc1")
                                .address(fqdn)
                                .port(7000)
                                .status("UP")
                                .state("NORMAL")
                                .token("0")
                                .fqdn(fqdn)
                                .rack("rack")
                                .owns("")
                                .load("")
                                .hostId("")
                                .build(), null, sidecarInstanceId);
    }
}
