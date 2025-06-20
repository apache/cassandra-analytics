/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.AUTH_ROLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link VertxHttpClient}
 */
public class VertxHttpClientTest
{
    private static Vertx vertx;

    @BeforeAll
    public static void setUp()
    {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void tearDown()
    {
        vertx.close();
    }

    @Test
    public void testAuthHeaderSet()
    {
        HttpClientConfig config = httpClientConfigBuilder().cassandraRole("custom_role").build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            SidecarInstance instance = mock(SidecarInstance.class);
            when(instance.port()).thenReturn(9043);
            when(instance.hostname()).thenReturn("localhost");
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            HttpRequest<Buffer> request = client.vertxRequest(instance, context);
            assertThat(request.headers()).isNotEmpty();
            assertThat(request.headers().get(AUTH_ROLE)).isEqualTo("custom_role");
        }
    }

    private HttpClientConfig.Builder<?> httpClientConfigBuilder()
    {
        return new HttpClientConfig.Builder<>()
               .userAgent("sidecar-client-test/1.0.0")
               .ssl(false)
               .timeoutMillis(100)
               .idleTimeoutMillis(100);
    }
}
