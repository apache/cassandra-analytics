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

import io.netty.handler.codec.http.HttpMethod;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;

import org.apache.cassandra.sidecar.common.request.Request;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.AUTH_ROLE;
import static org.apache.cassandra.sidecar.common.http.SidecarQueryParamNames.INSTANCE_ID;
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
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(), context);
            assertThat(request.headers()).isNotEmpty();
            assertThat(request.headers().get(AUTH_ROLE)).isEqualTo("custom_role");
        }
    }

    @Test
    public void testInstanceIdQueryParamAppended()
    {
        HttpClientConfig config = httpClientConfigBuilder().instanceId(42).build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(), context);
            assertThat(request.queryParams().get(INSTANCE_ID)).isEqualTo("42");
        }
    }

    @Test
    public void testInstanceIdQueryParamNotAppendedWhenNull()
    {
        HttpClientConfig config = httpClientConfigBuilder().build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(), context);
            assertThat(request.queryParams().contains(INSTANCE_ID)).isFalse();
        }
    }

    @Test
    public void testInstanceIdQueryParamAppendedWithExistingQueryParams()
    {
        HttpClientConfig config = httpClientConfigBuilder().instanceId(7).build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            Request mockRequest = mock(Request.class);
            when(mockRequest.method()).thenReturn(HttpMethod.GET);
            when(mockRequest.requestURI()).thenReturn("/api/v1/ring?existingParam=value");
            RequestContext context = new RequestContext.Builder().request(mockRequest).build();
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(), context);
            assertThat(request.queryParams().get("existingParam")).isEqualTo("value");
            assertThat(request.queryParams().get(INSTANCE_ID)).isEqualTo("7");
        }
    }

    @Test
    public void testPerInstanceIdOverridesGlobalInstanceId()
    {
        HttpClientConfig config = httpClientConfigBuilder().instanceId(1).build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            // The instance carries its own id (3), which must win over the job-level id (1).
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(3), context);
            assertThat(request.queryParams().get(INSTANCE_ID)).isEqualTo("3");
        }
    }

    @Test
    public void testPerInstanceIdUsedWhenGlobalInstanceIdIsNull()
    {
        HttpClientConfig config = httpClientConfigBuilder().build();
        try (VertxHttpClient client = new VertxHttpClient(vertx, config))
        {
            RequestContext context = new RequestContext.Builder().ringRequest().build();
            HttpRequest<Buffer> request = client.vertxRequest(mockInstance(5), context);
            assertThat(request.queryParams().get(INSTANCE_ID)).isEqualTo("5");
        }
    }

    private SidecarInstance mockInstance()
    {
        return mockInstance(null);
    }

    private SidecarInstance mockInstance(Integer instanceId)
    {
        SidecarInstance instance = mock(SidecarInstance.class);
        when(instance.port()).thenReturn(9043);
        when(instance.hostname()).thenReturn("localhost");
        when(instance.instanceId()).thenReturn(instanceId);
        return instance;
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
