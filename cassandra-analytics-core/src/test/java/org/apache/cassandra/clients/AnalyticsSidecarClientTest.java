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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.SparkConf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link AnalyticsSidecarClient}
 */
class AnalyticsSidecarClientTest
{
    @Test
    void testBuildHttpClientConfigDefaultInstanceIdIsNull()
    {
        BulkSparkConf conf = new BulkSparkConf(new SparkConf(), defaultOptions());
        HttpClientConfig httpClientConfig = AnalyticsSidecarClient.buildHttpClientConfig(conf);
        assertThat(httpClientConfig.instanceId()).isNull();
    }

    @Test
    void testBuildHttpClientConfigWiresInstanceId()
    {
        SparkConf sparkConf = new SparkConf().set(BulkSparkConf.SIDECAR_INSTANCE_ID, "9");
        BulkSparkConf conf = new BulkSparkConf(sparkConf, defaultOptions());
        HttpClientConfig httpClientConfig = AnalyticsSidecarClient.buildHttpClientConfig(conf);
        assertThat(httpClientConfig.instanceId()).isEqualTo(9);
    }

    @Test
    void testWarnIfGlobalInstanceIdIsAmbiguousAllowsSingleInstance()
    {
        HttpClientConfig httpClientConfig = configWithGlobalInstanceId("1");
        SimpleSidecarInstancesProvider provider =
        new SimpleSidecarInstancesProvider(Collections.singletonList(new SidecarInstanceImpl("127.0.0.1", 9999)));

        // A single instance is unambiguous: the global id can only apply to it, so this must not fail.
        assertThatCode(() -> AnalyticsSidecarClient.warnIfGlobalInstanceIdIsAmbiguous(httpClientConfig, provider))
        .doesNotThrowAnyException();
    }

    @Test
    void testWarnIfGlobalInstanceIdIsAmbiguousDoesNotThrowForMultipleInstances()
    {
        HttpClientConfig httpClientConfig = configWithGlobalInstanceId("2");
        SimpleSidecarInstancesProvider provider =
        new SimpleSidecarInstancesProvider(Arrays.asList(new SidecarInstanceImpl("127.0.0.1", 9999),
                                                         new SidecarInstanceImpl("127.0.0.2", 9999),
                                                         new SidecarInstanceImpl("127.0.0.3", 9999)));

        // Multiple instances relying on a single global id is a warning (per-instance ids can override it),
        // not a hard failure - so the job must still be allowed to start.
        assertThatCode(() -> AnalyticsSidecarClient.warnIfGlobalInstanceIdIsAmbiguous(httpClientConfig, provider))
        .doesNotThrowAnyException();
    }

    @Test
    void testWarnIfGlobalInstanceIdIsAmbiguousWithPerInstanceIds()
    {
        HttpClientConfig httpClientConfig = configWithGlobalInstanceId("1");
        SimpleSidecarInstancesProvider provider =
        new SimpleSidecarInstancesProvider(Arrays.asList(new SidecarInstanceImpl("127.0.0.1", 9999, 1),
                                                         new SidecarInstanceImpl("127.0.0.2", 9999, 2),
                                                         new SidecarInstanceImpl("127.0.0.3", 9999, 3)));

        assertThatCode(() -> AnalyticsSidecarClient.warnIfGlobalInstanceIdIsAmbiguous(httpClientConfig, provider))
        .doesNotThrowAnyException();
    }

    @Test
    void testWarnIfGlobalInstanceIdIsAmbiguousNoopWhenUnset()
    {
        HttpClientConfig httpClientConfig = AnalyticsSidecarClient.buildHttpClientConfig(
        new BulkSparkConf(new SparkConf(), defaultOptions()));
        SimpleSidecarInstancesProvider provider =
        new SimpleSidecarInstancesProvider(Arrays.asList(new SidecarInstanceImpl("127.0.0.1", 9999),
                                                         new SidecarInstanceImpl("127.0.0.2", 9999)));

        assertThat(httpClientConfig.instanceId()).isNull();
        assertThatCode(() -> AnalyticsSidecarClient.warnIfGlobalInstanceIdIsAmbiguous(httpClientConfig, provider))
        .doesNotThrowAnyException();
    }

    private HttpClientConfig configWithGlobalInstanceId(String instanceId)
    {
        SparkConf sparkConf = new SparkConf().set(BulkSparkConf.SIDECAR_INSTANCE_ID, instanceId);
        return AnalyticsSidecarClient.buildHttpClientConfig(new BulkSparkConf(sparkConf, defaultOptions()));
    }

    private Map<String, String> defaultOptions()
    {
        Map<String, String> options = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        options.put(WriterOptions.SIDECAR_CONTACT_POINTS.name(), "127.0.0.1");
        options.put(WriterOptions.KEYSPACE.name(), "ks");
        options.put(WriterOptions.TABLE.name(), "table");
        options.put(WriterOptions.KEYSTORE_PASSWORD.name(), "dummy_password");
        // Base64 of "dummy"; getKeyStore() only decodes it, it never parses the bytes as a real keystore.
        options.put(WriterOptions.KEYSTORE_BASE64_ENCODED.name(), "ZHVtbXk=");
        return options;
    }
}
