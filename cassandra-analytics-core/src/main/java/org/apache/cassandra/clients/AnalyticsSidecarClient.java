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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfigImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstancesProvider;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.DataTransport;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.cassandra.spark.validation.BulkWriterKeyStoreValidation;
import org.apache.cassandra.spark.validation.BulkWriterTrustStoreValidation;
import org.apache.cassandra.spark.validation.SslValidation;
import org.apache.cassandra.spark.validation.StartupValidator;

public class AnalyticsSidecarClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsSidecarClient.class);

    private AnalyticsSidecarClient()
    {
    }

    public static SidecarClient from(SidecarInstancesProvider sidecarInstancesProvider, BulkSparkConf conf)
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true)
                                                    .setWorkerPoolSize(conf.getMaxHttpConnections()));

        HttpClientConfig httpClientConfig = buildHttpClientConfig(conf);
        if (httpClientConfig.instanceId() != null)
        {
            LOGGER.info("Sidecar HTTP client configured with instanceId={} (applied to every outbound sidecar request)",
                        httpClientConfig.instanceId());
        }

        StartupValidator.instance().register(new SslValidation(conf));
        StartupValidator.instance().register(new BulkWriterKeyStoreValidation(conf));
        StartupValidator.instance().register(new BulkWriterTrustStoreValidation(conf));

        SidecarClientConfig sidecarConfig =
        SidecarClientConfigImpl.builder()
                               .maxRetries(conf.getSidecarRequestRetries())
                               .retryDelayMillis(conf.getSidecarRequestRetryDelayMillis())
                               .maxRetryDelayMillis(conf.getSidecarRequestMaxRetryDelayMillis())
                               .build();

        return Sidecar.buildClient(sidecarConfig, vertx, httpClientConfig, sidecarInstancesProvider);
    }

    static HttpClientConfig buildHttpClientConfig(BulkSparkConf conf)
    {
        String userAgent = transportModeBasedWriterUserAgent(conf.getTransportInfo().getTransport());
        return new HttpClientConfig.Builder<>()
               .timeoutMillis(conf.getHttpResponseTimeoutMs())
               .idleTimeoutMillis(conf.getHttpConnectionTimeoutMs())
               .userAgent(userAgent)
               .keyStoreInputStream(conf.getKeyStore())
               .keyStorePassword(conf.getKeyStorePassword())
               .keyStoreType(conf.getKeyStoreTypeOrDefault())
               .trustStoreInputStream(conf.getTrustStore())
               .trustStorePassword(conf.getTrustStorePasswordOrDefault())
               .trustStoreType(conf.getTrustStoreTypeOrDefault())
               .ssl(conf.hasKeystoreAndKeystorePassword())
               .cassandraRole(conf.getCassandraRole())
               .instanceId(conf.getSidecarInstanceId())
               .build();
    }

    static String transportModeBasedWriterUserAgent(DataTransport transport)
    {
        switch (transport)
        {
            case S3_COMPAT:
                return BuildInfo.WRITER_S3_USER_AGENT;
            case DIRECT:
            default:
                return BuildInfo.WRITER_USER_AGENT;
        }
    }
}
