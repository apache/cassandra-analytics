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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfigImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
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
        warnIfGlobalInstanceIdIsAmbiguous(httpClientConfig, sidecarInstancesProvider);

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

    /**
     * Warns when a single job-level {@code instanceId} would be stamped uniformly onto requests fanned out
     * across more than one sidecar instance, none of which carry their own per-instance id. That is only correct
     * when every instance resolves the same id (for example a 1:1 Cassandra-to-Sidecar deployment where each local
     * instance is id {@code 1}); otherwise requests are misrouted. Operators should instead assign a per-instance id
     * to each sidecar contact point (see {@link org.apache.cassandra.spark.common.SidecarInstanceFactory}).
     */
    static void warnIfGlobalInstanceIdIsAmbiguous(HttpClientConfig httpClientConfig,
                                                  SidecarInstancesProvider sidecarInstancesProvider)
    {
        Integer globalInstanceId = httpClientConfig.instanceId();
        if (globalInstanceId == null)
        {
            return;
        }

        List<SidecarInstance> instances = sidecarInstancesProvider.instances();
        boolean anyPerInstanceId = instances.stream().anyMatch(instance -> instance.instanceId() != null);
        if (instances.size() > 1 && !anyPerInstanceId)
        {
            LOGGER.warn("Spark conf {}={} will be applied uniformly to every request across {} sidecar instances, "
                        + "none of which declare their own instanceId. This is only correct when every instance "
                        + "resolves the same id (for example a 1:1 Cassandra-to-Sidecar deployment where each local "
                        + "instance is id {}). If the instances have distinct ids this misroutes requests; assign a "
                        + "per-instance id to each sidecar contact point (host:port={}) instead.",
                        BulkSparkConf.SIDECAR_INSTANCE_ID, globalInstanceId, instances.size(),
                        globalInstanceId, globalInstanceId);
        }
        else
        {
            LOGGER.info("Sidecar HTTP client configured with job-level instanceId={} (used only for requests to "
                        + "instances without their own instanceId)", globalInstanceId);
        }
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
