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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClientConfigImpl;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.DataTransport;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.cassandra.spark.validation.KeyStoreValidation;
import org.apache.cassandra.spark.validation.SslValidation;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.cassandra.spark.validation.TrustStoreValidation;

import static org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_OVERRIDE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_OVERRIDE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_MILLIS_TO_SLEEP;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_POOL_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_RETRIES;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MILLIS_TO_SLEEP;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_SIDECAR_PORT;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_TIMEOUT_SECONDS;

/**
 * A helper class that encapsulates configuration for the Spark Bulk Reader and Writer and helper methods to build the
 * {@link SidecarClient}
 */
public final class Sidecar
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Sidecar.class);

    private Sidecar()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static SidecarClient from(SidecarInstancesProvider sidecarInstancesProvider,
                                     ClientConfig config,
                                     SecretsProvider secretsProvider) throws IOException
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true).setWorkerPoolSize(config.maxPoolSize()));

        HttpClientConfig.Builder<?> builder = new HttpClientConfig.Builder<>()
                                              .ssl(false)
                                              .timeoutMillis(TimeUnit.SECONDS.toMillis(config.timeoutSeconds()))
                                              .idleTimeoutMillis((int) TimeUnit.SECONDS.toMillis(config.timeoutSeconds()))
                                              .receiveBufferSize((int) config.chunkBufferSize())
                                              .maxChunkSize((int) config.maxBufferSize())
                                              .userAgent(BuildInfo.READER_USER_AGENT);

        if (secretsProvider != null)
        {
            builder = builder
                      .ssl(true)
                      .keyStoreInputStream(secretsProvider.keyStoreInputStream())
                      .keyStorePassword(String.valueOf(secretsProvider.keyStorePassword()))
                      .keyStoreType(secretsProvider.keyStoreType())
                      .trustStoreInputStream(secretsProvider.trustStoreInputStream())
                      .trustStorePassword(String.valueOf(secretsProvider.trustStorePassword()))
                      .trustStoreType(secretsProvider.trustStoreType());

            StartupValidator.instance().register(new KeyStoreValidation(secretsProvider));
            StartupValidator.instance().register(new TrustStoreValidation(secretsProvider));
        }

        HttpClientConfig httpClientConfig = builder.build();

        SidecarClientConfig sidecarConfig = SidecarClientConfigImpl.builder()
                                                                   .maxRetries(config.maxRetries())
                                                                   .retryDelayMillis(config.millisToSleep())
                                                                   .maxRetryDelayMillis(config.maxMillisToSleep())
                                                                   .build();

        return buildClient(sidecarConfig, vertx, httpClientConfig, sidecarInstancesProvider);
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

    public static SidecarClient from(SidecarInstancesProvider sidecarInstancesProvider, BulkSparkConf conf)
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true)
                                                    .setWorkerPoolSize(conf.getMaxHttpConnections()));

        String userAgent = transportModeBasedWriterUserAgent(conf.getTransportInfo().getTransport());
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
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
                                            .build();

        StartupValidator.instance().register(new SslValidation(conf));
        StartupValidator.instance().register(new KeyStoreValidation(conf));
        StartupValidator.instance().register(new TrustStoreValidation(conf));

        SidecarClientConfig sidecarConfig =
        SidecarClientConfigImpl.builder()
                               .maxRetries(conf.getSidecarRequestRetries())
                               .retryDelayMillis(conf.getSidecarRequestRetryDelayMillis())
                               .maxRetryDelayMillis(conf.getSidecarRequestMaxRetryDelayMillis())
                               .build();

        return buildClient(sidecarConfig, vertx, httpClientConfig, sidecarInstancesProvider);
    }

    public static SidecarClient buildClient(SidecarClientConfig sidecarConfig,
                                            Vertx vertx,
                                            HttpClientConfig httpClientConfig,
                                            SidecarInstancesProvider clusterConfig)
    {
        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(sidecarConfig.maxRetries(),
                                                                           sidecarConfig.retryDelayMillis(),
                                                                           sidecarConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        return new SidecarClient(clusterConfig, requestExecutor, sidecarConfig, defaultRetryPolicy);
    }

    public static List<CompletableFuture<NodeSettings>> allNodeSettings(SidecarClient client,
                                                                        Set<? extends SidecarInstance> instances)
    {
        return instances.stream()
                        .map(instance -> client
                                         .nodeSettings(instance)
                                         .exceptionally(throwable -> {
                                             LOGGER.warn(String.format("Failed to execute node settings on instance=%s",
                                                                       instance), throwable);
                                             return null;
                                         }))
                        .collect(Collectors.toList());
    }

    public static SidecarInstance toSidecarInstance(CassandraInstance instance, int sidecarPort)
    {
        return new SidecarInstanceImpl(instance.nodeName(), sidecarPort);
    }

    public static final class ClientConfig
    {
        public static final String SIDECAR_PORT = "sidecar_port";
        public static final String MAX_RETRIES_KEY = "maxRetries";
        public static final String DEFAULT_MILLIS_TO_SLEEP_KEY = "defaultMillisToSleep";
        public static final String MAX_MILLIS_TO_SLEEP_KEY = "maxMillisToSleep";
        public static final String MAX_BUFFER_SIZE_BYTES_KEY = "maxBufferSizeBytes";
        public static final String CHUNK_BUFFER_SIZE_BYTES_KEY = "chunkBufferSizeBytes";
        public static final String MAX_POOL_SIZE_KEY = "maxPoolSize";
        public static final String TIMEOUT_SECONDS_KEY = "timeoutSeconds";

        private final int userProvidedPort;
        private final int maxRetries;
        private final int maxPoolSize;
        private final int timeoutSeconds;
        private final long millisToSleep;
        private final long maxMillisToSleep;
        private final long maxBufferSize;
        private final long chunkSize;
        private final Map<FileType, Long> maxBufferOverride;
        private final Map<FileType, Long> chunkBufferOverride;

        // CHECKSTYLE IGNORE: Constructor with many parameters
        private ClientConfig(int userProvidedPort,
                             int maxRetries,
                             long millisToSleep,
                             long maxMillisToSleep,
                             long maxBufferSize,
                             long chunkSize,
                             int maxPoolSize,
                             int timeoutSeconds,
                             Map<FileType, Long> maxBufferOverride,
                             Map<FileType, Long> chunkBufferOverride)
        {
            this.userProvidedPort = userProvidedPort;
            this.maxRetries = maxRetries;
            this.millisToSleep = millisToSleep;
            this.maxMillisToSleep = maxMillisToSleep;
            this.maxBufferSize = maxBufferSize;
            this.chunkSize = chunkSize;
            this.maxPoolSize = maxPoolSize;
            this.timeoutSeconds = timeoutSeconds;
            this.maxBufferOverride = maxBufferOverride;
            this.chunkBufferOverride = chunkBufferOverride;
        }

        public int userProvidedPort()
        {
            return userProvidedPort;
        }

        public int effectivePort()
        {
            return userProvidedPort == -1 ? DEFAULT_SIDECAR_PORT : userProvidedPort;
        }

        public int maxRetries()
        {
            return maxRetries;
        }

        public long millisToSleep()
        {
            return millisToSleep;
        }

        public long maxMillisToSleep()
        {
            return maxMillisToSleep;
        }

        public long maxBufferSize()
        {
            return maxBufferSize(FileType.DATA);
        }

        public long maxBufferSize(FileType fileType)
        {
            return maxBufferOverride.getOrDefault(fileType, maxBufferSize);
        }

        public Map<FileType, Long> maxBufferOverride()
        {
            return maxBufferOverride;
        }

        public long chunkBufferSize()
        {
            return chunkBufferSize(FileType.DATA);
        }

        public long chunkBufferSize(FileType fileType)
        {
            return chunkBufferOverride.getOrDefault(fileType, chunkSize);
        }

        public Map<FileType, Long> chunkBufferOverride()
        {
            return chunkBufferOverride;
        }

        public int maxPoolSize()
        {
            return maxPoolSize;
        }

        public int timeoutSeconds()
        {
            return timeoutSeconds;
        }

        public static ClientConfig create()
        {
            return ClientConfig.create(-1, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int userProvidedPort, int effectivePort)
        {
            return ClientConfig.create(userProvidedPort, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int userProvidedPort, int maxRetries, long millisToSleep)
        {
            return ClientConfig.create(userProvidedPort,
                                       maxRetries,
                                       millisToSleep,
                                       DEFAULT_MAX_MILLIS_TO_SLEEP,
                                       DEFAULT_MAX_BUFFER_SIZE,
                                       DEFAULT_CHUNK_BUFFER_SIZE,
                                       DEFAULT_MAX_POOL_SIZE,
                                       DEFAULT_TIMEOUT_SECONDS,
                                       DEFAULT_MAX_BUFFER_OVERRIDE,
                                       DEFAULT_CHUNK_BUFFER_OVERRIDE);
        }

        public static ClientConfig create(Map<String, String> options)
        {
            Optional<Integer> userProvidedPort = MapUtils.getOptionalInt(options, SIDECAR_PORT, SIDECAR_PORT);
            return create(userProvidedPort.orElse(-1),
                          MapUtils.getInt(options, MAX_RETRIES_KEY, DEFAULT_MAX_RETRIES),
                          MapUtils.getLong(options, DEFAULT_MILLIS_TO_SLEEP_KEY, DEFAULT_MILLIS_TO_SLEEP),
                          MapUtils.getLong(options, MAX_MILLIS_TO_SLEEP_KEY, DEFAULT_MAX_MILLIS_TO_SLEEP),
                          MapUtils.getLong(options, MAX_BUFFER_SIZE_BYTES_KEY, DEFAULT_MAX_BUFFER_SIZE),
                          MapUtils.getLong(options, CHUNK_BUFFER_SIZE_BYTES_KEY, DEFAULT_CHUNK_BUFFER_SIZE),
                          MapUtils.getInt(options, MAX_POOL_SIZE_KEY, DEFAULT_MAX_POOL_SIZE),
                          MapUtils.getInt(options, TIMEOUT_SECONDS_KEY, DEFAULT_TIMEOUT_SECONDS),
                          buildMaxBufferOverride(options, DEFAULT_MAX_BUFFER_OVERRIDE),
                          buildChunkBufferOverride(options, DEFAULT_CHUNK_BUFFER_OVERRIDE)
            );
        }

        public static Map<FileType, Long> buildMaxBufferOverride(Map<String, String> options,
                                                                 Map<FileType, Long> defaultValue)
        {
            return buildOverrideMap(MAX_BUFFER_SIZE_BYTES_KEY, options, defaultValue);
        }

        public static Map<FileType, Long> buildChunkBufferOverride(Map<String, String> options,
                                                                   Map<FileType, Long> defaultValue)
        {
            return buildOverrideMap(CHUNK_BUFFER_SIZE_BYTES_KEY, options, defaultValue);
        }

        private static Map<FileType, Long> buildOverrideMap(String keyPrefix,
                                                            Map<String, String> options,
                                                            Map<FileType, Long> defaultValue)
        {
            Map<FileType, Long> result = new HashMap<>(defaultValue);
            for (FileType type : FileType.values())
            {
                // Override with DataSourceOptions if set, e.g. maxBufferSizeBytes_Index.db
                String key = MapUtils.lowerCaseKey(String.format("%s_%s", keyPrefix, type.getFileSuffix()));
                Optional.ofNullable(options.get(key)).map(Long::parseLong).ifPresent(s -> result.put(type, s));
            }
            return result;
        }

        // CHECKSTYLE IGNORE: Method with many parameters
        public static ClientConfig create(int userProvidedPort,
                                          int maxRetries,
                                          long millisToSleep,
                                          long maxMillisToSleep,
                                          long maxBufferSizeBytes,
                                          long chunkSizeBytes,
                                          int maxPoolSize,
                                          int timeoutSeconds,
                                          Map<FileType, Long> maxBufferOverride,
                                          Map<FileType, Long> chunkBufferOverride)
        {
            return new ClientConfig(userProvidedPort,
                                    maxRetries,
                                    millisToSleep,
                                    maxMillisToSleep,
                                    maxBufferSizeBytes,
                                    chunkSizeBytes,
                                    maxPoolSize,
                                    timeoutSeconds,
                                    maxBufferOverride,
                                    chunkBufferOverride);
        }
    }
}
