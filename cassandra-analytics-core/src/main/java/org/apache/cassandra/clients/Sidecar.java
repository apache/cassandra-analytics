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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarConfig;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.cassandra.spark.utils.MapUtils;

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
                                     SslConfig sslConfig) throws IOException
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true).setWorkerPoolSize(config.maxPoolSize()));

        HttpClientConfig.Builder<?> builder = new HttpClientConfig.Builder<>()
                .ssl(false)
                .timeoutMillis(TimeUnit.SECONDS.toMillis(config.timeoutSeconds()))
                .idleTimeoutMillis((int) TimeUnit.SECONDS.toMillis(config.timeoutSeconds()))
                .receiveBufferSize((int) config.chunkBufferSize())
                .maxChunkSize((int) config.maxBufferSize())
                .userAgent(BuildInfo.READER_USER_AGENT);

        if (sslConfig != null)
        {
            builder = builder
                    .ssl(true)
                    .keyStoreInputStream(sslConfig.keyStoreInputStream())
                    .keyStorePassword(sslConfig.keyStorePassword())
                    .keyStoreType(sslConfig.keyStoreType())
                    .trustStoreInputStream(sslConfig.trustStoreInputStream())
                    .trustStorePassword(sslConfig.trustStorePassword())
                    .trustStoreType(sslConfig.trustStoreType());
        }

        HttpClientConfig httpClientConfig = builder.build();

        SidecarConfig sidecarConfig = new SidecarConfig.Builder()
                .maxRetries(config.maxRetries())
                .retryDelayMillis(config.millisToSleep())
                .maxRetryDelayMillis(config.maxMillisToSleep())
                .build();

        return buildClient(sidecarConfig, vertx, httpClientConfig, sidecarInstancesProvider);
    }

    public static SidecarClient from(SidecarInstancesProvider sidecarInstancesProvider, BulkSparkConf conf)
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true)
                                                    .setWorkerPoolSize(conf.getMaxHttpConnections()));

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                .timeoutMillis(conf.getHttpResponseTimeoutMs())
                .idleTimeoutMillis(conf.getHttpConnectionTimeoutMs())
                .userAgent(BuildInfo.WRITER_USER_AGENT)
                .keyStoreInputStream(conf.getKeyStore())
                .keyStorePassword(conf.getKeyStorePassword())
                .keyStoreType(conf.getKeyStoreTypeOrDefault())
                .trustStoreInputStream(conf.getTrustStore())
                .trustStorePassword(conf.getTrustStorePasswordOrDefault())
                .trustStoreType(conf.getTrustStoreTypeOrDefault())
                .ssl(conf.hasKeystoreAndKeystorePassword())
                .build();

        SidecarConfig sidecarConfig = new SidecarConfig.Builder()
                .maxRetries(conf.getSidecarRequestRetries())
                .retryDelayMillis(TimeUnit.SECONDS.toMillis(conf.getSidecarRequestRetryDelayInSeconds()))
                .maxRetryDelayMillis(TimeUnit.SECONDS.toMillis(conf.getSidecarRequestMaxRetryDelayInSeconds()))
                .build();

        return buildClient(sidecarConfig, vertx, httpClientConfig, sidecarInstancesProvider);
    }

    public static SidecarClient buildClient(SidecarConfig sidecarConfig,
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

    public static List<NodeSettings> allNodeSettingsBlocking(BulkSparkConf conf,
                                                             SidecarClient client,
                                                             Set<? extends SidecarInstance> instances)
    {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<NodeSettings> allNodeSettings = Collections.synchronizedList(new ArrayList<>());
        for (SidecarInstance instance : instances)
        {
            futures.add(client.nodeSettings(instance)
                              .exceptionally(throwable -> {
                                  LOGGER.warn(String.format("Failed to execute node settings on instance=%s",
                                                            instance), throwable);
                                  return null;
                              })
                              .thenAccept(nodeSettings -> {
                                  if (nodeSettings != null)
                                  {
                                      allNodeSettings.add(nodeSettings);
                                  }
                              }));
        }
        try
        {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .get(conf.getSidecarRequestMaxRetryDelayInSeconds(), TimeUnit.SECONDS);
        }
        catch (ExecutionException | InterruptedException exception)
        {
            throw new RuntimeException(exception);
        }
        catch (TimeoutException exception)
        {
            // Any futures that have already completed will have put their results into `allNodeSettings`
            // at this point. Cancel any remaining futures and move on.
            for (CompletableFuture<Void> future : futures)
            {
                future.cancel(true);
            }
        }
        long successCount = allNodeSettings.size();
        if (successCount == 0)
        {
            throw new RuntimeException(String.format("Unable to determine the node settings. 0/%d instances available.",
                                                     instances.size()));
        }
        else if (successCount < instances.size())
        {
            LOGGER.debug("{}/{} instances were used to determine the node settings",
                         successCount, instances.size());
        }
        return allNodeSettings;
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

        private final int port;
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
        private ClientConfig(int port,
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
            this.port = port;
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

        public int port()
        {
            return port;
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
            return ClientConfig.create(DEFAULT_SIDECAR_PORT, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int port)
        {
            return ClientConfig.create(port, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int port, int maxRetries, long millisToSleep)
        {
            return ClientConfig.create(port,
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
            return create(MapUtils.getInt(options, SIDECAR_PORT, DEFAULT_SIDECAR_PORT),
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
        public static ClientConfig create(int port,
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
            return new ClientConfig(port,
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
