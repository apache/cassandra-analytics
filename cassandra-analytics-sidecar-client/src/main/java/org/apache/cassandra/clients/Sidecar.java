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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import o.a.c.sidecar.client.shaded.common.response.GossipInfoResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.NodeSettings;
import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import org.apache.cassandra.secrets.SecretsProvider;
import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfigImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.client.VertxHttpClient;
import o.a.c.sidecar.client.shaded.client.VertxRequestExecutor;
import o.a.c.sidecar.client.shaded.client.retry.ExponentialBackoffRetryPolicy;
import o.a.c.sidecar.client.shaded.client.retry.RetryPolicy;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.cassandra.spark.validation.KeyStoreValidation;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.cassandra.spark.validation.TrustStoreValidation;
import org.jetbrains.annotations.Nullable;

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
                                              .userAgent(BuildInfo.READER_USER_AGENT)
                                              .cassandraRole(config.cassandraRole());

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
                                                                        Set<SidecarInstance> instances)
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

    /**
     * Retrieve gossip info from all nodes on the cluster
     *
     * @param client    Sidecar client
     * @param instances all Sidecar instances
     * @return completable futures with GossipInfoResponse
     */
    public static List<CompletableFuture<GossipInfoResponse>> gossipInfoFromAllNodes(SidecarClient client,
                                                                                     Set<SidecarInstance> instances)
    {
        return instances.stream()
                        .map(instance -> client
                                         .gossipInfo(instance)
                                         .exceptionally(throwable -> {
                                             LOGGER.warn(String.format("Failed to retrieve gossipinfo from instance=%s",
                                                                       instance), throwable);
                                             return null;
                                         }))
                        .collect(Collectors.toList());
    }

    /**
     * Retrieves SSTable versions from all nodes in the cluster via gossip info.
     * This method fetches gossip information from all Sidecar instances and extracts
     * the SSTable versions running on each node.
     *
     * @param client              Sidecar client
     * @param instances           all Sidecar instances in the cluster
     * @param maxRetryDelayMillis maximum delay in milliseconds between retries
     * @param maxRetries          maximum number of retry attempts
     * @return a set of SSTable versions across all nodes in the cluster
     * @throws RuntimeException if unable to retrieve gossip info from any nodes
     */
    public static Set<String> getSSTableVersionsFromCluster(SidecarClient client,
                                                            Set<SidecarInstance> instances,
                                                            long maxRetryDelayMillis,
                                                            int maxRetries)
    {
        LOGGER.debug("Retrieving SSTable versions from cluster via gossip...");

        List<CompletableFuture<GossipInfoResponse>> gossipInfoFutures = gossipInfoFromAllNodes(client, instances);

        // Calculate total timeout. Requests are issued in parallel, so the timeout is per-request
        // (delay * retries) and must not be multiplied by the number of instances.
        final long totalTimeout = maxRetryDelayMillis * maxRetries;

        List<GossipInfoResponse> gossipInfoResponses = FutureUtils.bestEffortGet(gossipInfoFutures,
                                                                                 totalTimeout,
                                                                                 TimeUnit.MILLISECONDS);

        if (gossipInfoResponses.isEmpty())
        {
            LOGGER.warn("Unable to retrieve gossip info from any nodes. 0/{} instances available.",
                        gossipInfoFutures.size());
            // do not fail here, bridge determination logic checks for feature flag and proceeds accordingly
            return Collections.emptySet();
        }
        else if (gossipInfoResponses.size() < gossipInfoFutures.size())
        {
            LOGGER.warn("{}/{} instances were used to retrieve gossip info and determine SSTable versions",
                        gossipInfoResponses.size(), gossipInfoFutures.size());
        }

        // Extract and collect SSTable versions from all gossip info responses
        Set<String> sstableVersions = gossipInfoResponses.stream()
                                                         .flatMap(response -> response.values().stream())
                                                         .map(GossipInfoResponse.GossipInfo::sstableVersions)
                                                         .filter(Objects::nonNull)
                                                         .flatMap(List::stream)
                                                         .collect(Collectors.toSet());

        LOGGER.info("Detected SSTable versions on cluster: {}", sstableVersions);
        return sstableVersions;
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
        public static final String CASSANDRA_ROLE_KEY = "cassandra_role";
        public static final String DEFAULT_CASSANDRA_ROLE = null;

        private final int userProvidedPort;
        private final int maxRetries;
        private final int maxPoolSize;
        private final int timeoutSeconds;
        private final long millisToSleep;
        private final long maxMillisToSleep;
        private final long maxBufferSize;
        private final long chunkSize;
        private final String cassandraRole;
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
                             String cassandraRole,
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
            this.cassandraRole = cassandraRole;
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

        @Nullable
        public String cassandraRole()
        {
            return cassandraRole;
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
                                       DEFAULT_CASSANDRA_ROLE,
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
                          MapUtils.getOrDefault(options, CASSANDRA_ROLE_KEY, DEFAULT_CASSANDRA_ROLE),
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
                                          String cassandraRole,
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
                                    cassandraRole,
                                    maxBufferOverride,
                                    chunkBufferOverride);
        }
    }
}
