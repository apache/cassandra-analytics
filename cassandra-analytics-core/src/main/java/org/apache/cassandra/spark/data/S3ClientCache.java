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

package org.apache.cassandra.spark.data;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Unified cache for S3 clients and transfer managers.
 * <p>
 * This class consolidates all S3 client creation across the codebase into a single
 * cached location. Clients are keyed by region|endpoint|accessKeyId|secretHash for
 * proper isolation across different configurations.
 * <p>
 * Key design decisions:
 * <ul>
 *   <li>No JVM shutdown hooks - problematic in Spark executors. Use explicit {@link #closeAll()}</li>
 *   <li>Thread-safe with ConcurrentHashMap</li>
 *   <li>Clients should NOT be closed by callers - cache manages lifecycle</li>
 * </ul>
 */
public final class S3ClientCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ClientCache.class);
    static final int MAX_CONCURRENCY_PER_TASK_SLOT = 16;
    static final int MAX_CONCURRENCY_FLOOR = 128;
    static final int MAX_CONCURRENCY_CEILING = 1024;
    static final int MAX_PENDING_ACQUIRES_PER_CONNECTION = 2;
    static final int MAX_PENDING_ACQUIRES_FLOOR = 256;
    static final int MAX_PENDING_ACQUIRES_CEILING = 2048;
    static final int CONNECTION_ACQUISITION_TIMEOUT_SECONDS = 60;
    static final int CONNECTION_TIMEOUT_SECONDS = 5;
    static final int READ_TIMEOUT_SECONDS = 120;
    static final int CONNECTION_MAX_IDLE_TIME_SECONDS = 60;
    static final String NETTY_DNS_RESOLVER_CLASS = "io.netty.resolver.dns.DnsAddressResolverGroup";
    private static final boolean NON_BLOCKING_DNS_RESOLVER_AVAILABLE = isClassAvailable(NETTY_DNS_RESOLVER_CLASS);
    private static volatile boolean loggedTaskSlotSource = false;

    // Separate caches for different client types
    private static final ConcurrentHashMap<String, S3Client> SYNC_CLIENT_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, S3AsyncClient> ASYNC_CLIENT_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, S3TransferManager> TRANSFER_MANAGER_CACHE = new ConcurrentHashMap<>();

    private S3ClientCache()
    {
        // Static utility class
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /**
     * Get or create a cached synchronous S3Client for the given config.
     *
     * @param config S3ClientConfig with region, endpoint, and credentials
     * @return Cached or newly created S3Client (do NOT close - cache manages lifecycle)
     */
    public static S3Client getS3Client(S3ClientConfig config)
    {
        String key = getCacheKey(config);
        return SYNC_CLIENT_CACHE.computeIfAbsent(key, k -> {
            LOGGER.info("Creating new S3Client for key: {}", k);
            return buildS3Client(config);
        });
    }

    /**
     * Get or create a cached asynchronous S3AsyncClient for the given config.
     *
     * @param config S3ClientConfig with region, endpoint, and credentials
     * @return Cached or newly created S3AsyncClient (do NOT close - cache manages lifecycle)
     */
    public static S3AsyncClient getS3AsyncClient(S3ClientConfig config)
    {
        ResolvedAsyncHttpConfig resolved = resolveAsyncHttpConfig(config);
        String key = getAsyncCacheKey(config, resolved);
        return ASYNC_CLIENT_CACHE.computeIfAbsent(key, k -> {
            LOGGER.info("Creating new S3AsyncClient for key: {}", k);
            return buildS3AsyncClient(config, resolved);
        });
    }

    /**
     * Get or create a cached S3TransferManager for the given config.
     * TransferManager uses the async client internally for parallel downloads.
     *
     * @param config S3ClientConfig with region, endpoint, and credentials
     * @return Cached or newly created S3TransferManager (do NOT close - cache manages lifecycle)
     */
    public static S3TransferManager getTransferManager(S3ClientConfig config)
    {
        ResolvedAsyncHttpConfig resolved = resolveAsyncHttpConfig(config);
        String key = getAsyncCacheKey(config, resolved);
        return TRANSFER_MANAGER_CACHE.computeIfAbsent(key, k -> {
            LOGGER.info("Creating new S3TransferManager for key: {}", k);
            // TransferManager wraps an async client
            S3AsyncClient asyncClient = getS3AsyncClient(config);
            return S3TransferManager.builder()
                .s3Client(asyncClient)
                .build();
        });
    }

    // ========================================================================
    // Lifecycle Management
    // ========================================================================

    /**
     * Close all cached clients and clear the caches.
     * Should be called from the driver's stop() method.
     */
    public static void closeAll()
    {
        LOGGER.info("Closing all S3 client caches (sync={}, async={}, transfer={})",
                    SYNC_CLIENT_CACHE.size(), ASYNC_CLIENT_CACHE.size(), TRANSFER_MANAGER_CACHE.size());

        // Collect all unique keys across all caches
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(TRANSFER_MANAGER_CACHE.keySet());
        allKeys.addAll(ASYNC_CLIENT_CACHE.keySet());
        allKeys.addAll(SYNC_CLIENT_CACHE.keySet());

        // Close clients for each key (reuses closeClientsForKey logic)
        for (String key : allKeys)
        {
            closeClientsForKey(key);
        }
    }

    /**
     * Close cached clients for a specific config and remove from cache.
     * <p>
     * The sync client is keyed only by the base S3 identity. Async clients and transfer managers add
     * the resolved HTTP signature to their keys, so this evicts all async HTTP variants for the same
     * base identity rather than only the variant that would be resolved by the current JVM.
     *
     * @param config The config whose clients should be closed
     */
    public static void close(S3ClientConfig config)
    {
        String baseKey = getCacheKey(config);
        closeClientsForBaseKey(baseKey);
    }

    // ========================================================================
    // Key Generation
    // ========================================================================

    /**
     * Generate a cache key from the S3 config.
     * Key format: region|endpoint|accessKeyId|secretHash
     * <p>
     * Uses String.hashCode() for secret which is deterministic based on content.
     * Empty strings for credentials are normalized to "default" to match
     * getCredentialsProvider() behavior which treats empty strings the same as null.
     *
     * @param config S3ClientConfig
     * @return Cache key string
     */
    static String getCacheKey(S3ClientConfig config)
    {
        String accessKey = config.s3AccessKeyId();
        String secret = config.s3SecretAccessKey();
        // Normalize empty strings to match getCredentialsProvider() behavior
        String normalizedAccessKey = (accessKey != null && !accessKey.isEmpty()) ? accessKey : "default";
        int secretHash = (secret != null && !secret.isEmpty()) ? secret.hashCode() : 0;
        return config.s3Region() + "|" +
               Objects.toString(config.s3EndpointOverride(), "") + "|" +
               normalizedAccessKey + "|" +
               secretHash;
    }

    static String getAsyncCacheKey(S3ClientConfig config)
    {
        return getAsyncCacheKey(config, resolveAsyncHttpConfig(config));
    }

    private static String getAsyncCacheKey(S3ClientConfig config, ResolvedAsyncHttpConfig resolved)
    {
        return getCacheKey(config)
               + "|async|maxConcurrency=" + resolved.maxConcurrency
               + "|pending=" + resolved.maxPendingConnectionAcquires
               + "|acquireTimeout=" + CONNECTION_ACQUISITION_TIMEOUT_SECONDS
               + "|connectTimeout=" + CONNECTION_TIMEOUT_SECONDS
               + "|readTimeout=" + READ_TIMEOUT_SECONDS
               + "|idleTime=" + CONNECTION_MAX_IDLE_TIME_SECONDS
               + "|tcpKeepAlive=true|dns=nonBlocking";
    }

    // ========================================================================
    // Testing Support
    // ========================================================================

    /**
     * Reset all caches. For testing only.
     */
    static void reset()
    {
        closeAll();
        loggedTaskSlotSource = false;
    }

    /**
     * Get total cache size across all client types.
     *
     * @return Number of unique keys in the cache (max of sync, async, and transfer manager caches)
     */
    static int cacheSize()
    {
        return Math.max(SYNC_CLIENT_CACHE.size(),
                        Math.max(ASYNC_CLIENT_CACHE.size(), TRANSFER_MANAGER_CACHE.size()));
    }

    /**
     * Check if a key exists in any cache.
     *
     * @param config Config to check
     * @return true if cached
     */
    static boolean isCached(S3ClientConfig config)
    {
        String baseKey = getCacheKey(config);
        return hasKeyForBase(SYNC_CLIENT_CACHE.keySet(), baseKey) ||
               hasKeyForBase(ASYNC_CLIENT_CACHE.keySet(), baseKey) ||
               hasKeyForBase(TRANSFER_MANAGER_CACHE.keySet(), baseKey);
    }

    // ========================================================================
    // Private Implementation
    // ========================================================================

    private static S3Client buildS3Client(S3ClientConfig config)
    {
        AwsCredentialsProvider credentialsProvider = getCredentialsProvider(config);

        software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.s3Region()))
            .credentialsProvider(credentialsProvider);

        if (config.s3EndpointOverride() != null && !config.s3EndpointOverride().trim().isEmpty())
        {
            builder.endpointOverride(URI.create(config.s3EndpointOverride()))
                   .forcePathStyle(true);
        }

        return builder.build();
    }

    private static S3AsyncClient buildS3AsyncClient(S3ClientConfig config, ResolvedAsyncHttpConfig resolved)
    {
        AwsCredentialsProvider credentialsProvider = getCredentialsProvider(config);
        NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(resolved.maxConcurrency)
            .maxPendingConnectionAcquires(resolved.maxPendingConnectionAcquires)
            .connectionAcquisitionTimeout(Duration.ofSeconds(CONNECTION_ACQUISITION_TIMEOUT_SECONDS))
            .connectionTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS))
            .readTimeout(Duration.ofSeconds(READ_TIMEOUT_SECONDS))
            .connectionMaxIdleTime(Duration.ofSeconds(CONNECTION_MAX_IDLE_TIME_SECONDS))
            .tcpKeepAlive(true);

        if (isNonBlockingDnsResolverAvailable())
        {
            httpClientBuilder.useNonBlockingDnsResolver(true);
        }
        else
        {
            LOGGER.info("Netty non-blocking DNS resolver is not available on the classpath; "
                        + "building S3AsyncClient with the AWS SDK default DNS resolver");
        }

        software.amazon.awssdk.services.s3.S3AsyncClientBuilder builder = S3AsyncClient.builder()
            .region(Region.of(config.s3Region()))
            .credentialsProvider(credentialsProvider)
            .httpClientBuilder(httpClientBuilder);

        if (config.s3EndpointOverride() != null && !config.s3EndpointOverride().trim().isEmpty())
        {
            builder.endpointOverride(URI.create(config.s3EndpointOverride()))
                   .forcePathStyle(true);
        }

        LOGGER.info("Built S3AsyncClient region={} maxConcurrency={} maxPendingConnectionAcquires={} "
                    + "nonBlockingDnsResolver={} (knob={}, taskSlotsSource={}, taskSlots={})",
                    config.s3Region(),
                    resolved.maxConcurrency,
                    resolved.maxPendingConnectionAcquires,
                    isNonBlockingDnsResolverAvailable(),
                    config.s3HttpMaxConcurrency(),
                    resolved.taskSlots.source,
                    resolved.taskSlots.taskSlots);
        return builder.build();
    }

    static boolean isNonBlockingDnsResolverAvailable()
    {
        return NON_BLOCKING_DNS_RESOLVER_AVAILABLE;
    }

    private static boolean isClassAvailable(String className)
    {
        try
        {
            Class.forName(className, false, S3ClientCache.class.getClassLoader());
            return true;
        }
        catch (ClassNotFoundException exception)
        {
            return false;
        }
    }

    static int resolveMaxConcurrency(S3ClientConfig config)
    {
        int maxConcurrency = config.s3HttpMaxConcurrency();
        if (maxConcurrency > 0)
        {
            return maxConcurrency;
        }
        return resolveMaxConcurrency(resolveTaskSlotsForAutoSize().taskSlots);
    }

    static int resolveMaxConcurrency(int taskSlots)
    {
        return clamp(taskSlots * MAX_CONCURRENCY_PER_TASK_SLOT,
                     MAX_CONCURRENCY_FLOOR,
                     MAX_CONCURRENCY_CEILING);
    }

    static ResolvedTaskSlots resolveTaskSlotsForAutoSize()
    {
        SparkConf conf = null;
        try
        {
            SparkEnv env = SparkEnv.get();
            if (env != null)
            {
                conf = env.conf();
            }
        }
        catch (Throwable ignored)
        {
            // SparkEnv can be unavailable in unit tests or non-Spark callers.
        }
        return resolveTaskSlotsForAutoSize(conf,
                                           System.getenv("SPARK_EXECUTOR_CORES"),
                                           Runtime.getRuntime().availableProcessors());
    }

    static ResolvedTaskSlots resolveTaskSlotsForAutoSize(SparkConf conf,
                                                         String sparkExecutorCoresEnv,
                                                         int availableProcessors)
    {
        int taskCpus = 1;
        if (conf != null)
        {
            int executorCores = conf.getInt("spark.executor.cores", -1);
            taskCpus = Math.max(1, conf.getInt("spark.task.cpus", 1));
            if (executorCores > 0)
            {
                return resolvedTaskSlots("SparkEnv.spark.executor.cores/spark.task.cpus",
                                         Math.max(1, executorCores / taskCpus));
            }
        }

        if (sparkExecutorCoresEnv != null && !sparkExecutorCoresEnv.isEmpty())
        {
            try
            {
                int executorCores = Integer.parseInt(sparkExecutorCoresEnv.trim());
                if (executorCores > 0)
                {
                    return resolvedTaskSlots("SPARK_EXECUTOR_CORES env",
                                             Math.max(1, executorCores / taskCpus));
                }
            }
            catch (NumberFormatException ignored)
            {
                // Fall through to JVM-visible CPU count.
            }
        }

        return resolvedTaskSlots("Runtime.availableProcessors fallback", Math.max(1, availableProcessors));
    }

    static int resolveMaxPendingConnectionAcquires(int maxConcurrency)
    {
        long pending = (long) maxConcurrency * MAX_PENDING_ACQUIRES_PER_CONNECTION;
        return clamp(pending, MAX_PENDING_ACQUIRES_FLOOR, MAX_PENDING_ACQUIRES_CEILING);
    }

    private static ResolvedAsyncHttpConfig resolveAsyncHttpConfig(S3ClientConfig config)
    {
        ResolvedTaskSlots taskSlots = resolveTaskSlotsForAutoSize();
        int maxConcurrency = config.s3HttpMaxConcurrency() > 0
                             ? config.s3HttpMaxConcurrency()
                             : resolveMaxConcurrency(taskSlots.taskSlots);
        int maxPendingConnectionAcquires = resolveMaxPendingConnectionAcquires(maxConcurrency);
        return new ResolvedAsyncHttpConfig(taskSlots, maxConcurrency, maxPendingConnectionAcquires);
    }

    private static ResolvedTaskSlots resolvedTaskSlots(String source, int taskSlots)
    {
        logTaskSlotSourceOnce(source, taskSlots);
        return new ResolvedTaskSlots(source, taskSlots);
    }

    private static void logTaskSlotSourceOnce(String source, int taskSlots)
    {
        if (!loggedTaskSlotSource)
        {
            loggedTaskSlotSource = true;
            LOGGER.info("Resolved S3 async HTTP auto-sizing taskSlots={} from {}", taskSlots, source);
        }
    }

    private static int clamp(long value, int floor, int ceiling)
    {
        return (int) Math.max(floor, Math.min(ceiling, value));
    }

    private static AwsCredentialsProvider getCredentialsProvider(S3ClientConfig config)
    {
        String accessKeyId = config.s3AccessKeyId();
        String secretAccessKey = config.s3SecretAccessKey();
        if (accessKeyId != null && !accessKeyId.isEmpty() &&
            secretAccessKey != null && !secretAccessKey.isEmpty())
        {
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        return DefaultCredentialsProvider.create();
    }

    /**
     * Close all clients for a key and remove from caches.
     */
    private static void closeClientsForKey(String key)
    {
        // Close transfer manager first (wraps async client)
        S3TransferManager manager = TRANSFER_MANAGER_CACHE.remove(key);
        if (manager != null)
        {
            try
            {
                manager.close();
            }
            catch (Exception e)
            {
                LOGGER.warn("Error closing S3TransferManager for key {}: {}", key, e.getMessage());
            }
        }

        // Close async client
        S3AsyncClient asyncClient = ASYNC_CLIENT_CACHE.remove(key);
        if (asyncClient != null)
        {
            try
            {
                asyncClient.close();
            }
            catch (Exception e)
            {
                LOGGER.warn("Error closing S3AsyncClient for key {}: {}", key, e.getMessage());
            }
        }

        // Close sync client
        S3Client syncClient = SYNC_CLIENT_CACHE.remove(key);
        if (syncClient != null)
        {
            try
            {
                syncClient.close();
            }
            catch (Exception e)
            {
                LOGGER.warn("Error closing S3Client for key {}: {}", key, e.getMessage());
            }
        }
    }

    private static void closeClientsForBaseKey(String baseKey)
    {
        Set<String> keys = new HashSet<>();
        collectKeysForBase(keys, SYNC_CLIENT_CACHE.keySet(), baseKey);
        collectKeysForBase(keys, ASYNC_CLIENT_CACHE.keySet(), baseKey);
        collectKeysForBase(keys, TRANSFER_MANAGER_CACHE.keySet(), baseKey);
        for (String key : keys)
        {
            closeClientsForKey(key);
        }
    }

    private static void collectKeysForBase(Set<String> collector, Set<String> keys, String baseKey)
    {
        for (String key : keys)
        {
            if (isKeyForBase(key, baseKey))
            {
                collector.add(key);
            }
        }
    }

    private static boolean hasKeyForBase(Set<String> keys, String baseKey)
    {
        for (String key : keys)
        {
            if (isKeyForBase(key, baseKey))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isKeyForBase(String key, String baseKey)
    {
        return key.equals(baseKey) || key.startsWith(baseKey + "|async|");
    }

    static final class ResolvedTaskSlots
    {
        final String source;
        final int taskSlots;

        private ResolvedTaskSlots(String source, int taskSlots)
        {
            this.source = source;
            this.taskSlots = taskSlots;
        }
    }

    private static final class ResolvedAsyncHttpConfig
    {
        final ResolvedTaskSlots taskSlots;
        final int maxConcurrency;
        final int maxPendingConnectionAcquires;

        private ResolvedAsyncHttpConfig(ResolvedTaskSlots taskSlots,
                                        int maxConcurrency,
                                        int maxPendingConnectionAcquires)
        {
            this.taskSlots = taskSlots;
            this.maxConcurrency = maxConcurrency;
            this.maxPendingConnectionAcquires = maxPendingConnectionAcquires;
        }
    }
}
