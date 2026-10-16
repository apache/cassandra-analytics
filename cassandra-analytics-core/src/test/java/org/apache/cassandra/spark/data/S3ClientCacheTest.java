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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link S3ClientCache}.
 */
class S3ClientCacheTest
{
    private static final Map<String, String> BASE_OPTIONS = ImmutableMap.<String, String>builder()
        .put("s3-region", "us-west-2")
        .put("s3-bucket", "test-bucket")
        .build();

    @BeforeEach
    void setUp()
    {
        S3ClientCache.reset();
    }

    @AfterEach
    void tearDown()
    {
        S3ClientCache.reset();
    }

    @Test
    void testCacheKeyGeneration()
    {
        S3ClientConfig config = createConfig(BASE_OPTIONS);
        String key = S3ClientCache.getCacheKey(config);

        // Key format: region|endpoint|accessKeyId|secretHash
        assertThat(key).startsWith("us-west-2|");
        assertThat(key).contains("|default|");
    }

    @Test
    void testCacheKeyWithCredentials()
    {
        Map<String, String> options = new HashMap<>(BASE_OPTIONS);
        options.put("s3-access-key-id", "AKIATEST123");
        options.put("s3-secret-access-key", "secretKey123");

        S3ClientConfig config = createConfig(options);
        String key = S3ClientCache.getCacheKey(config);

        assertThat(key).startsWith("us-west-2|");
        assertThat(key).contains("|AKIATEST123|");
        // Secret hash should be consistent
        int expectedHash = "secretKey123".hashCode();
        assertThat(key).endsWith("|" + expectedHash);
    }

    @Test
    void testCacheKeyWithEndpoint()
    {
        Map<String, String> options = new HashMap<>(BASE_OPTIONS);
        options.put("s3-endpoint-override", "http://localhost:9000");

        S3ClientConfig config = createConfig(options);
        String key = S3ClientCache.getCacheKey(config);

        assertThat(key).contains("http://localhost:9000");
    }

    @Test
    void testCacheKeyDifferentRegions()
    {
        S3ClientConfig config1 = createConfig(BASE_OPTIONS);

        Map<String, String> options2 = new HashMap<>(BASE_OPTIONS);
        options2.put("s3-region", "eu-west-1");
        S3ClientConfig config2 = createConfig(options2);

        String key1 = S3ClientCache.getCacheKey(config1);
        String key2 = S3ClientCache.getCacheKey(config2);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1).startsWith("us-west-2|");
        assertThat(key2).startsWith("eu-west-1|");
    }

    @Test
    void testSameConfigReturnsSameClient()
    {
        S3ClientConfig config = createConfig(BASE_OPTIONS);

        // Request the same client type twice
        S3Client client1 = S3ClientCache.getS3Client(config);
        S3Client client2 = S3ClientCache.getS3Client(config);

        // Should return the exact same instance
        assertThat(client1).isSameAs(client2);
        assertThat(S3ClientCache.cacheSize()).isEqualTo(1);
    }

    @Test
    void testSameConfigReturnsSameAsyncClient()
    {
        S3ClientConfig config = createConfig(BASE_OPTIONS);

        S3AsyncClient client1 = S3ClientCache.getS3AsyncClient(config);
        S3AsyncClient client2 = S3ClientCache.getS3AsyncClient(config);

        assertThat(client1).isSameAs(client2);
    }

    @Test
    void testAsyncCacheKeyIncludesHttpSignature()
    {
        Map<String, String> lowConcurrencyOptions = new HashMap<>(BASE_OPTIONS);
        lowConcurrencyOptions.put("s3-http-max-concurrency", "128");
        S3ClientConfig lowConcurrency = createConfig(lowConcurrencyOptions);

        Map<String, String> highConcurrencyOptions = new HashMap<>(BASE_OPTIONS);
        highConcurrencyOptions.put("s3-http-max-concurrency", "256");
        S3ClientConfig highConcurrency = createConfig(highConcurrencyOptions);

        assertThat(S3ClientCache.getCacheKey(lowConcurrency)).isEqualTo(S3ClientCache.getCacheKey(highConcurrency));
        assertThat(S3ClientCache.getAsyncCacheKey(lowConcurrency)).isNotEqualTo(S3ClientCache.getAsyncCacheKey(highConcurrency));
        assertThat(S3ClientCache.getAsyncCacheKey(highConcurrency)).contains("maxConcurrency=256");
    }

    @Test
    void testDifferentAsyncHttpConfigReturnsDifferentAsyncClient()
    {
        Map<String, String> lowConcurrencyOptions = new HashMap<>(BASE_OPTIONS);
        lowConcurrencyOptions.put("s3-http-max-concurrency", "128");
        S3ClientConfig lowConcurrency = createConfig(lowConcurrencyOptions);

        Map<String, String> highConcurrencyOptions = new HashMap<>(BASE_OPTIONS);
        highConcurrencyOptions.put("s3-http-max-concurrency", "256");
        S3ClientConfig highConcurrency = createConfig(highConcurrencyOptions);

        S3AsyncClient lowConcurrencyClient = S3ClientCache.getS3AsyncClient(lowConcurrency);
        S3AsyncClient highConcurrencyClient = S3ClientCache.getS3AsyncClient(highConcurrency);

        assertThat(lowConcurrencyClient).isNotSameAs(highConcurrencyClient);
        assertThat(S3ClientCache.cacheSize()).isEqualTo(2);

        S3ClientCache.close(lowConcurrency);

        assertThat(S3ClientCache.isCached(lowConcurrency)).isFalse();
        assertThat(S3ClientCache.isCached(highConcurrency)).isFalse();
    }

    @Test
    void testDifferentConfigReturnsDifferentClient()
    {
        S3ClientConfig config1 = createConfig(BASE_OPTIONS);

        Map<String, String> options2 = new HashMap<>(BASE_OPTIONS);
        options2.put("s3-region", "eu-west-1");
        S3ClientConfig config2 = createConfig(options2);

        S3Client client1 = S3ClientCache.getS3Client(config1);
        S3Client client2 = S3ClientCache.getS3Client(config2);

        assertThat(client1).isNotSameAs(client2);
        assertThat(S3ClientCache.cacheSize()).isEqualTo(2);
    }

    @Test
    void testResolveMaxConcurrencyUsesOverride()
    {
        Map<String, String> options = new HashMap<>(BASE_OPTIONS);
        options.put("s3-http-max-concurrency", "333");

        assertThat(S3ClientCache.resolveMaxConcurrency(createConfig(options))).isEqualTo(333);
    }

    @Test
    void testResolveMaxConcurrencyAutoClampsFromTaskSlots()
    {
        assertThat(S3ClientCache.resolveMaxConcurrency(1)).isEqualTo(128);
        assertThat(S3ClientCache.resolveMaxConcurrency(16)).isEqualTo(256);
        assertThat(S3ClientCache.resolveMaxConcurrency(256)).isEqualTo(1024);
    }

    @Test
    void testResolveMaxPendingConnectionAcquires()
    {
        assertThat(S3ClientCache.resolveMaxPendingConnectionAcquires(50)).isEqualTo(256);
        assertThat(S3ClientCache.resolveMaxPendingConnectionAcquires(256)).isEqualTo(512);
        assertThat(S3ClientCache.resolveMaxPendingConnectionAcquires(2000)).isEqualTo(2048);
    }

    @Test
    void testResolveTaskSlotsPrioritizesSparkConfThenEnvThenRuntime()
    {
        SparkConf conf = new SparkConf(false)
                         .set("spark.executor.cores", "16")
                         .set("spark.task.cpus", "2");

        S3ClientCache.ResolvedTaskSlots fromSparkConf =
        S3ClientCache.resolveTaskSlotsForAutoSize(conf, "4", 128);
        assertThat(fromSparkConf.taskSlots).isEqualTo(8);
        assertThat(fromSparkConf.source).isEqualTo("SparkEnv.spark.executor.cores/spark.task.cpus");

        S3ClientCache.ResolvedTaskSlots fromEnv =
        S3ClientCache.resolveTaskSlotsForAutoSize(null, "4", 128);
        assertThat(fromEnv.taskSlots).isEqualTo(4);
        assertThat(fromEnv.source).isEqualTo("SPARK_EXECUTOR_CORES env");

        S3ClientCache.ResolvedTaskSlots fromRuntime =
        S3ClientCache.resolveTaskSlotsForAutoSize(null, "not-an-int", 7);
        assertThat(fromRuntime.taskSlots).isEqualTo(7);
        assertThat(fromRuntime.source).isEqualTo("Runtime.availableProcessors fallback");
    }

    @Test
    void testCloseAll()
    {
        S3ClientConfig config1 = createConfig(BASE_OPTIONS);

        Map<String, String> options2 = new HashMap<>(BASE_OPTIONS);
        options2.put("s3-region", "eu-west-1");
        S3ClientConfig config2 = createConfig(options2);

        // Create some clients
        S3ClientCache.getS3Client(config1);
        S3ClientCache.getS3AsyncClient(config2);

        assertThat(S3ClientCache.cacheSize()).isGreaterThan(0);

        // Close all
        S3ClientCache.closeAll();

        assertThat(S3ClientCache.cacheSize()).isEqualTo(0);
        assertThat(S3ClientCache.isCached(config1)).isFalse();
        assertThat(S3ClientCache.isCached(config2)).isFalse();
    }

    @Test
    void testCloseSpecificConfig()
    {
        S3ClientConfig config1 = createConfig(BASE_OPTIONS);

        Map<String, String> options2 = new HashMap<>(BASE_OPTIONS);
        options2.put("s3-region", "eu-west-1");
        S3ClientConfig config2 = createConfig(options2);

        // Create clients for both configs
        S3ClientCache.getS3Client(config1);
        S3ClientCache.getS3Client(config2);

        assertThat(S3ClientCache.cacheSize()).isEqualTo(2);

        // Close only config1
        S3ClientCache.close(config1);

        assertThat(S3ClientCache.isCached(config1)).isFalse();
        assertThat(S3ClientCache.isCached(config2)).isTrue();
        assertThat(S3ClientCache.cacheSize()).isEqualTo(1);
    }

    @Test
    void testThreadSafety() throws InterruptedException
    {
        int numThreads = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        S3ClientConfig config = createConfig(BASE_OPTIONS);

        for (int t = 0; t < numThreads; t++)
        {
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++)
                    {
                        S3Client client = S3ClientCache.getS3Client(config);
                        if (client == null)
                        {
                            errors.incrementAndGet();
                        }
                    }
                }
                catch (Exception e)
                {
                    errors.incrementAndGet();
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(errors.get()).isEqualTo(0);
        // All threads should get the same cached instance
        assertThat(S3ClientCache.cacheSize()).isEqualTo(1);
    }

    @Test
    void testNullCredentialsUsesDefaultProvider()
    {
        // Config without explicit credentials
        S3ClientConfig config = createConfig(BASE_OPTIONS);

        // Should not throw - will use DefaultCredentialsProvider
        S3Client client = S3ClientCache.getS3Client(config);
        assertThat(client).isNotNull();
    }

    @Test
    void testCacheKeyDeterministic()
    {
        // Same config created multiple times should produce same key
        S3ClientConfig config1 = createConfig(BASE_OPTIONS);
        S3ClientConfig config2 = createConfig(BASE_OPTIONS);

        String key1 = S3ClientCache.getCacheKey(config1);
        String key2 = S3ClientCache.getCacheKey(config2);

        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void testSecretHashDeterministic()
    {
        Map<String, String> options = new HashMap<>(BASE_OPTIONS);
        options.put("s3-access-key-id", "AKIATEST");
        options.put("s3-secret-access-key", "mySecretKey");

        S3ClientConfig config1 = createConfig(options);
        S3ClientConfig config2 = createConfig(options);

        String key1 = S3ClientCache.getCacheKey(config1);
        String key2 = S3ClientCache.getCacheKey(config2);

        // Same secret should produce same hash
        assertThat(key1).isEqualTo(key2);
    }

    private S3ClientConfig createConfig(Map<String, String> options)
    {
        return S3ClientConfig.create(new CaseInsensitiveStringMap(options));
    }
}
