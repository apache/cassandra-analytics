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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SslConfig;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the cache in {@link CassandraDataSourceHelper}
 */
public class CassandraDataSourceHelperCacheTest
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS =
            ImmutableMap.of("sidecar_contact_points", "127.0.0.1",
                            "keyspace", "big-data",
                            "table", "customers");

    private CacheTicker cacheTicker;

    @BeforeEach
    public void setup()
    {
        cacheTicker = new CacheTicker();
        CassandraDataSourceHelper.initCassandraDataSourceCache(cacheTicker);
    }

    @Test
    public void testCassandraDataLayerCacheExpiration() throws ExecutionException
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);

        Cache<Map<String, String>, CassandraDataLayer> cassandraDataLayerCache = CassandraDataSourceHelper.getCassandraDataLayerCache();
        CassandraDataLayer dataLayer = getCassandraDataLayer(cassandraDataLayerCache, options, options);

        assertNotNull(dataLayer);

        // Advance ticker 1 minute
        cacheTicker.advance(1, TimeUnit.MINUTES);

        // Should get the same instance
        CassandraDataLayer dataLayer1 = getCassandraDataLayer(cassandraDataLayerCache, options, options);
        assertSame(dataLayer, dataLayer1);

        // Advance ticker 1 hour
        cacheTicker.advance(1, TimeUnit.HOURS);

        // Should still get the same instance
        CassandraDataLayer dataLayer2 = getCassandraDataLayer(cassandraDataLayerCache, options, options);
        assertSame(dataLayer, dataLayer2);

        // Advance ticker 10 hours and 58 minutes and 59 seconds
        cacheTicker.advance(10, TimeUnit.HOURS);
        cacheTicker.advance(58, TimeUnit.MINUTES);
        cacheTicker.advance(59, TimeUnit.SECONDS);

        // Should still get the same instance
        CassandraDataLayer dataLayer3 = getCassandraDataLayer(cassandraDataLayerCache, options, options);
        assertSame(dataLayer, dataLayer3);

        // Advance ticker for 1 second
        cacheTicker.advance(1, TimeUnit.SECONDS);

        // Should get a different instance
        CassandraDataLayer dataLayer4 = getCassandraDataLayer(cassandraDataLayerCache, options, options);
        assertNotSame(dataLayer, dataLayer4);
    }

    @Test
    public void testMultipleThreadsAccessingTheSameKey() throws InterruptedException, ExecutionException
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);

        Cache<Map<String, String>, CassandraDataLayer> cassandraDataLayerCache = CassandraDataSourceHelper.getCassandraDataLayerCache();

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CassandraDataLayer[] cassandraDataLayerArray = new CassandraDataLayer[threads];
        CountDownLatch latch = new CountDownLatch(threads);

        for (int thread = 0; thread < threads; thread++)
        {
            int finalI = thread;
            pool.submit(() -> {
                try
                {
                    // Invoke getCassandraDataLayer roughly at the same time
                    latch.countDown();
                    latch.await();
                    // The first thread to win creates the object, the rest should get the same instance
                    cassandraDataLayerArray[finalI] = getCassandraDataLayer(cassandraDataLayerCache, options, options);
                }
                catch (InterruptedException | ExecutionException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
        }

        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES));

        for (int thread = 1; thread < threads; thread++)
        {
            assertSame(cassandraDataLayerArray[0], cassandraDataLayerArray[thread]);
        }

        // Advance ticker for 12 hours
        cacheTicker.advance(12, TimeUnit.HOURS);
        assertNotSame(cassandraDataLayerArray[0], getCassandraDataLayer(cassandraDataLayerCache, options, options));
    }

    private CassandraDataLayer getCassandraDataLayer(Cache<Map<String, String>, CassandraDataLayer> cassandraDataLayerCache,
                                                     Map<String, String> key,
                                                     Map<String, String> options) throws ExecutionException
    {
        return cassandraDataLayerCache.get(key, () -> {
            ClientConfig config = ClientConfig.create(options);
            return new CassandraDataLayer(config, Sidecar.ClientConfig.create(options), SslConfig.create(options));
        });
    }

    private static class CacheTicker extends Ticker
    {
        private final AtomicLong nanos = new AtomicLong(0);

        /**
         * Returns the number of nanoseconds elapsed since this ticker's fixed point of reference
         */
        @Override
        public long read()
        {
            return nanos.get();
        }

        /**
         * Artificially advance time for a given {@code value} in the given {@link TimeUnit}
         *
         * @param value the value to advance
         * @param unit  the {@link TimeUnit} for the given {@code value}
         */
        public void advance(long value, TimeUnit unit)
        {
            nanos.addAndGet(unit.toNanos(value));
        }
    }
}
