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

package org.apache.cassandra.cdc.sidecar;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ResultSetFuture;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SidecarStatePersisterTest
{
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();

    @Test
    public void testStatePersister() throws InterruptedException
    {
        Map<TokenRange, byte[]> store = new ConcurrentHashMap<>();
        CountDownLatch writeLatch = new CountDownLatch(1);
        SidecarCdcCassandraClient cassandraClient = new SidecarCdcCassandraClient()
        {
            @Override
            public List<ResultSetFuture> storeStateAsync(@NotNull String jobId, @NotNull TokenRange range, @NotNull ByteBuffer buf, long timestamp)
            {
                byte[] ar = new byte[buf.remaining()];
                buf.get(ar);
                store.put(range, ar);
                ResultSetFuture future = mock(ResultSetFuture.class);
                when(future.isDone()).thenReturn(true);
                writeLatch.countDown();
                return Collections.singletonList(future);
            }

            @Override
            public Stream<byte[]> loadStateForRange(String jobId, @Nullable TokenRange tokenRange)
            {
                return Stream.of(store.get(tokenRange));
            }
        };
        SidecarCdcOptions sidecarCdcOptions = new SidecarCdcOptions()
        {
            @Override
            public Duration persistDelay()
            {
                return Duration.ofMillis(50);
            }
        };
        AsyncExecutor asyncExecutor = AsyncExecutor.wrap(EXECUTOR_SERVICE);

        // build and start SidecarStatePersister
        SidecarStatePersister statePersister = new SidecarStatePersister(
        sidecarCdcOptions,
        CdcOptions.DEFAULT,
        SidecarCdcStats.STUB,
        cassandraClient,
        asyncExecutor
        );
        assertEquals(-1, statePersister.timerId);
        statePersister.start();
        assertTrue(statePersister.timerId >= 0);

        // write random data
        byte[] data = new byte[1024];
        ThreadLocalRandom.current().nextBytes(data);
        TokenRange tokenRange = TokenRange.openClosed(BigInteger.ONE, BigInteger.valueOf(10L));
        statePersister.persist("101", 0, tokenRange, ByteBuffer.wrap(data));

        // wait for write to complete and verify data matches
        writeLatch.await(5, TimeUnit.SECONDS);
        List<byte[]> result = statePersister.loadStateForRange("101", tokenRange).collect(Collectors.toList());
        assertEquals(1, result.size());
        assertArrayEquals(data, result.get(0));

        statePersister.stop();
        assertEquals(-1, statePersister.timerId);
    }
}
