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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.ThreadLocalMonotonicTimestampGenerator;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.CdcKryoRegister;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.StatePersister;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.util.CompressionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SidecarStatePersister buffers CDC state and flushes at regular time intervals, so we only write the latest CDC state and don't wastefully write expired data.
 */
public class SidecarStatePersister implements StatePersister
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarStatePersister.class);

    // group latest state by jobId/token range, so we persist independently
    protected final ConcurrentHashMap<PersistWrapper.Key, PersistWrapper> latestState = new ConcurrentHashMap<>();
    protected final ConcurrentLinkedQueue<TimedFutureWrapper> activeFlush = new ConcurrentLinkedQueue<>();
    private final ThreadLocalMonotonicTimestampGenerator timestampGenerator = new ThreadLocalMonotonicTimestampGenerator();
    private final SidecarCdcOptions sidecarCdcOptions;
    private final CdcOptions cdcOptions;
    private final SidecarCdcCassandraClient cassandraClient;
    private final SidecarCdcStats sidecarCdcStats;
    private final AsyncExecutor asyncExecutor;
    volatile long timerId = -1L;

    public SidecarStatePersister(SidecarCdcOptions sidecarCdcOptions,
                                 CdcOptions cdcOptions,
                                 SidecarCdcStats sidecarCdcStats,
                                 SidecarCdcCassandraClient cassandraClient,
                                 AsyncExecutor asyncExecutor)
    {
        this.sidecarCdcOptions = sidecarCdcOptions;
        this.cdcOptions = cdcOptions;
        this.sidecarCdcStats = sidecarCdcStats;
        this.cassandraClient = cassandraClient;
        this.asyncExecutor = asyncExecutor;
    }

    // StatePersister implemented methods

    @Override
    public void persist(String jobId, int partitionId, @Nullable TokenRange tokenRange, @NotNull ByteBuffer buf)
    {
        PersistWrapper latest = new PersistWrapper(jobId, partitionId, tokenRange, buf, timestampGenerator.next());
        PersistWrapper.Key key = latest.key();
        if (!latest.equals(this.latestState.get(key)))
        {
            this.latestState.compute(key, (k, prev) -> !latest.equals(prev) ? latest : prev);
        }
    }

    @NotNull
    @Override
    public List<CdcState> loadState(String jobId, int partitionId, @Nullable TokenRange tokenRange)
    {
        CompressionUtil compressionUtil = CdcBridgeFactory.get(cdcOptions.version()).compressionUtil();
        List<Integer> sizes = new ArrayList<>();
        // deserialize and merge the CDC state objects into canonical view
        List<CdcState> result = loadStateForRange(jobId, tokenRange)
                                .peek(bytes -> sizes.add(bytes.length))
                                .map(bytes -> CdcState.deserialize(CdcKryoRegister.kryo(), compressionUtil, bytes))
                                .collect(Collectors.toList());
        int count = sizes.size();
        int len = sizes.stream().mapToInt(i -> i).sum();
        LOGGER.debug("Read CDC state from Cassandra jobId={} start={} end={} stateCount={} stateSize={}",
                     jobId, tokenRange == null ? "null" : tokenRange.lowerEndpoint(), tokenRange == null ? "null" : tokenRange.upperEndpoint(), count, len);
        sidecarCdcStats.captureCdcConsumerReadFromState(count, len);
        return result;
    }

    @VisibleForTesting
    public Stream<byte[]> loadStateForRange(String jobId, @Nullable TokenRange tokenRange)
    {
        return cassandraClient
               .loadStateForRange(jobId, tokenRange);
    }

    /**
     * Start the SidecarStatePersister to flush to Cassandra every `persistDelay()`
     */
    public synchronized void start()
    {
        if (timerId >= 0)
        {
            // already running
            return;
        }
        this.timerId = asyncExecutor.periodicTimer(this::persistToCassandra, sidecarCdcOptions.persistDelay().toMillis());
    }

    /**
     * Stop the SidecarStatePersister gracefully, blocking to await for any pending flushes to complete.
     */
    public void stop()
    {
        stop(true);
    }

    public synchronized void stop(boolean flush)
    {
        if (this.timerId < 0)
        {
            // not running
            return;
        }

        asyncExecutor.cancelTimer(this.timerId);
        this.timerId = -1;

        if (flush)
        {
            flush();
        }
    }

    // internal methods

    protected void persistToCassandra()
    {
        persistToCassandra(false);
    }

    protected void persistToCassandra(boolean force)
    {
        // clean-up finished futures
        activeFlush.removeIf(wrapper -> {
            if (wrapper.allDone())
            {
                try
                {
                    wrapper.await();
                    sidecarCdcStats.capturePersistSucceeded(System.nanoTime() - wrapper.startTimeNanos);
                }
                catch (InterruptedException e)
                {
                    LOGGER.warn("Persist failed with InterruptedException", e);
                    Thread.currentThread().interrupt();
                    sidecarCdcStats.capturePersistFailed(e);
                }
                catch (Throwable throwable)
                {
                    LOGGER.warn("Persist failed", throwable);
                    sidecarCdcStats.capturePersistFailed(throwable);
                }
                return true;
            }
            return false;
        });

        if (!force && !activeFlush.isEmpty())
        {
            // check for active requests so we don't get backed up
            LOGGER.debug("CDC persist flush backed up, can't persist until active requests complete activeRequests={}", activeFlush.size());
            sidecarCdcStats.capturePersistBackedUp(activeFlush.size());
            return;
        }

        // drain the latestState map, so we don't persist multiple times wastefully
        List<PersistWrapper> states = this.latestState
                                      .keySet()
                                      .stream()
                                      .map(this.latestState::remove)
                                      .filter(Objects::nonNull)
                                      .collect(Collectors.toList());

        if (states.isEmpty())
        {
            // nothing to persist
            return;
        }

        states.stream()
              .map(this::persistToCassandra)
              .filter(Objects::nonNull)
              .forEach(activeFlush::add);
    }

    @Nullable
    protected TimedFutureWrapper persistToCassandra(@NotNull PersistWrapper state)
    {
        TokenRange range = state.tokenRange();
        if (range == null)
        {
            LOGGER.warn("Cannot persist state with null token range");
            return null;
        }

        try
        {
            LOGGER.debug("Persisting CDC state jobId={} partitionId={} start={} end={} sizeBytes={}",
                         state.jobId,
                         state.partitionId,
                         range.lowerEndpoint(),
                         range.upperEndpoint(),
                         state.buf.remaining());

            sidecarCdcStats.capturePersistingCdcStateLength(state.buf.remaining());
            return new TimedFutureWrapper(cassandraClient.storeStateAsync(state.jobId, range, state.buf, state.timestamp));
        }
        catch (Throwable t)
        {
            LOGGER.error("Unexpected error persisting CDC state to Cassandra", t);
            sidecarCdcStats.capturePersistFailed(t);
            // we failed to persist, so add back to latestState map if not already overwritten
            this.latestState.putIfAbsent(state.key(), state);
            return null;
        }
    }

    /**
     * Flush active state persist calls
     */
    protected void flush()
    {
        // persist any buffered state and flush in-flight requests
        persistToCassandra(true);
        flushActiveSafe();
    }

    protected void flushActiveSafe()
    {
        try
        {
            flushActive();
        }
        catch (ExecutionException e)
        {
            LOGGER.warn("Failed to flush active CDC state", ThrowableUtils.rootCause(e));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Flush active in-flight persist writes.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected void flushActive() throws ExecutionException, InterruptedException
    {
        for (TimedFutureWrapper wrapper : activeFlush)
        {
            wrapper.await();
        }
    }

    // helper classes

    protected static class PersistWrapper implements Comparable<PersistWrapper>
    {
        final String jobId;
        final int partitionId;
        @Nullable
        final TokenRange tokenRange;
        final ByteBuffer buf;
        final long timestamp;

        protected static class Key
        {
            private final String jobId;
            private final TokenRange tokenRange;

            protected Key(String jobId,
                          TokenRange tokenRange)
            {
                this.jobId = jobId;
                this.tokenRange = tokenRange;
            }

            public int hashCode()
            {
                return Objects.hash(jobId, tokenRange);
            }

            public boolean equals(Object o)
            {
                if (this == o)
                {
                    return true;
                }

                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                PersistWrapper.Key other = (PersistWrapper.Key) o;
                return jobId.equals(other.jobId)
                       && Objects.equals(tokenRange, other.tokenRange);
            }
        }

        protected PersistWrapper(String jobId,
                                 int partitionId,
                                 @Nullable TokenRange tokenRange,
                                 ByteBuffer buf,
                                 long timestamp)
        {
            this.jobId = jobId;
            this.partitionId = partitionId;
            this.tokenRange = tokenRange;
            this.buf = buf;
            this.timestamp = timestamp;
        }

        @Nullable
        public TokenRange tokenRange()
        {
            return tokenRange;
        }

        public Key key()
        {
            return new Key(jobId, tokenRange());
        }

        @Override
        public int compareTo(@NotNull PersistWrapper o)
        {
            return Long.compare(this.timestamp, o.timestamp);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(jobId, tokenRange());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            PersistWrapper other = (PersistWrapper) o;
            return jobId.equals(other.jobId)
                   && Objects.equals(tokenRange(), other.tokenRange())
                   && this.buf.equals(other.buf);
        }

        public static PersistWrapper max(PersistWrapper w1, PersistWrapper w2)
        {
            if (w1 == null)
            {
                return w2;
            }
            else if (w2 == null)
            {
                return w1;
            }

            return w1.compareTo(w2) > 0 ? w1 : w2;
        }
    }

    protected static class TimedFutureWrapper
    {
        protected final List<ResultSetFuture> futures;
        protected final long startTimeNanos;

        protected TimedFutureWrapper(List<ResultSetFuture> futures)
        {
            this.futures = futures;
            this.startTimeNanos = System.nanoTime();
        }

        public void await() throws ExecutionException, InterruptedException
        {
            for (ResultSetFuture future : futures)
            {
                future.get();
            }
        }

        public boolean allDone()
        {
            return futures.stream().allMatch(Future::isDone);
        }
    }
}
