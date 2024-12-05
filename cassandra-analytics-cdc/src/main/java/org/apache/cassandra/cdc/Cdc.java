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

package org.apache.cassandra.cdc;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.StatePersister;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused") // external facing API
public class Cdc implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Cdc.class);

    @NotNull
    private final String jobId;
    private final int partitionId;
    private final TokenRangeSupplier tokenRangeSupplier;
    private final TableIdLookup tableIdLookup;
    private final SchemaSupplier schemaSupplier;
    private final CassandraSource cassandraSource;
    private final StatePersister statePersister;
    private final CdcOptions cdcOptions;
    private final AsyncExecutor asyncExecutor;
    private final CommitLogProvider commitLogProvider;
    private final ICdcStats stats;
    private final EventConsumer eventConsumer;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> active = new AtomicReference<>(null);

    private volatile CdcState currentState = null;
    protected volatile long batchStartNanos;
    protected volatile Set<CqlTable> cdcEnabledTables = Collections.emptySet();

    protected Cdc(@NotNull CdcBuilder builder)
    {
        this.jobId = builder.jobId;
        this.partitionId = builder.partitionId;
        this.tokenRangeSupplier = builder.tokenRangeSupplier;
        this.tableIdLookup = builder.tableIdLookup;
        this.schemaSupplier = builder.schemaSupplier;
        this.cassandraSource = builder.cassandraSource;
        this.statePersister = builder.statePersister;
        this.cdcOptions = builder.cdcOptions;
        this.asyncExecutor = builder.asyncExecutor;
        this.commitLogProvider = builder.commitLogProvider;
        this.stats = builder.stats;
        this.eventConsumer = builder.eventConsumer;
    }

    public static CdcBuilder builder(@NotNull String jobId,
                                     int partitionId,
                                     EventConsumer eventConsumer,
                                     SchemaSupplier schemaSupplier)
    {
        return new CdcBuilder(jobId, partitionId, eventConsumer, schemaSupplier);
    }

    public String jobId()
    {
        return jobId;
    }

    public int partitionId()
    {
        return partitionId;
    }

    public long epoch()
    {
        return currentState.epoch;
    }

    @NotNull
    public CommitLogMarkers markers()
    {
        return this.currentState.markers;
    }

    public void start()
    {
        TokenRange tokenRange = tokenRangeSupplier.get();
        this.currentState = statePersister.loadCanonicalState(jobId, partitionId, tokenRange);

        if (!isRunning.get()
            && isRunning.compareAndSet(false, true))
        {
            LOGGER.info("Starting CDC Consumer jobId={} partitionId={} lower={} upper={}",
                        jobId,
                        partitionId,
                        tokenRange == null ? null : tokenRange.lowerEndpoint(),
                        tokenRange == null ? null : tokenRange.upperEndpoint());
            refreshSchema();
            scheduleRun(0);
            scheduleMonitorSchema();
        }
    }

    public void stop()
    {
        try
        {
            stop(true);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            LOGGER.error("Failed to stop CDC consumer cleanly", ThrowableUtils.rootCause(e));
        }
    }

    public void stop(boolean blocking) throws ExecutionException, InterruptedException
    {
        if (isRunning.get() && isRunning.compareAndSet(true, false))
        {
            LOGGER.info("Stopping CDC Consumer jobId={} partitionId={}", jobId, partitionId);
            final CompletableFuture<Void> activeFuture = active.get();
            if (activeFuture != null && blocking)
            {
                // block until active future completes
                long timeout = cdcOptions.stopTimeout().toMillis();
                try
                {
                    activeFuture.get(timeout, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {
                    LOGGER.warn("Failed to cleanly shutdown active future after {} millis", timeout);
                    stats.cdcConsumerStopTimeout();
                }
            }
            LOGGER.info("Stopped CDC Consumer jobId={} partitionId={}", jobId, partitionId);
        }
    }

    protected void scheduleNextRun()
    {
        scheduleRun(cdcOptions.nextDelayMillis(batchStartNanos));
    }

    protected void scheduleRun(long delayMillis)
    {
        if (!isRunning.get() || isFinished())
        {
            return;
        }

        active.getAndUpdate((curr) -> {
            if (curr == null)
            {
                CompletableFuture<Void> future = new CompletableFuture<>();
                if (delayMillis <= 0)
                {
                    // submit immediately
                    asyncExecutor.submit(() -> runSafe(future));
                }
                else
                {
                    // schedule for later
                    asyncExecutor.schedule(() -> runSafe(future), delayMillis);
                }
                return future;
            }
            return curr;
        });
    }

    protected void completeActiveFuture(CompletableFuture<Void> future)
    {
        if (active.compareAndSet(future, null))
        {
            future.complete(null);
        }
    }

    protected void runSafe(CompletableFuture<Void> future)
    {
        try
        {
            run();
            completeActiveFuture(future);
            scheduleNextRun();
        }
        catch (NotEnoughReplicasException e)
        {
            // NotEnoughReplicasException can occur when too many replicas are down
            // OR if there are no new commit logs to read if writes are idle on the cluster
            completeActiveFuture(future);
            scheduleRun(cdcOptions.sleepWhenInsufficientReplicas().toMillis());
        }
        catch (Throwable t)
        {
            completeActiveFuture(future);

            if (handleError(t))
            {
                LOGGER.warn("CdcConsumer epoch failed with recoverable error, scheduling next run jobId={} partition={} epoch={}",
                            jobId, partitionId, currentState.epoch, t);
                scheduleNextRun();
            }
            else
            {
                LOGGER.error("CdcConsumer epoch failed with unrecoverable error jobId={} partition={} epoch={}",
                             jobId, partitionId, currentState.epoch, t);
                stop();
            }
        }
    }

    /**
     * @param t throwable
     * @return true if Cdc consumer can continue, or false if it should stop.
     */
    protected boolean handleError(Throwable t)
    {
        LOGGER.error("Unexpected error in CdcConsumer", t);
        return true;
    }

    protected MicroBatchIterator newMicroBatchIterator() throws NotEnoughReplicasException
    {
        return newMicroBatchIterator(null, currentState);
    }

    protected MicroBatchIterator newMicroBatchIterator(@Nullable TokenRange tokenRange, CdcState startState) throws NotEnoughReplicasException
    {
        return new MicroBatchIterator(cdcBridge(),
                                      partitionId,
                                      tokenRange,
                                      startState,
                                      cassandraSource,
                                      this::keyspaceSupplier,
                                      cdcOptions,
                                      asyncExecutor,
                                      commitLogProvider,
                                      stats);
    }

    protected Set<String> keyspaceSupplier()
    {
        return cdcEnabledTables
               .stream()
               .map(CqlTable::keyspace)
               .collect(Collectors.toSet());
    }

    protected void run() throws NotEnoughReplicasException
    {
        this.batchStartNanos = System.nanoTime();
        TokenRange tokenRange = tokenRangeSupplier.get();

        // purge if full before starting otherwise it will keep failing when trying to add more entries to CdcState
        CdcState startState = this.currentState.purgeIfFull(stats, cdcOptions);

        try (MicroBatchIterator it = newMicroBatchIterator(tokenRange, startState))
        {
            // consume all events for micro-batch
            while (it.hasNext())
            {
                CdcEvent event = it.next();
                eventConsumer.accept(event);
            }

            // persist end state
            CdcState endState = it.endState();
            persist(endState, tokenRange);

            // only update state after persisting so failures
            // persisting or committing to the transport layer
            // cause the next microbatch to restart
            this.currentState = endState;
        }
    }

    protected boolean isFinished()
    {
        return !cdcOptions.isRunning() || epochsExceeded();
    }

    protected boolean epochsExceeded()
    {
        final int maxEpochs = cdcOptions.maxEpochs();
        return maxEpochs > 0 && this.currentState.epoch >= maxEpochs;
    }

    // cdc bridge

    protected CassandraBridge bridge()
    {
        return CdcBridgeFactory.get(cdcOptions.version());
    }

    protected CdcBridge cdcBridge()
    {
        return CdcBridgeFactory.getCdcBridge(cdcOptions.version());
    }

    // persist state

    public ByteBuffer serializeStateToBytes() throws IOException
    {
        try (Output out = KryoUtils.serialize(CdcKryoRegister.kryo(), this.currentState, CdcState.SERIALIZER))
        {
            return bridge().compressionUtil().compress(out.getBuffer());
        }
    }

    protected void persist(CdcState cdcState, TokenRange tokenRange)
    {
        if (!cdcOptions.persistState())
        {
            return;
        }

        try
        {
            final ByteBuffer buf = serializeStateToBytes();
            LOGGER.debug("Persisting Iterator state between micro-batch partitionId={} epoch={} size={}",
                         partitionId, cdcState.epoch, buf.remaining());
            statePersister.persist(jobId, partitionId, tokenRange, buf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // schema monitor

    public void scheduleMonitorSchema()
    {
        long delayMillis = cdcOptions.schemaRefreshDelay().toMillis();
        if (delayMillis > 0)
        {
            asyncExecutor.schedule(this::refreshSchema, delayMillis);
        }
    }

    protected void refreshSchema()
    {
        if (!isRunning.get())
        {
            return;
        }

        try
        {
            schemaSupplier
            .getCdcEnabledTables()
            .handle((tables, throwable) -> {
                if (throwable != null)
                {
                    LOGGER.warn("Error refreshing schema", throwable);
                    return null;
                }
                this.cdcEnabledTables = tables;
                if (tables == null || tables.isEmpty())
                {
                    LOGGER.warn("No CQL enabled tables");
                    return null;
                }

                // update Schema instance with latest schema
                cdcBridge().updateCdcSchema(tables, cdcOptions.partitioner(), tableIdLookup);
                return null;
            })
            .whenComplete((aVoid, throwable) -> {
                if (throwable != null)
                {
                    LOGGER.error("Unexpected error refreshing schema", throwable);
                }
                scheduleMonitorSchema();
            });
        }
        catch (Exception e)
        {
            LOGGER.error("Unexpected error refreshing schema", e);
            scheduleMonitorSchema();
        }
    }

    // Closable

    @Override
    public void close()
    {
        this.stop();
    }
}
