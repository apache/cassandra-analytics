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

package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Return a set of SSTables for a single Cassandra Instance
 */
public class SingleReplica extends SSTablesSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleReplica.class);

    private final PartitionedDataLayer dataLayer;
    private final CassandraInstance instance;
    private final Range<BigInteger> range;
    private final int partitionId;
    private final ExecutorService executor;
    private final Stats stats;
    private boolean isRepairPrimary;

    public SingleReplica(@NotNull CassandraInstance instance,
                         @NotNull PartitionedDataLayer dataLayer,
                         @NotNull Range<BigInteger> range,
                         int partitionId,
                         @NotNull ExecutorService executor,
                         boolean isRepairPrimary)
    {
        this(instance, dataLayer, range, partitionId, executor, Stats.DoNothingStats.INSTANCE, isRepairPrimary);
    }

    public SingleReplica(@NotNull CassandraInstance instance,
                         @NotNull PartitionedDataLayer dataLayer,
                         @NotNull Range<BigInteger> range,
                         int partitionId,
                         @NotNull ExecutorService executor,
                         @NotNull Stats stats,
                         boolean isRepairPrimary)
    {
        this.dataLayer = dataLayer;
        this.instance = instance;
        this.range = range;
        this.partitionId = partitionId;
        this.executor = executor;
        this.stats = stats;
        this.isRepairPrimary = isRepairPrimary;
    }

    public CassandraInstance instance()
    {
        return instance;
    }

    public Range<BigInteger> range()
    {
        return range;
    }

    public boolean isRepairPrimary()
    {
        return isRepairPrimary;
    }

    public void setIsRepairPrimary(boolean isRepairPrimary)
    {
        this.isRepairPrimary = isRepairPrimary;
    }

    /**
     * Open all SparkSSTableReaders for all SSTables for this replica
     *
     * @param readerOpener provides function to open SparkSSTableReader using SSTable
     * @return set of SparkSSTableReader to pass over to the CompactionIterator
     */
    @Override
    public <T extends SparkSSTableReader> Set<T> openAll(ReaderOpener<T> readerOpener)
    {
        try
        {
            return openReplicaAsync(readerOpener).get();
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
        catch (ExecutionException exception)
        {
            throw new RuntimeException(ThrowableUtils.rootCause(exception));
        }
    }

    <T extends SparkSSTableReader> CompletableFuture<Set<T>> openReplicaAsync(@NotNull ReaderOpener<T> readerOpener)
    {
        // List SSTables and open SSTable readers
        try
        {
            long timeNanos = System.nanoTime();
            return dataLayer.listInstance(partitionId, range, instance)
                            .thenApply(stream -> {
                                stats.timeToListSnapshot(this, System.nanoTime() - timeNanos);
                                return stream;
                            })
                            .thenCompose(stream -> openAll(stream, readerOpener));
        }
        catch (Throwable throwable)
        {
            LOGGER.warn("Unexpected error attempting to open SSTable readers for replica node={} token={} dataCenter={}",
                        instance().nodeName(), instance().token(), instance().dataCenter(), throwable);
            CompletableFuture<Set<T>> exceptionally = new CompletableFuture<>();
            exceptionally.completeExceptionally(throwable);
            return exceptionally;
        }
    }

    private <T extends SparkSSTableReader> CompletableFuture<Set<T>> openAll(@NotNull Stream<SSTable> stream,
                                                                             @NotNull ReaderOpener<T> readerOpener)
    {
        Set<T> result = ConcurrentHashMap.newKeySet();
        CompletableFuture[] futures = stream
                // Verify all the required SSTable file components are available
                .peek(SSTable::verify)
                // Open SSTable readers in parallel using executor
                .map(ssTable -> CompletableFuture.runAsync(() -> openReader(readerOpener, ssTable, result), executor))
                .toArray(CompletableFuture[]::new);

        // All futures must complete non-exceptionally for the resulting future to complete
        return CompletableFuture.allOf(futures).thenApply(aVoid -> ImmutableSet.copyOf(result));
    }

    private <T extends SparkSSTableReader> void openReader(@NotNull ReaderOpener<T> readerOpener,
                                                           @NotNull SSTable ssTable,
                                                           @NotNull Set<T> result)
    {
        try
        {
            T reader = readerOpener.openReader(ssTable, isRepairPrimary);
            if (!reader.ignore())
            {
                result.add(reader);
            }
        }
        catch (IOException exception)
        {
            throw new SSTableStreamException(exception);
        }
    }
}
