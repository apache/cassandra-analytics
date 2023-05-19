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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.stats.Stats;
import org.jetbrains.annotations.NotNull;

/**
 * Return a set of SSTables for a token range, returning enough replica copies to satisfy consistency level
 */
public class MultipleReplicas extends SSTablesSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleReplicas.class);

    @NotNull
    private final Set<SingleReplica> primaryReplicas;
    @NotNull
    private final Set<SingleReplica> backupReplicas;
    @NotNull
    private final Stats stats;

    public MultipleReplicas(@NotNull Set<SingleReplica> primaryReplicas,
                            @NotNull Set<SingleReplica> backupReplicas,
                            @NotNull Stats stats)
    {
        this.primaryReplicas = ImmutableSet.copyOf(primaryReplicas);
        this.backupReplicas = ImmutableSet.copyOf(backupReplicas);
        this.stats = stats;
    }

    /**
     * Open SSTable readers for enough replicas to satisfy consistency level
     *
     * @param readerOpener open SparkSSTableReader for SSTable
     * @return set of SparkSSTableReaders to compact
     */
    @Override
    public <T extends SparkSSTableReader> Set<T> openAll(ReaderOpener<T> readerOpener)
    {
        if (primaryReplicas.isEmpty())
        {
            return Collections.emptySet();
        }

        long startTimeNanos = System.nanoTime();
        ConcurrentLinkedQueue<SingleReplica> otherReplicas = new ConcurrentLinkedQueue<>(backupReplicas);
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(primaryReplicas.size());
        Set<T> result = ConcurrentHashMap.newKeySet();
        // Open all primary replicas async
        for (SingleReplica primaryReplica : primaryReplicas)
        {
            openReplicaOrRetry(primaryReplica, readerOpener, result, count, latch, otherReplicas);
        }

        // Block until all replicas opened
        try
        {
            latch.await();
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }

        // Need to meet the required number of primary replicas to meet consistency level
        if (count.get() < primaryReplicas.size())
        {
            stats.notEnoughReplicas(primaryReplicas, backupReplicas);
            throw new NotEnoughReplicasException(String.format("Required %d replicas but only %d responded",
                                                               primaryReplicas.size(), count.get()));
        }

        stats.openedReplicas(primaryReplicas, backupReplicas, System.nanoTime() - startTimeNanos);
        return ImmutableSet.copyOf(result);
    }

    private <T extends SparkSSTableReader> void openReplicaOrRetry(
            @NotNull SingleReplica replica,
            @NotNull ReaderOpener<T> readerOpener,
            @NotNull Set<T> result,
            @NotNull AtomicInteger count,
            @NotNull CountDownLatch latch,
            @NotNull ConcurrentLinkedQueue<SingleReplica> otherReplicas)
    {
        replica.openReplicaAsync(readerOpener)
               .whenComplete((readers, throwable) -> {
                   if (throwable != null)
                   {
                       LOGGER.warn("Failed to open SSTableReaders for replica node={} token={} dataCenter={}",
                                   replica.instance().nodeName(), replica.instance().token(), replica.instance().dataCenter(), throwable);
                       stats.failedToOpenReplica(replica, throwable);
                       SingleReplica anotherReplica = otherReplicas.poll();
                       if (anotherReplica != null)
                       {
                           LOGGER.warn("Retrying on another replica node={} token={} dataCenter={}",
                                       anotherReplica.instance().nodeName(), anotherReplica.instance().token(), anotherReplica.instance().dataCenter());
                           // If the failed replica was the repair primary we need the backup replacement replica to be the new repair primary
                           anotherReplica.setIsRepairPrimary(replica.isRepairPrimary());
                           openReplicaOrRetry(anotherReplica, readerOpener, result, count, latch, otherReplicas);
                       }
                       else
                       // No more replicas to retry so end
                       {
                           latch.countDown();
                       }
                       return;
                   }

                   try
                   {
                       // Successfully opened all SSTable readers
                       result.addAll(readers);
                       count.incrementAndGet();
                   }
                   finally
                   {
                       latch.countDown();
                   }
               });
    }
}
