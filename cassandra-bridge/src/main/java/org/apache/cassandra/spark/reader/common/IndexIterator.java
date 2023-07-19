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

package org.apache.cassandra.spark.reader.common;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.stats.Stats;
import org.jetbrains.annotations.NotNull;

/**
 * Iterator for reading through IndexEntries for multiple Index.db files.
 *
 * @param <ReaderType>
 */
public class IndexIterator<ReaderType extends IIndexReader> implements StreamScanner<IndexEntry>, IndexConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexIterator.class);

    private final AtomicInteger finished = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final Set<ReaderType> readers;
    private final LinkedBlockingQueue<IndexEntry> queue = new LinkedBlockingQueue<>();
    private final long startTimeNanos;
    private final Stats stats;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private IndexEntry curr = null;

    public IndexIterator(@NotNull SSTablesSupplier ssTables,
                         @NotNull Stats stats,
                         @NotNull IndexReaderOpener<ReaderType> supplier)
    {
        this.startTimeNanos = System.nanoTime();
        this.stats = stats;
        this.readers = ssTables.openAll((ssTable, isRepairPrimary) -> supplier.openReader(ssTable, isRepairPrimary, this));
        stats.openedIndexFiles(System.nanoTime() - startTimeNanos);
    }

    public interface IndexReaderOpener<ReaderType extends IIndexReader>
    {
        ReaderType openReader(SSTable ssTable, boolean isRepairPrimary, IndexConsumer consumer) throws IOException;
    }

    public void accept(IndexEntry wrapper)
    {
        if (closed.get())
        {
            return;
        }

        try
        {
            queue.put(wrapper);
            stats.indexEntryConsumed();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public IndexEntry rid()
    {
        return Objects.requireNonNull(curr, "advanceToNextColumn() must be called before data()");
    }

    public boolean hasNext() throws IOException
    {
        return isRunning() && noFailures() && (hasPendingItems() || notFinished());
    }

    protected boolean isRunning()
    {
        return !closed.get();
    }

    protected boolean hasPendingItems()
    {
        return !queue.isEmpty();
    }

    protected boolean noFailures()
    {
        return !maybeFail();
    }

    protected boolean maybeFail()
    {
        Throwable t = failure.get();
        if (t != null)
        {
            throw new RuntimeException(t);
        }
        return false;
    }

    private boolean notFinished()
    {
        return !isFinished();
    }

    private boolean isFinished()
    {
        return finished.get() == readers.size();
    }

    public void advanceToNextColumn()
    {
        if (closed.get())
        {
            throw new IllegalStateException("Iterator closed");
        }

        try
        {
            long startTimeNanos = System.nanoTime();
            this.curr = queue.take();
            stats.indexIteratorTimeBlocked(System.nanoTime() - startTimeNanos);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        if (!this.closed.get() && this.closed.compareAndSet(false, true))
        {
            queue.clear();
            stats.closedIndexIterator(System.nanoTime() - startTimeNanos);
        }
    }

    public void onFailure(Throwable t)
    {
        LOGGER.warn("IndexReader failed with exception", t);
        stats.indexReaderFailure(t);
        if (failure.get() == null)
        {
            failure.compareAndSet(null, t);
        }
    }

    public void onFinished(long runtimeNanos)
    {
        stats.indexReaderFinished(runtimeNanos);
        finished.incrementAndGet();
    }
}
