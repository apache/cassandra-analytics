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

package org.apache.cassandra.cdc.scanner;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cdc.msg.AbstractCdcEvent;
import org.apache.cassandra.cdc.msg.AbstractRangeTombstone;
import org.apache.cassandra.cdc.msg.AbstractValue;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.jetbrains.annotations.NotNull;

/**
 * A scanner that is backed by a sorted collection of {@link PartitionUpdateWrapper}.
 * Not thread safe. (And not a stream).
 *
 * @param <V> AbstractValue type
 * @param <T> AbstractRangeTombstone type
 * @param <E> AbstractCdcEvent type
 */
@NotThreadSafe
public abstract class AbstractCdcSortedStreamScanner<V extends AbstractValue,
                                                    T extends AbstractRangeTombstone<V>,
                                                    E extends AbstractCdcEvent<V, T>> implements StreamScanner<E>
{
    final Queue<PartitionUpdateWrapper> updates;
    private final UnfilteredPartitionIterator partitionIterator;

    private UnfilteredRowIterator currentPartition = null;
    private E event;
    protected AbstractCdcEvent.EventBuilder<V, T, E> rangeDeletionBuilder;
    private final Random random;
    private final CdcState endState;

    protected AbstractCdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates,
                                             @NotNull CdcState endState)
    {
        this(updates, endState, ThreadLocalRandom.current());
    }

    protected AbstractCdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates,
                                             @NotNull CdcState endState,
                                             @NotNull Random random)
    {
        this.updates = new PriorityQueue<>(PartitionUpdateWrapper::compareTo);
        this.updates.addAll(updates);
        this.partitionIterator = new HybridUnfilteredPartitionIterator<>(this);
        this.endState = endState;
        this.random = random;
    }

    public CdcState endState()
    {
        return this.endState;
    }

    @Override
    public E data()
    {
        Preconditions.checkState(event != null,
                                 "No data available. Make sure hasNext is called before this method!");
        E data = event;
        event = null; // reset to null
        return data;
    }

    /**
     * Prepare the {@link #data()} to be fetched.
     * Briefly, the procedure is to iterate through the rows/rangetombstones in each partition.
     * A CdcEvent is produced for each row.
     * For range tombstones, a CdcEvent is produced for all markers/bounds combined within the same partition.
     * Caller must call this method before calling {@link #data()}.
     *
     * @return true if there are more data; otherwise, false.
     */
    @Override
    public boolean next()
    {
        while (true)
        {
            if (allExhausted())
            {
                return false;
            }
            // sampling CDC events for tracking
            String trackingId = null;
            if (random.nextDouble() < samplingRate())
            {
                trackingId = UUID.randomUUID().toString();
            }

            if (currentPartition == null)
            {
                currentPartition = partitionIterator.next();

                // it is a Cassandra partition deletion
                if (!currentPartition.partitionLevelDeletion().isLive())
                {
                    event = makePartitionTombstone(currentPartition, trackingId);
                    currentPartition = null;
                    return true;
                }

                // the partition contains no other rows but only a static row
                Row staticRow = currentPartition.staticRow();
                if (!currentPartition.hasNext() && staticRow != null && staticRow != Rows.EMPTY_STATIC_ROW)
                {
                    event = makeStaticRow(staticRow, currentPartition, trackingId);
                    currentPartition = null; // reset
                    return true;
                }
            }

            if (!currentPartition.hasNext())
            {
                // The current partition is exhausted. Clean up and advance to the next partition by `continue`.
                currentPartition = null; // reset
                // Publish any range deletion for the partition
                if (rangeDeletionBuilder != null)
                {
                    event = rangeDeletionBuilder.build();
                    rangeDeletionBuilder = null; // reset
                    return true;
                }
                else
                {
                    continue;
                }
            }

            // An unfiltered can either be a Row or RangeTombstoneMarker
            Unfiltered unfiltered = currentPartition.next();

            if (unfiltered.isRow())
            {
                Row row = (Row) unfiltered;
                event = makeRow(row, currentPartition, trackingId);
                return true;
            }
            else if (unfiltered.isRangeTombstoneMarker())
            {
                // Range tombstone can get complicated.
                // - In the most simple case, that is a DELETE statement with a single clustering key range, we expect
                //   the UnfilteredRowIterator with 2 markers, i.e. open and close range tombstone markers
                // - In a slightly more complicated case, it contains IN operator (on prior clustering keys), we expect
                //   the UnfilteredRowIterator with 2 * N markers, where N is the number of values specified for IN.
                // - In the most complicated case, client could comopse a complex partition update with a BATCH statement.
                //   It could have those further scenarios: (only discussing the statements applying to the same partition key)
                //   - Multiple disjoint ranges => we should expect 2 * N markers, where N is the number of ranges.
                //   - Overlapping ranges with the same timestamp => we should expect 2 markers, considering the
                //     overlapping ranges are merged into a single one. (as the boundary is omitted)
                //   - Overlapping ranges with different timestamp ==> we should expect 3 markers, i.e. open bound,
                //     boundary and end bound
                //   - Ranges mixed with INSERT! => The order of the unfiltered (i.e. Row/RangeTombstoneMarker) is determined
                //     by comparing the row clustering with the bounds of the ranges. See o.a.c.d.r.RowAndDeletionMergeIterator
                RangeTombstoneMarker rangeTombstoneMarker = (RangeTombstoneMarker) unfiltered;
                // We encode the ranges within the same spark row. Therefore, it needs to keep the markers when
                // iterating through the partition, and _only_ generate a spark row with range tombstone info when
                // exhausting the partition / UnfilteredRowIterator.
                handleRangeTombstone(rangeTombstoneMarker, currentPartition, trackingId);
                // continue to consume the next unfiltered row/marker
            }
            else
            {
                // As of Cassandra 4, the unfiltered kind can either be row or range tombstone marker, see o.a.c.db.rows.Unfiltered.Kind
                // Having the else branch only for completeness.
                throw new IllegalStateException("Encountered unknown Unfiltered kind.");
            }
        }
    }

    @Override
    public void advanceToNextColumn()
    {
        throw new UnsupportedOperationException("not implemented!");
    }

    private boolean allExhausted()
    {
        return !partitionIterator.hasNext() // no next partition
               && currentPartition == null // current partition has exhausted
               && rangeDeletionBuilder == null; // no range deletion being built
    }


    @Override
    public void close()
    {
        updates.clear();
    }

    public E makeRow(Row row, UnfilteredRowIterator partition, String trackingId)
    {
        return makeRow(row, partition, false, trackingId);
    }

    private E makeStaticRow(Row row, UnfilteredRowIterator partition, String trackingId)
    {
        return makeRow(row, partition, true, trackingId);
    }

    private E makeRow(Row row, UnfilteredRowIterator partition, boolean isStaticOnly, String trackingId)
    {
        // It is a Cassandra row deletion
        if (!row.deletion().isLive())
        {
            return buildRowDelete(row, partition, trackingId);
        }

        // Empty primaryKeyLivenessInfo == update; non-empty == insert
        // The cql row could also be a deletion kind.
        // Here, it only _assumes_ UPDATE/INSERT, and the kind is updated accordingly on build.
        if (row.primaryKeyLivenessInfo().isEmpty())
        {
            return buildUpdate(row, partition, isStaticOnly, trackingId);
        }

        return buildInsert(row, partition, isStaticOnly, trackingId);
    }

    /* Rate for sampling CDC events [0.0..1.0]*/
    public Double samplingRate()
    {
        return 0.0;
    }

    public abstract E buildRowDelete(Row row, UnfilteredRowIterator partition, String trackingId);

    public abstract E buildUpdate(Row row, UnfilteredRowIterator partition, boolean isStaticOnly, String trackingId);

    public abstract E buildInsert(Row row, UnfilteredRowIterator partition, boolean isStaticOnly, String trackingId);

    public abstract E makePartitionTombstone(UnfilteredRowIterator partition, String trackingId);

    public abstract void handleRangeTombstone(RangeTombstoneMarker marker, UnfilteredRowIterator partition, String trackingId);
}
