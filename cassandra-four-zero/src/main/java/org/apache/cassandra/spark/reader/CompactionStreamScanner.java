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

package org.apache.cassandra.spark.reader;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.jetbrains.annotations.NotNull;

public class CompactionStreamScanner extends AbstractStreamScanner
{
    private final Collection<? extends Scannable> toCompact;
    private final UUID taskId;

    private PurgingCompactionController controller;
    private AbstractCompactionStrategy.ScannerList scanners;
    private CompactionIterator ci;

    CompactionStreamScanner(@NotNull TableMetadata cfMetaData,
                            @NotNull Partitioner partitionerType,
                            @NotNull Collection<? extends Scannable> toCompact)
    {
        this(cfMetaData, partitionerType, TimeProvider.INSTANCE, toCompact);
    }

    public CompactionStreamScanner(@NotNull TableMetadata cfMetaData,
                                   @NotNull Partitioner partitionerType,
                                   @NotNull TimeProvider timeProvider,
                                   @NotNull Collection<? extends Scannable> toCompact)
    {
        super(cfMetaData, partitionerType, timeProvider);
        this.toCompact = toCompact;
        this.taskId = UUID.randomUUID();
    }

    @Override
    public void close()
    {
        Arrays.asList(controller, scanners, ci)
              .forEach(IOUtils::closeQuietly);
    }

    @Override
    protected void handleRowTombstone(Row row)
    {
        throw new IllegalStateException("Row tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    protected void handlePartitionTombstone(UnfilteredRowIterator partition)
    {
        throw new IllegalStateException("Partition tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    protected void handleCellTombstone()
    {
        throw new IllegalStateException("Cell tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    protected void handleCellTombstoneInComplex(Cell<?> cell)
    {
        // Do nothing: to not introduce behavior change to the SBR code path
    }

    @Override
    protected void handleRangeTombstone(RangeTombstoneMarker marker)
    {
        throw new IllegalStateException("Range tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    UnfilteredPartitionIterator initializePartitions()
    {
        int nowInSec = timeProvider.nowInTruncatedSeconds();
        Keyspace keyspace = Keyspace.openWithoutSSTables(metadata.keyspace);
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(metadata.name);
        controller = new PurgingCompactionController(cfStore, CompactionParams.TombstoneOption.NONE);
        List<ISSTableScanner> scannerList = toCompact.stream()
                                                     .map(Scannable::scanner)
                                                     .collect(Collectors.toList());
        scanners = new AbstractCompactionStrategy.ScannerList(scannerList);
        ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, taskId);
        return ci;
    }

    private static class PurgingCompactionController extends AbstractCompactionController implements AutoCloseable
    {
        PurgingCompactionController(ColumnFamilyStore cfs, CompactionParams.TombstoneOption tombstoneOption)
        {
            super(cfs, Integer.MAX_VALUE, tombstoneOption);
        }

        @Override
        public boolean compactingRepaired()
        {
            return false;
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            // Purge all tombstones
            return time -> true;
        }

        @Override
        public void close()
        {
        }
    }
}
