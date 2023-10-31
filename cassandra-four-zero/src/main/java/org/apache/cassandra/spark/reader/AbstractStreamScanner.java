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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractStreamScanner implements StreamScanner<Rid>, Closeable
{
    // All partitions in the SSTable
    private UnfilteredPartitionIterator allPartitions;
    // A single partition, containing rows and/or range tombstones
    private UnfilteredRowIterator partition;
    // The static row of the current partition, which may be empty
    @SuppressWarnings("FieldCanBeLocal")
    private Row staticRow;
    // Current atom (row or range tombstone) being processed
    private Unfiltered unfiltered;
    // If processing a row this holds the state of iterating that row
    private Iterator<ColumnData> columns;
    // State of processing data for a single column in a row (which may be multi-celled in the case of complex columns)
    protected ColumnDataState columnData;

    @NotNull
    final TableMetadata metadata;

    @NotNull
    protected final TimeProvider timeProvider;

    protected final Rid rid = new Rid();

    AbstractStreamScanner(@NotNull TableMetadata metadata,
                          @NotNull Partitioner partitionerType,
                          @NotNull TimeProvider timeProvider)
    {
        this.metadata = metadata.unbuild()
                                .partitioner(partitionerType == Partitioner.Murmur3Partitioner
                                        ? new Murmur3Partitioner()
                                        : new RandomPartitioner())
                                .build();
        this.timeProvider = timeProvider;

        // Counter tables are not supported
        if (metadata.isCounter())
        {
            throw new IllegalArgumentException(
                    String.format("Streaming reads of SSTables from counter tables are not supported, "
                                + "rejecting stream of data from %s.%s",
                                  metadata.keyspace, metadata.name));
        }
    }

    @Override
    public Rid rid()
    {
        return rid;
    }

    /* Abstract methods */

    abstract UnfilteredPartitionIterator initializePartitions();

    @Override
    public abstract void close() throws IOException;

    protected abstract void handleRowTombstone(Row row);

    protected abstract void handlePartitionTombstone(UnfilteredRowIterator partition);

    protected abstract void handleCellTombstone();

    protected abstract void handleCellTombstoneInComplex(Cell<?> cell);

    @Override
    public void advanceToNextColumn()
    {
        columnData.consume();
    }

    // CHECKSTYLE IGNORE: Long method
    @Override
    public boolean hasNext() throws IOException
    {
        if (allPartitions == null)
        {
            allPartitions = initializePartitions();
        }

        while (true)
        {
            if (partition == null)
            {
                try
                {
                    // We've exhausted the partition iterator
                    if (allPartitions.hasNext())
                    {
                        // Advance to next partition
                        partition = allPartitions.next();

                        if (partition.partitionLevelDeletion().isLive())
                        {
                            // Reset rid with new partition key
                            rid.setPartitionKeyCopy(partition.partitionKey().getKey(),
                                                    ReaderUtils.tokenToBigInteger(partition.partitionKey().getToken()));
                        }
                        else
                        {
                            // There's a partition-level delete
                            handlePartitionTombstone(partition);
                            return true;
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                catch (SSTableStreamException exception)
                {
                    throw exception.getIOException();
                }

                // If the partition has a non-empty static row, grab its columns,
                // so we process those before moving onto its atoms (the Unfiltered instances)
                staticRow = partition.staticRow();
                if (!staticRow.isEmpty())
                {
                    columns = staticRow.iterator();
                    prepareColumnData();
                    return true;
                }
            }

            // We may be in the midst of processing some multi-cell column data,
            // if so, we'll resume that where we left off
            if (columnData != null && columnData.hasData())
            {
                return true;
            }

            // Continue to process columns of the last read row, which may be static
            if (columns != null && columns.hasNext())
            {
                prepareColumnData();
                return true;
            }

            // Current row was exhausted (or none were present), so move to the next atom
            columns = null;
            try
            {
                // Advance to next unfiltered
                if (partition.hasNext())
                {
                    unfiltered = partition.next();
                }
                else
                {
                    // Current partition is exhausted
                    partition = null;
                    unfiltered = null;
                }
            }
            catch (SSTableStreamException exception)
            {
                throw exception.getIOException();
            }

            if (unfiltered != null)
            {
                if (unfiltered.isRow())
                {
                    Row row = (Row) unfiltered;

                    // There is a CQL row level delete
                    if (!row.deletion().isLive())
                    {
                        handleRowTombstone(row);
                        return true;
                    }

                    // For non-compact tables, set up a ClusteringColumnDataState to emit a Rid that emulates a
                    // pre-3.0 CQL row marker. This is necessary for backwards compatibility with 2.1 & 2.0 output,
                    // and also for tables with only primary key columns defined.
                    // An empty PKLI is the 3.0 equivalent of having no row marker (e.g. row modifications via
                    // UPDATE not INSERT) so we don't emit a fake row marker in that case.
                    if (!row.primaryKeyLivenessInfo().isEmpty())
                    {
                        if (TableMetadata.Flag.isCQLTable(metadata.flags))
                        {
                            columnData = new ClusteringColumnDataState(row.clustering());
                        }
                        columns = row.iterator();
                        return true;
                    }

                    // The row's actual columns may be empty, in which case we'll simply skip over them during the next
                    // iteration and move to the next unfiltered. So then only the row marker and/or row deletion (if
                    // either are present) will get emitted
                    columns = row.iterator();
                }
                else
                {
                    // As of Cassandra 4, the unfiltered kind can either be row or range tombstone marker,
                    // see o.a.c.db.rows.Unfiltered.Kind; having the else branch only for completeness
                    throw new IllegalStateException("Encountered unknown Unfiltered kind");
                }
            }
        }
    }

    /**
     * Prepare the columnData to be consumed the next
     */
    private void prepareColumnData()
    {
        ColumnData data = columns.next();
        if (data.column().isComplex())
        {
            columnData = new ComplexDataState(data.column().isStatic() ? Clustering.STATIC_CLUSTERING
                                                                       : unfiltered.clustering(),
                                              (ComplexColumnData) data);
        }
        else
        {
            columnData = new SimpleColumnDataState(data.column().isStatic() ? Clustering.STATIC_CLUSTERING
                                                                            : unfiltered.clustering(),
                                                   data);
        }
    }

    private interface ColumnDataState
    {
        /**
         * Indicate whether the column has data
         *
         * @return true if it has data to be consumed
         */
        boolean hasData();

        /**
         * Consume the data in the column
         */
        void consume();
    }

    /**
     * Maps clustering values to column data, to emulate CQL row markers which were removed in Cassandra 3.0,
     * but which we must still emit Rid for in order to preserve backwards compatibility
     * and to handle tables containing only primary key columns
     */
    protected final class ClusteringColumnDataState implements ColumnDataState
    {
        private boolean consumed = false;
        private final ClusteringPrefix clustering;

        ClusteringColumnDataState(ClusteringPrefix clustering)
        {
            this.clustering = clustering;
        }

        @Override
        public boolean hasData()
        {
            return !consumed;
        }

        @Override
        public void consume()
        {
            if (!consumed)
            {
                rid.setColumnNameCopy(ReaderUtils.encodeCellName(metadata,
                                                                 clustering,
                                                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                 null));
                rid.setValueCopy(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                consumed = true;
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Holds current processing state of any simple column data
     */
    private final class SimpleColumnDataState implements ColumnDataState
    {
        private ClusteringPrefix clustering;
        private final Cell cell;

        private SimpleColumnDataState(ClusteringPrefix clustering, ColumnData data)
        {
            Preconditions.checkArgument(data.column().isSimple(), "The type of the ColumnData should be simple");
            this.clustering = clustering;
            this.cell = (Cell) data;
        }

        @Override
        public boolean hasData()
        {
            return (clustering != null);
        }

        @Override
        public void consume()
        {
            boolean isStatic = cell.column().isStatic();
            rid.setColumnNameCopy(ReaderUtils.encodeCellName(metadata,
                                                             isStatic ? Clustering.STATIC_CLUSTERING : clustering,
                                                             cell.column().name.bytes,
                                                             null));
            if (cell.isTombstone())
            {
                handleCellTombstone();
            }
            else
            {
                rid.setValueCopy(cell.buffer());
            }
            rid.setTimestamp(cell.timestamp());
            // Null out clustering so hasData will return false
            clustering = null;
        }
    }

    /**
     * Holds current processing state of any complex column data
     */
    private final class ComplexDataState implements ColumnDataState
    {
        private final ColumnMetadata column;
        private ClusteringPrefix clustering;
        private final Iterator<Cell<?>> cells;
        private final int cellCount;
        private final DeletionTime deletionTime;

        private ComplexDataState(ClusteringPrefix clustering, ComplexColumnData data)
        {
            this.clustering = clustering;
            this.column = data.column();
            this.cells = data.iterator();
            this.cellCount = data.cellsCount();
            this.deletionTime = data.complexDeletion();
        }

        @Override
        public boolean hasData()
        {
            return clustering != null && cells.hasNext();
        }

        @Override
        public void consume()
        {
            rid.setColumnNameCopy(ReaderUtils.encodeCellName(metadata,
                                                             clustering,
                                                             column.name.bytes,
                                                             ByteBufferUtil.EMPTY_BYTE_BUFFER));
            // The complex data is live, but there could be element deletion inside; check for it later in the block
            if (deletionTime.isLive())
            {
                ComplexTypeBuffer buffer = ComplexTypeBuffer.newBuffer(column.type, cellCount);
                long maxTimestamp = Long.MIN_VALUE;
                while (cells.hasNext())
                {
                    Cell<?> cell = cells.next();
                    // Re: isLive vs. isTombstone - isLive considers TTL so that if a cell is expiring soon,
                    // it is handled as tombstone
                    if (cell.isLive(timeProvider.nowInTruncatedSeconds()))
                    {
                        buffer.addCell(cell);
                    }
                    else
                    {
                        handleCellTombstoneInComplex(cell);
                    }
                    // In the case the cell is deleted, the deletion time is also the cell's timestamp
                    maxTimestamp = Math.max(maxTimestamp, cell.timestamp());
                }

                rid.setValueCopy(buffer.build());
                rid.setTimestamp(maxTimestamp);
            }
            else
            {
                // The entire collection/UDT is deleted
                handleCellTombstone();
                rid.setTimestamp(deletionTime.markedForDeleteAt());
            }

            // Null out clustering to indicate no data
            clustering = null;
        }
    }

    private abstract static class ComplexTypeBuffer
    {
        private final List<ByteBuffer> buffers;
        private final int cellCount;
        private int length = 0;

        ComplexTypeBuffer(int cellCount, int bufferSize)
        {
            this.cellCount = cellCount;
            this.buffers = new ArrayList<>(bufferSize);
        }

        static ComplexTypeBuffer newBuffer(AbstractType<?> type, int cellCount)
        {
            ComplexTypeBuffer buffer;
            if (type instanceof SetType)
            {
                buffer = new SetBuffer(cellCount);
            }
            else if (type instanceof ListType)
            {
                buffer = new ListBuffer(cellCount);
            }
            else if (type instanceof MapType)
            {
                buffer = new MapBuffer(cellCount);
            }
            else if (type instanceof UserType)
            {
                buffer = new UdtBuffer(cellCount);
            }
            else
            {
                throw new IllegalStateException("Unexpected type deserializing CQL Collection: " + type);
            }
            return buffer;
        }

        void addCell(Cell cell)
        {
            add(cell.buffer());  // Copy over value
        }

        void add(ByteBuffer buffer)
        {
            buffers.add(buffer);
            length += buffer.remaining();
        }

        ByteBuffer build()
        {
            ByteBuffer result = ByteBuffer.allocate(4 + (buffers.size() * 4) + length);
            result.putInt(cellCount);
            for (ByteBuffer buffer : buffers)
            {
                result.putInt(buffer.remaining());
                result.put(buffer);
            }
            // Cast to ByteBuffer required when compiling with Java 8
            return (ByteBuffer) result.flip();
        }
    }

    private static class SetBuffer extends ComplexTypeBuffer
    {
        SetBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }

        @Override
        void addCell(Cell cell)
        {
            add(cell.path().get(0));  // Set - copy over key
        }
    }

    private static class ListBuffer extends ComplexTypeBuffer
    {
        ListBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }
    }

    private static class MapBuffer extends ComplexTypeBuffer
    {

        MapBuffer(int cellCount)
        {
            super(cellCount, cellCount * 2);
        }

        @Override
        void addCell(Cell cell)
        {
            add(cell.path().get(0));  // Map - copy over key and value
            super.addCell(cell);
        }
    }

    private static class UdtBuffer extends ComplexTypeBuffer
    {
        UdtBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }
    }
}
