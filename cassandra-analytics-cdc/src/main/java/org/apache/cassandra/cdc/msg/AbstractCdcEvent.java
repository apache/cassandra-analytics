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

package org.apache.cassandra.cdc.msg;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.reader.ComplexTypeBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.ByteBufferUtils.split;
import static org.apache.cassandra.spark.utils.ByteBufferUtils.toHexString;

/**
 * Cassandra version ignorant abstraction of CdcEvent
 *
 * @param <V> AbstractValue type
 * @param <T> AbstractRangeTombstone type
 */
public abstract class AbstractCdcEvent<V extends AbstractValue, T extends AbstractRangeTombstone<V>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCdcEvent.class);

    TableMetadata tableMetadata;
    List<V> partitionKeys = null;
    List<V> clusteringKeys = null;
    List<V> staticColumns = null;
    List<V> valueColumns = null;

    // The max timestamp of the cells in the event
    protected long maxTimestampMicros = Long.MIN_VALUE;
    // Records the ttl info of the event
    TimeToLive timeToLive;
    // Records the tombstoned elements/cells in a complex data
    Map<String, List<ByteBuffer>> tombstonedCellsInComplex = null;
    // Records the range tombstone markers with in the same partition
    List<T> rangeTombstoneList = null;
    AbstractRangeTombstoneBuilder<V, T> rangeTombstoneBuilder = null;
    public final boolean track;
    public final String trackingId;

    public final String keyspace;
    public final String table;
    protected Kind kind;
    protected final CassandraSource cassandraSource;

    public AbstractCdcEvent(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
    {
        this(kind, partition.metadata().keyspace, partition.metadata().name, trackingId, cassandraSource);
        this.tableMetadata = partition.metadata();
        setPartitionKeys(partition);
        setStaticColumns(partition);
    }

    protected AbstractCdcEvent(Kind kind, String keyspace, String table, String trackingId, CassandraSource cassandraSource)
    {
        this.kind = kind;
        this.keyspace = keyspace;
        this.table = table;
        this.tableMetadata = null;
        this.trackingId = trackingId;
        this.track = trackingId != null;
        this.cassandraSource = cassandraSource;
    }

    void setPartitionKeys(UnfilteredRowIterator partition)
    {
        if (kind == Kind.PARTITION_DELETE)
        {
            updateMaxTimestamp(partition.partitionLevelDeletion().markedForDeleteAt());
        }

        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().partitionKeyColumns();
        List<V> pk = new ArrayList<>(columnMetadatas.size());

        ByteBuffer pkbb = partition.partitionKey().getKey();
        // single partition key
        if (columnMetadatas.size() == 1)
        {
            pk.add(makeValue(pkbb, columnMetadatas.get(0)));
        }
        else // composite partition key
        {
            ByteBuffer[] pkbbs = split(pkbb, columnMetadatas.size());
            for (int i = 0; i < columnMetadatas.size(); i++)
            {
                pk.add(makeValue(pkbbs[i], columnMetadatas.get(i)));
            }
        }
        partitionKeys = copyList(pk);
    }

    void setStaticColumns(UnfilteredRowIterator partition)
    {
        Row staticRow = partition.staticRow();

        if (staticRow.isEmpty())
        {
            return;
        }

        List<V> sc = new ArrayList<>(staticRow.columnCount());
        for (ColumnData cd : staticRow)
        {
            addColumn(sc, cd);
        }
        staticColumns = copyList(sc);
    }

    void setClusteringKeys(Unfiltered unfiltered, UnfilteredRowIterator partition)
    {
        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().clusteringColumns();
        if (columnMetadatas.isEmpty()) // the table has no clustering keys
        {
            return;
        }

        List<V> ck = new ArrayList<>(columnMetadatas.size());
        for (ColumnMetadata cm : columnMetadatas)
        {
            ByteBuffer ckbb = unfiltered.clustering().bufferAt(cm.position());
            ck.add(makeValue(ckbb, cm));
        }
        clusteringKeys = copyList(ck);
    }

    void setValueColumns(Row row)
    {
        if (kind == Kind.ROW_DELETE)
        {
            updateMaxTimestamp(row.deletion().time().markedForDeleteAt());
            return;
        }

        // Just a sanity check. An empty row will not be added to the PartitionUpdate/cdc, so not really expect the case
        if (row.isEmpty())
        {
            LOGGER.warn("Encountered an unexpected empty row in CDC. keyspace={}, table={}", keyspace, table);
            return;
        }

        List<V> vc = new ArrayList<>(row.columnCount());
        for (ColumnData cd : row)
        {
            addColumn(vc, cd);
        }
        valueColumns = copyList(vc);
    }

    private void addColumn(List<V> holder, ColumnData cd)
    {
        ColumnMetadata columnMetadata = cd.column();
        String columnName = columnMetadata.name.toCQLString();
        if (columnMetadata.isComplex()) // multi-cell column
        {
            ComplexColumnData complex = (ComplexColumnData) cd;
            DeletionTime deletionTime = complex.complexDeletion();
            if (deletionTime.isLive())
            {
                // the complex data is live, but there could be element deletion inside.
                if (complex.column().type instanceof ListType)
                {
                    // In the case of unfrozen lists, it reads the value from C*
                    readFromCassandra(holder, complex);
                }
                else
                {
                    processComplexData(holder, complex);
                }
            }
            else if (complex.cellsCount() > 0)
            {
                // The condition, complex data is not live && cellCount > 0, indicates that a new value is set to the column.
                // The CQL operation could be either insert or update the column.
                // Since the new value is in the mutation already, reading from C* can be skipped
                processComplexData(holder, complex);
            }
            else // the entire multi-cell collection/UDT is deleted.
            {
                kind = Kind.DELETE;
                updateMaxTimestamp(deletionTime.markedForDeleteAt());
                holder.add(makeValue(null, complex.column()));
            }
        }
        else // simple column
        {
            Cell<?> cell = (Cell<?>) cd;
            updateMaxTimestamp(cell.timestamp());
            if (cell.isTombstone())
            {
                holder.add(makeValue(null, cell.column()));
            }
            else
            {
                holder.add(makeValue(cell.buffer(), cell.column()));
                if (cell.isExpiring())
                {
                    setTTL(cell.ttl(), cell.localDeletionTime());
                }
            }
        }
    }

    private void processComplexData(List<V> holder, ComplexColumnData complex)
    {
        ComplexTypeBuffer buffer = ComplexTypeBuffer.newBuffer(complex.column().type, complex.cellsCount());
        boolean allTombstone = true;
        String columnName = complex.column().name.toCQLString();
        for (Cell<?> cell : complex)
        {
            updateMaxTimestamp(cell.timestamp());
            if (cell.isTombstone())
            {
                kind = Kind.COMPLEX_ELEMENT_DELETE;

                CellPath path = cell.path();
                if (path.size() > 0) // size can either be 0 (EmptyCellPath) or 1 (SingleItemCellPath).
                {
                    addCellTombstoneInComplex(columnName, path.get(0));
                }
            }
            else // cell is alive
            {
                allTombstone = false;
                buffer.addCell(cell);
                if (cell.isExpiring())
                {
                    setTTL(cell.ttl(), cell.localDeletionTime());
                }
            }
        }

        // Multi-cell data types are collections and user defined type (UDT).
        // Update to collections does not mix additions with deletions, since updating with 'null' is rejected.
        // However, UDT permits setting 'null' value. It is possible to see tombstone and modification together
        // from the update to UDT
        if (allTombstone)
        {
            holder.add(makeValue(null, complex.column()));
        }
        else
        {
            holder.add(makeValue(buffer.pack(), complex.column()));
        }
    }

    private void readFromCassandra(List<V> holder, ComplexColumnData complex)
    {
        updateMaxTimestamp(complex.maxTimestamp());
        List<AbstractValue> primaryKeyColumns = new ArrayList<>(getPrimaryKeyColumns());
        String columnName = complex.column().name.toCQLString();
        List<ByteBuffer> valueRead = cassandraSource.readFromCassandra(keyspace, table, ImmutableList.of(columnName), primaryKeyColumns);
        if (valueRead == null)
        {
            LOGGER.warn("Unable to process element update inside a List type. Skipping...");
        }
        else
        {
            // Only one column is read from cassandra, valueRead.get(0) should give the value of that
            // column.
            holder.add(makeValue(valueRead.get(0), complex.column()));
        }
    }

    private List<V> getPrimaryKeyColumns()
    {
        if (clusteringKeys == null)
        {
            return getPartitionKeys();
        }
        else
        {
            List<V> primaryKeys = new ArrayList<>(partitionKeys.size() + clusteringKeys.size());
            primaryKeys.addAll(partitionKeys);
            primaryKeys.addAll(clusteringKeys);
            return primaryKeys;
        }
    }

    private void setTTL(int ttlInSec, int expirationTimeInSec)
    {
        // Skip updating TTL if it already has been set.
        // For the same row, the upsert query can only set one TTL value.
        if (timeToLive != null)
        {
            return;
        }

        timeToLive = new TimeToLive(ttlInSec, expirationTimeInSec);
    }

    public void validateRangeTombstoneMarkers()
    {
        if (rangeTombstoneList == null)
        {
            return;
        }

        Preconditions.checkState(!rangeTombstoneBuilder.hasIncompleteRange(),
                                 "The last range tombstone is not closed");
    }

    // adds the serialized cellpath to the tombstone
    private void addCellTombstoneInComplex(String columnName, ByteBuffer key)
    {
        if (tombstonedCellsInComplex == null)
        {
            tombstonedCellsInComplex = new HashMap<>();
        }
        List<ByteBuffer> tombstones = tombstonedCellsInComplex.computeIfAbsent(columnName, k -> new ArrayList<>());
        tombstones.add(key);
    }

    public abstract static class EventBuilder<ValueType extends AbstractValue,
                                             TombstoneType extends AbstractRangeTombstone<ValueType>,
                                             EventType extends AbstractCdcEvent<ValueType, TombstoneType>>
    {
        protected EventType event = null;
        protected UnfilteredRowIterator partition = null;
        protected final CassandraSource cassandraSource;

        protected EventBuilder(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
        {
            this.cassandraSource = cassandraSource;
            if (partition == null)
            {
                // creating an EMPTY builder
                return;
            }
            this.event = buildEvent(kind, partition, trackingId, cassandraSource);
            this.partition = partition;
        }

        public abstract EventType buildEvent(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource);

        public EventBuilder<ValueType, TombstoneType, EventType> withRow(org.apache.cassandra.db.rows.Row row)
        {
            ensureBuilderNonempty();
            event.setClusteringKeys(row, partition);
            event.setValueColumns(row);
            return this;
        }

        public EventBuilder<ValueType, TombstoneType, EventType> addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            ensureBuilderNonempty();
            event.addRangeTombstoneMarker(marker);
            return this;
        }

        protected boolean isEmptyBuilder()
        {
            return event == null || partition == null;
        }

        protected void ensureBuilderNonempty()
        {
            Preconditions.checkState(!isEmptyBuilder(), "Cannot build with an empty builder.");
        }

        public EventType build()
        {
            ensureBuilderNonempty();
            event.validateRangeTombstoneMarkers();
            EventType res = event;
            event = null;
            partition = null;
            return res;
        }
    }

    public abstract AbstractRangeTombstoneBuilder<V, T> rangeTombstoneBuilder(TableMetadata metadata);

    void addRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        if (rangeTombstoneList == null)
        {
            rangeTombstoneList = new ArrayList<>();
            rangeTombstoneBuilder = rangeTombstoneBuilder(tableMetadata);
        }

        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker boundaryMarker = (RangeTombstoneBoundaryMarker) marker;
            updateMaxTimestamp(boundaryMarker.startDeletionTime().markedForDeleteAt());
            updateMaxTimestamp(boundaryMarker.endDeletionTime().markedForDeleteAt());
        }
        else
        {
            updateMaxTimestamp(((RangeTombstoneBoundMarker) marker).deletionTime().markedForDeleteAt());
        }

        rangeTombstoneBuilder.add(marker);

        if (rangeTombstoneBuilder.canBuild())
        {
            rangeTombstoneList.add(rangeTombstoneBuilder.build());
        }
    }

    public V makeValue(ByteBuffer value, ColumnMetadata columnMetadata)
    {
        return makeValue(columnMetadata.ksName,
                         columnMetadata.name.toCQLString(),
                         columnMetadata.type.asCQL3Type().toString(),
                         value);
    }

    public abstract V makeValue(String keyspace, String name, String type, ByteBuffer value);

    // Update the maxTimestamp if the input `timestamp` is larger.
    private void updateMaxTimestamp(long timestamp)
    {
        maxTimestampMicros = Math.max(maxTimestampMicros, timestamp);
    }

    /**
     * @return the kind of the cdc event.
     */
    public Kind getKind()
    {
        return kind;
    }

    /**
     * @return the timestamp of the cdc event in {@link TimeUnit}
     */
    public long getTimestamp(TimeUnit timeUnit)
    {
        return timeUnit.convert(maxTimestampMicros, TimeUnit.MICROSECONDS);
    }

    /**
     * @return the partition keys. The returned list must not be null and empty.
     */
    @NotNull
    public List<V> getPartitionKeys()
    {
        return partitionKeys;
    }

    /**
     * @return the clustering keys. The returned list could be null if the mutation carries no clustering keys.
     */
    @Nullable
    public List<V> getClusteringKeys()
    {
        return clusteringKeys;
    }

    /**
     * @return the static columns. The returned list could be null if the mutation carries no static columns.
     */
    @Nullable
    public List<V> getStaticColumns()
    {
        return staticColumns;
    }

    /**
     * @return the value columns. The returned list could be null if the mutation carries no value columns.
     */
    @Nullable
    public List<V> getValueColumns()
    {
        return valueColumns;
    }

    /**
     * The map returned contains the list of deleted keys (i.e. cellpath in Cassandra's terminology) of each affected
     * complext column. A complex column could be an unfrozen map, set and udt in Cassandra.
     *
     * @return the tombstoned cells in the complex data columns. The returned map could be null if the mutation does not
     * delete elements from complex.
     */
    @Nullable
    public Map<String, List<ByteBuffer>> getTombstonedCellsInComplex()
    {
        if (tombstonedCellsInComplex == null)
        {
            return null;
        }

        return new HashMap<>(tombstonedCellsInComplex);
    }

    /**
     * @return the range tombstone list. The returned list could be null if the mutation is not a range deletin.
     */
    @Nullable
    public List<T> getRangeTombstoneList()
    {
        return rangeTombstoneList;
    }

    /**
     * @return the time to live. The returned value could be null if the mutation carries no such value.
     */
    @Nullable
    public TimeToLive getTtl()
    {
        return timeToLive;
    }

    public enum Kind
    {
        INSERT,
        UPDATE,
        DELETE,
        PARTITION_DELETE,
        ROW_DELETE,
        RANGE_DELETE,
        COMPLEX_ELEMENT_DELETE,
    }

    public static class TimeToLive
    {
        public final int ttlInSec;
        public final int expirationTimeInSec;

        public TimeToLive(int ttlInSec, int expirationTimeInSec)
        {
            this.ttlInSec = ttlInSec;
            this.expirationTimeInSec = expirationTimeInSec;
        }
    }

    @Nullable
    public static <T> List<T> copyList(List<T> input)
    {
        if (input == null)
        {
            return null;
        }
        return ImmutableList.copyOf(input);
    }

    // convert primary keys into a hex string - useful in the tests to match expected with actual rows.
    public String getHexKey()
    {
        StringBuilder str = new StringBuilder();
        for (V column : getPrimaryKeyColumns())
        {
            str.append(toHexString(column.getValue())).append(':');
        }
        return str.toString();
    }
}
