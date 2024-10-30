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
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.spark.utils.ByteBufferUtils.split;

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

public class FourZeroCdcEventBuilder extends CdcEventBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FourZeroCdcEventBuilder.class);

    private TableMetadata tableMetadata;
    private UnfilteredRowIterator partition = null;

    FourZeroCdcEventBuilder(CdcEvent.Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
    {
        this(kind, partition.metadata().keyspace, partition.metadata().name, trackingId, cassandraSource);
        this.tableMetadata = partition.metadata();
        this.partition = partition;
        setPartitionKeys(partition);
        setStaticColumns(partition);
    }

    FourZeroCdcEventBuilder(CdcEvent.Kind kind, String keyspace, String table, String trackingId, CassandraSource cassandraSource)
    {
        super(kind, keyspace, table, trackingId, cassandraSource);
        this.kind = kind;
        this.keyspace = keyspace;
        this.table = table;
        this.trackingId = trackingId;
        this.track = trackingId != null;
        this.cassandraSource = cassandraSource;
    }

    public static FourZeroCdcEventBuilder of(CdcEvent.Kind kind,
                                             UnfilteredRowIterator partition,
                                             String trackingId,
                                             CassandraSource cassandraSource)
    {
        return new FourZeroCdcEventBuilder(kind, partition, trackingId, cassandraSource);
    }

    public static CdcEvent build(CdcEvent.Kind kind,
                                 UnfilteredRowIterator partition,
                                 String trackingId,
                                 CassandraSource cassandraSource)
    {
        return of(kind, partition, trackingId, cassandraSource).build();
    }

    public static CdcEvent build(CdcEvent.Kind kind,
                                 UnfilteredRowIterator partition,
                                 Row row,
                                 String trackingId,
                                 CassandraSource cassandraSource)
    {
        return of(kind, partition, trackingId, cassandraSource)
               .withRow(row)
               .build();
    }

    public FourZeroCdcEventBuilder withRow(Row row)
    {
        Preconditions.checkNotNull(partition, "Cannot build with an empty builder.");
        setClusteringKeys(row, partition);
        setValueColumns(row);
        return this;
    }

    void setPartitionKeys(UnfilteredRowIterator partition)
    {
        if (kind == CdcEvent.Kind.PARTITION_DELETE)
        {
            updateMaxTimestamp(partition.partitionLevelDeletion().markedForDeleteAt());
        }

        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().partitionKeyColumns();
        List<Value> pk = new ArrayList<>(columnMetadatas.size());

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
        this.partitionKeys = pk;
    }

    void setStaticColumns(UnfilteredRowIterator partition)
    {
        Row staticRow = partition.staticRow();

        if (staticRow.isEmpty())
        {
            return;
        }

        List<Value> sc = new ArrayList<>(staticRow.columnCount());
        for (ColumnData cd : staticRow)
        {
            addColumn(sc, cd);
        }
        this.staticColumns = sc;
    }

    void setClusteringKeys(Unfiltered unfiltered, UnfilteredRowIterator partition)
    {
        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().clusteringColumns();
        if (columnMetadatas.isEmpty()) // the table has no clustering keys
        {
            return;
        }

        List<Value> ck = new ArrayList<>(columnMetadatas.size());
        for (ColumnMetadata cm : columnMetadatas)
        {
            ByteBuffer ckbb = unfiltered.clustering().bufferAt(cm.position());
            ck.add(makeValue(ckbb, cm));
        }
        this.clusteringKeys = ck;
    }

    void setValueColumns(Row row)
    {
        if (kind == CdcEvent.Kind.ROW_DELETE)
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

        List<Value> vc = new ArrayList<>(row.columnCount());
        for (ColumnData cd : row)
        {
            addColumn(vc, cd);
        }
        this.valueColumns = vc;
    }

    private void addColumn(List<Value> holder, ColumnData cd)
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
                kind = CdcEvent.Kind.DELETE;
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

    private void processComplexData(List<Value> holder, ComplexColumnData complex)
    {
        ComplexTypeBuffer buffer = ComplexTypeBuffer.newBuffer(complex.column().type, complex.cellsCount());
        boolean allTombstone = true;
        String columnName = complex.column().name.toCQLString();
        for (Cell<?> cell : complex)
        {
            updateMaxTimestamp(cell.timestamp());
            if (cell.isTombstone())
            {
                kind = CdcEvent.Kind.COMPLEX_ELEMENT_DELETE;

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

    private void readFromCassandra(List<Value> holder, ComplexColumnData complex)
    {
        updateMaxTimestamp(complex.maxTimestamp());
        List<Value> primaryKeyColumns = new ArrayList<>(getPrimaryKeyColumns());
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

    private List<Value> getPrimaryKeyColumns()
    {
        if (clusteringKeys == null)
        {
            return partitionKeys;
        }
        else
        {
            List<Value> primaryKeys = new ArrayList<>(partitionKeys.size() + clusteringKeys.size());
            primaryKeys.addAll(partitionKeys);
            primaryKeys.addAll(clusteringKeys);
            return primaryKeys;
        }
    }

    public Value makeValue(ByteBuffer value, ColumnMetadata columnMetadata)
    {
        return makeValue(columnMetadata.ksName,
                         columnMetadata.name.toCQLString(),
                         columnMetadata.type.asCQL3Type().toString(),
                         value);
    }

    public Value makeValue(String keyspace, String name, String type, ByteBuffer value)
    {
        return new Value(keyspace, name, type, value);
    }


    public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
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

        ((FourZeroRangeTombstoneBuilder) rangeTombstoneBuilder).add(marker);

        if (rangeTombstoneBuilder.canBuild())
        {
            rangeTombstoneList.add(rangeTombstoneBuilder.build());
        }
    }

    public FourZeroRangeTombstoneBuilder rangeTombstoneBuilder(TableMetadata metadata)
    {
        return new FourZeroRangeTombstoneBuilder(metadata);
    }
}
