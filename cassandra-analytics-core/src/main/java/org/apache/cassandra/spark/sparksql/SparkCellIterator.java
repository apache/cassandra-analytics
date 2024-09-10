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

package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.FastThreadLocalUtf8Decoder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterate through CompactionIterator, deserializing ByteBuffers and normalizing into Object[] array in column order
 */
public class SparkCellIterator implements Iterator<Cell>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkCellIterator.class);

    protected final DataLayer dataLayer;
    private final Stats stats;
    private final CqlTable cqlTable;
    private final Object[] values;
    private final SparkType[] sparkTypes;
    @Nullable
    protected final PruneColumnFilter columnFilter;
    private final long startTimeNanos;
    @NotNull
    private final StreamScanner<RowData> scanner;
    @NotNull
    private final RowData rowData;

    // Mutable Iterator State
    private boolean skipPartition = false;
    private boolean newRow = false;
    private boolean closed = false;
    private Cell next = null;
    private long previousTimeNanos;

    protected final int partitionId;
    protected final int firstProjectedValueColumnPositionOrZero;
    protected final boolean hasProjectedValueColumns;
    private final SparkSqlTypeConverter sparkSqlTypeConverter;

    public SparkCellIterator(int partitionId,
                             @NotNull DataLayer dataLayer,
                             @Nullable StructType requiredSchema,
                             @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.partitionId = partitionId;
        this.dataLayer = dataLayer;
        stats = dataLayer.stats();
        cqlTable = dataLayer.cqlTable();
        columnFilter = buildColumnFilter(requiredSchema, cqlTable);
        if (columnFilter != null)
        {
            LOGGER.info("Adding prune column filter columns='{}'", String.join(",", columnFilter.requiredColumns()));
        }

        hasProjectedValueColumns = cqlTable.numValueColumns() > 0 &&
                                   cqlTable.valueColumns()
                                           .stream()
                                           .anyMatch(field -> columnFilter == null || columnFilter.requiredColumns().contains(field.name()));

        // The value array copies across all the partition/clustering/static columns
        // and the single column value for this cell to the SparkRowIterator
        values = new Object[cqlTable.numNonValueColumns() + (hasProjectedValueColumns ? 1 : 0)];

        // Open compaction scanner
        startTimeNanos = System.nanoTime();
        previousTimeNanos = startTimeNanos;
        scanner = openScanner(partitionId, partitionKeyFilters);
        long openTimeNanos = System.nanoTime() - startTimeNanos;
        LOGGER.info("Opened CompactionScanner runtimeNanos={}", openTimeNanos);
        stats.openedCompactionScanner(openTimeNanos);
        rowData = scanner.data();
        stats.openedSparkCellIterator();
        firstProjectedValueColumnPositionOrZero = maybeGetPositionOfFirstProjectedValueColumnOrZero();

        sparkSqlTypeConverter = dataLayer.bridge().typeConverter();
        sparkTypes = new SparkType[cqlTable.numFields()];
        for (int index = 0; index < cqlTable.numFields(); index++)
        {
            sparkTypes[index] = sparkSqlTypeConverter.toSparkType(cqlTable.field(index).type());
        }
    }

    protected StreamScanner<RowData> openScanner(int partitionId,
                                                 @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        return dataLayer.openCompactionScanner(partitionId, partitionKeyFilters, columnFilter);
    }

    @Nullable
    static PruneColumnFilter buildColumnFilter(@Nullable StructType requiredSchema, @NotNull CqlTable cqlTable)
    {
        return requiredSchema != null
               ? new PruneColumnFilter(Arrays.stream(requiredSchema.fields())
                                             .map(StructField::name)
                                             .filter(cqlTable::has)
                                             .collect(Collectors.toSet()))
               : null;
    }

    public boolean hasProjectedValueColumns()
    {
        return hasProjectedValueColumns;
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            return hasNextThrows();
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public boolean hasNextThrows() throws IOException
    {
        if (next != null || closed)
        {
            return !closed;
        }
        return getNext();
    }

    @Override
    public Cell next()
    {
        Cell result = next;
        assert result != null;
        next = null;
        newRow = false;
        long now = System.nanoTime();
        stats.nextCell(now - previousTimeNanos);
        previousTimeNanos = now;
        return result;
    }

    private boolean getNext() throws IOException
    {
        while (scanner.next())
        {
            // If hasNext returns true, it indicates the partition keys has been loaded into the rid.
            // Therefore, let's try to rebuild partition.
            // Deserialize partition keys - if we have moved to a new partition - and update 'values' Object[] array.
            maybeRebuildPartition();

            scanner.advanceToNextColumn();

            // Skip partition e.g. if token is outside of Spark worker token range
            if (skipPartition)
            {
                continue;
            }

            // Deserialize clustering keys - if moved to new CQL row - and update 'values' Object[] array
            ByteBuffer columnNameBuf = Objects.requireNonNull(rowData.getColumnName(), "ColumnName buffer in Rid is null, this is unexpected");
            maybeRebuildClusteringKeys(columnNameBuf);

            // Deserialize CQL field column name
            ByteBuffer component = ByteBufferUtils.extractComponent(columnNameBuf, cqlTable.numClusteringKeys());
            String columnName = component != null ? FastThreadLocalUtf8Decoder.stringThrowRuntime(component) : null;
            if (columnName == null || columnName.isEmpty())
            {
                if (!hasProjectedValueColumns || !scanner.hasMoreColumns())
                {
                    if (hasProjectedValueColumns)
                    {
                        // null out the value of the cell for the case where we have projected value columns
                        values[values.length - 1] = null;
                    }
                    // We use the position of a cell for a value column that is projected, or zero if no value
                    // columns are projected. The column we find is irrelevant because if we fall under this
                    // condition it means that we are in a situation where the row has only PK + CK, but no
                    // regular columns.
                    next = new Cell(values, firstProjectedValueColumnPositionOrZero, newRow, rowData.getTimestamp());
                    return true;
                }

                continue;
            }

            CqlField field = cqlTable.getField(columnName);
            if (field == null)
            {
                LOGGER.warn("Ignoring unknown column columnName='{}'", columnName);
                continue;
            }

            // Deserialize value field or static column and update 'values' Object[] array
            deserializeField(field);

            // Static column, so continue reading entire CQL row before returning
            if (field.isStaticColumn())
            {
                continue;
            }

            // Update next Cell
            next = new Cell(values, field.position(), newRow, rowData.getTimestamp());

            return true;
        }

        // Finished so close
        next = null;
        close();
        return false;
    }

    @Override
    public void close() throws IOException
    {
        if (!closed)
        {
            scanner.close();
            closed = true;
            long runtimeNanos = System.nanoTime() - startTimeNanos;
            LOGGER.info("Closed CompactionScanner runtimeNanos={}", runtimeNanos);
            stats.closedSparkCellIterator(runtimeNanos);
        }
    }

    /* Iterator Helpers */

    /**
     * If it is a new partition see if we can skip (e.g. if partition outside Spark worker token range), otherwise re-build partition keys
     */
    private void maybeRebuildPartition()
    {
        if (!rowData.isNewPartition())
        {
            return;
        }

        // Skip partitions not in the token range for this Spark partition
        newRow = true;

        for (CqlField field : cqlTable.staticColumns())
        {
            // We need to reset static columns between partitions, if a static column is null/not-populated
            // in the next partition, then the previous value might be carried across
            values[field.position()] = null;
        }

        skipPartition = !dataLayer.isInPartition(partitionId, rowData.getToken(), rowData.getPartitionKey());
        if (skipPartition)
        {
            stats.skippedPartitionInIterator(rowData.getPartitionKey(), rowData.getToken());
            return;
        }

        // Or new partition, so deserialize partition keys and update 'values' array
        readPartitionKey(sparkSqlTypeConverter, rowData.getPartitionKey(), cqlTable, this.values, stats);
    }

    public static void readPartitionKey(SparkSqlTypeConverter sparkSqlTypeConverter,
                                        ByteBuffer partitionKey,
                                        CqlTable table,
                                        Object[] values,
                                        Stats stats)
    {
        if (table.numPartitionKeys() == 1)
        {
            // Not a composite partition key
            CqlField field = table.partitionKeys().get(0);
            values[field.position()] = deserialize(sparkSqlTypeConverter, field, partitionKey, stats);
        }
        else
        {
            // Split composite partition keys
            ByteBuffer[] partitionKeyBufs = ByteBufferUtils.split(partitionKey, table.numPartitionKeys());
            int index = 0;
            for (CqlField field : table.partitionKeys())
            {
                values[field.position()] = deserialize(sparkSqlTypeConverter, field, partitionKeyBufs[index++], stats);
            }
        }
    }

    /**
     * Deserialize clustering key components and update 'values' array if changed. Mark isNewRow true if we move to new CQL row.
     */
    private void maybeRebuildClusteringKeys(@NotNull ByteBuffer columnNameBuf)
    {
        List<CqlField> clusteringKeys = cqlTable.clusteringKeys();
        if (clusteringKeys.isEmpty())
        {
            return;
        }

        int index = 0;
        for (CqlField field : clusteringKeys)
        {
            Object newObj = deserialize(field, ByteBufferUtils.extractComponent(columnNameBuf, index++));
            Object oldObj = values[field.position()];
            // Historically, we compare equality of clustering keys using the Spark types
            // to determine if we have moved to a new 'row'. We could also compare using the Cassandra types
            // or the raw ByteBuffers before converting to Spark types  - this might be slightly more performant.
            if (newRow || oldObj == null || newObj == null || !sparkTypes[field.position()].equals(newObj, oldObj))
            {
                newRow = true;
                values[field.position()] = newObj;
            }
        }
    }

    /**
     * Deserialize value field if required and update 'values' array
     */
    private void deserializeField(@NotNull CqlField field)
    {
        if (columnFilter == null || columnFilter.includeColumn(field.name()))
        {
            // Deserialize value
            Object value = deserialize(field, rowData.getValue());

            if (field.isStaticColumn())
            {
                values[field.position()] = value;
                return;
            }

            values[values.length - 1] = value;  // Last index in array always stores the cell value
        }
    }

    private Object deserialize(CqlField field, ByteBuffer buffer)
    {
        return deserialize(sparkSqlTypeConverter, field, buffer, stats);
    }

    private static Object deserialize(SparkSqlTypeConverter sparkSqlTypeConverter, CqlField field, ByteBuffer buffer, Stats stats)
    {
        long now = System.nanoTime();
        Object value = buffer == null ? null : field.deserializeToType(sparkSqlTypeConverter, buffer);
        stats.fieldDeserialization(field, System.nanoTime() - now);
        return value;
    }

    private int maybeGetPositionOfFirstProjectedValueColumnOrZero()
    {
        // find the position of the first value column that is projected
        for (CqlField valueColumn : cqlTable.valueColumns())
        {
            if (columnFilter == null || columnFilter.includeColumn(valueColumn.name()))
            {
                return valueColumn.position();
            }
        }
        return 0;
    }
}
