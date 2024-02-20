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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper iterator around SparkCellIterator to normalize cells into Spark SQL rows
 */
abstract class AbstractSparkRowIterator
{
    private final Stats stats;
    private final SparkCellIterator it;
    private final long openTimeNanos;
    private final RowBuilder builder;

    protected final List<SchemaFeature> requestedFeatures;
    protected final CqlTable cqlTable;
    protected final StructType columnFilter;
    protected final boolean hasProjectedValueColumns;

    private Cell cell = null;
    private InternalRow row = null;

    AbstractSparkRowIterator(int partitionId,
                             @NotNull DataLayer dataLayer,
                             @Nullable StructType requiredSchema,
                             @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.stats = dataLayer.stats();
        this.cqlTable = dataLayer.cqlTable();
        this.columnFilter = useColumnFilter(requiredSchema, cqlTable) ? requiredSchema : null;
        this.it = buildCellIterator(partitionId, dataLayer, columnFilter, partitionKeyFilters);
        this.stats.openedSparkRowIterator();
        this.openTimeNanos = System.nanoTime();
        this.requestedFeatures = dataLayer.requestedFeatures();
        this.hasProjectedValueColumns = it.hasProjectedValueColumns();
        this.builder = newBuilder();
    }

    protected SparkCellIterator buildCellIterator(int partitionId,
                                                  @NotNull DataLayer dataLayer,
                                                  @Nullable StructType columnFilter,
                                                  @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        return new SparkCellIterator(partitionId, dataLayer, columnFilter, partitionKeyFilters);
    }

    private static boolean useColumnFilter(@Nullable StructType requiredSchema, CqlTable cqlTable)
    {
        if (requiredSchema == null)
        {
            return false;
        }
        // Only use column filter if it excludes any of the CqlTable fields
        Set<String> requiredFields = Arrays.stream(requiredSchema.fields()).map(StructField::name).collect(Collectors.toSet());
        return cqlTable.fields().stream()
                       .map(CqlField::name)
                       .anyMatch(field -> !requiredFields.contains(field));
    }

    abstract RowBuilder newBuilder();

    public InternalRow get()
    {
        return row;
    }

    public boolean next() throws IOException
    {
        // We are finished if not already reading a row (if cell != null, it can happen if previous row was incomplete)
        // and SparkCellIterator has no next value
        if (cell == null && !it.hasNextThrows())
        {
            return false;
        }

        // Pivot values to normalize each cell into single SparkSQL or 'CQL' type row
        do
        {
            if (cell == null)
            {
                // Read next cell
                cell = it.next();
            }

            if (builder.isFirstCell())
            {
                // On first iteration, copy all partition keys, clustering keys, static columns
                assert cell.isNewRow;
                builder.copyKeys(cell);
            }
            else if (cell.isNewRow)
            {
                // Current row is incomplete, so we have moved to new row before reaching end
                // break out to return current incomplete row and handle next row in next iteration
                break;
            }

            builder.onCell(cell);

            if (hasProjectedValueColumns)
            {
                // If schema has value column
                builder.copyValue(cell);
            }
            cell = null;
            // Keep reading more cells until we read the entire row
        } while (builder.hasMoreCells() && it.hasNextThrows());

        // Build row and reset builder for next row
        row = builder.build();
        builder.reset();

        stats.nextRow();
        return true;
    }

    public void close() throws IOException
    {
        stats.closedSparkRowIterator(System.nanoTime() - openTimeNanos);
        it.close();
    }
}
