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
import java.util.function.Function;

import org.apache.cassandra.analytics.stats.Stats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper iterator around CellIterator to normalize cells into Spark SQL rows
 *
 * @param <T> type of row returned by Iterator.
 */
public abstract class RowIterator<T>
{
    private final Stats stats;
    protected final CellIterator it;
    private final long openTimeNanos;
    private final RowBuilder<T> builder;

    // NOTE: requiredColumns might contain additional decorated fields like last_modified_timestamp,
    // but PruneColumnFilter is pushed down to the SSTableReader so only contains the real table fields
    @Nullable
    protected final String[] requiredColumns;

    protected T row;
    private Cell cell = null;

    protected RowIterator(CellIterator it,
                          Stats stats,
                          @Nullable String[] requiredColumns,
                          Function<RowBuilder<T>, RowBuilder<T>> decorator)
    {
        this.stats = stats;
        this.it = it;
        this.openTimeNanos = System.nanoTime();
        this.requiredColumns = requiredColumns;
        this.builder = newBuilder(decorator);
    }

    @NotNull
    protected RowBuilder<T> newBuilder(Function<RowBuilder<T>, RowBuilder<T>> decorator)
    {
        RowBuilder<T> builder;
        if (requiredColumns != null)
        {
            builder = newPartialBuilder();
        }
        else
        {
            builder = newFullRowBuilder();
        }

        builder = decorator.apply(builder);

        builder.reset();
        return builder;
    }

    /**
     * @return an instance of a PartialRowBuilder that builds a row with a subset of columns in the schema and maps to the generic type.
     */
    public abstract PartialRowBuilder<T> newPartialBuilder();

    /**
     * @return an instance of a FullRowBuilder that builds a row with all columns in the schema and maps to the generic type.
     */
    public abstract FullRowBuilder<T> newFullRowBuilder();

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

            if (it.hasProjectedValueColumns())
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

    public T get()
    {
        return row;
    }
}
