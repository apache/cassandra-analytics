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

import java.util.Optional;
import java.util.function.Function;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;

/**
 * FullRowBuilder expects all fields in the schema to be returned, i.e. no prune column filter
 *
 * @param <T> type of row returned by builder
 */
public class FullRowBuilder<T> implements RowBuilder<T>
{
    static final Object[] EMPTY_RESULT = new Object[0];
    protected final int numColumns;
    protected final int numCells;
    protected final boolean hasProjectedValueColumns;
    protected int extraColumns;
    protected Object[] result;
    protected int count;
    private final CqlTable cqlTable;
    protected final Function<Object[], T> rowBuilder;

    FullRowBuilder(CqlTable cqlTable, boolean hasProjectedValueColumns, Function<Object[], T> rowBuilder)
    {
        this.cqlTable = cqlTable;
        this.numColumns = cqlTable.numFields();
        this.hasProjectedValueColumns = hasProjectedValueColumns;
        this.numCells = cqlTable.numNonValueColumns() + (hasProjectedValueColumns ? 1 : 0);
        this.rowBuilder = rowBuilder;
    }

    @Override
    public CqlTable getCqlTable()
    {
        return cqlTable;
    }

    @Override
    public void reset()
    {
        this.count = 0;
        int totalColumns = numColumns + extraColumns;
        if (totalColumns != 0)
        {
            result = new Object[totalColumns];
        }
        else
        {
            result = EMPTY_RESULT;
        }
    }

    @Override
    public boolean isFirstCell()
    {
        return count == 0;
    }

    @Override
    public void copyKeys(Cell cell)
    {
        // Need to handle special case where schema is only partition or clustering keys - i.e. no value columns
        int length = !hasProjectedValueColumns ? cell.values.length : cell.values.length - 1;
        System.arraycopy(cell.values, 0, result, 0, length);
        count += length;
    }

    @Override
    public void copyValue(Cell cell)
    {
        // Copy the next value column
        result[cell.position] = cell.values[cell.values.length - 1];
        count++;  // Increment the number of cells visited
    }

    @Override
    public Object[] array()
    {
        return result;
    }

    @Override
    public int columnsCount()
    {
        return numColumns;
    }

    @Override
    public boolean hasRegularValueColumn()
    {
        return hasProjectedValueColumns;
    }

    @Override
    public int expandRow(int extraColumns)
    {
        this.extraColumns = extraColumns;
        return numColumns;
    }

    @Override
    public boolean hasMoreCells()
    {
        return count < numColumns;
    }

    @Override
    public void onCell(Cell cell)
    {
        assert 0 < cell.values.length && cell.values.length <= numCells;
    }

    @Override
    public int fieldIndex(String name)
    {
        return Optional.ofNullable(cqlTable.getField(name))
                       .map(CqlField::position)
                       .orElse(-1);
    }

    public T build()
    {
        return rowBuilder.apply(result);
    }
}
