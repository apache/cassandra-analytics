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

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/**
 * FullRowBuilder expects all fields in the schema to be returned, i.e. no prune column filter
 */
class FullRowBuilder implements RowBuilder
{
    static final Object[] EMPTY_RESULT = new Object[0];
    final int numColumns;
    final int numCells;
    final boolean noValueColumns;
    int extraColumns;
    Object[] result;
    int count;
    private final CqlTable cqlTable;

    FullRowBuilder(CqlTable cqlTable, boolean noValueColumns)
    {
        this.cqlTable = cqlTable;
        this.numColumns = cqlTable.numFields();
        this.numCells = cqlTable.numNonValueColumns() + (noValueColumns ? 0 : 1);
        this.noValueColumns = noValueColumns;
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
        int length = noValueColumns ? cell.values.length : cell.values.length - 1;
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
        return !noValueColumns;
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
        List<CqlField> fields = cqlTable.fields();
        return IntStream.range(0, fields.size())
                        .filter(i -> Objects.equals(fields.get(i).name(), name))
                        .findFirst()
                        .orElse(-1);
    }

    @Override
    public GenericInternalRow build()
    {
        return new GenericInternalRow(result);
    }
}
