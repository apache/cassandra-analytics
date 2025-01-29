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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.jetbrains.annotations.NotNull;

/**
 * PartialRowBuilder that builds row only containing fields in requiredSchema prune-column filter
 * NOTE: Spark 3 changed the contract from Spark 2 and requires us to only return the columns specified in
 * the requiredSchema 'prune column' filter and not a sparse Object[] array with null values for excluded columns
 *
 * @param <T> type of row returned by builder
 */
public class PartialRowBuilder<T> extends FullRowBuilder<T>
{
    private final int[] positionsMap;
    private final boolean hasAllNonValueColumns;
    protected final String[] requiredSchema;
    private final Map<String, Integer> columnIndex;

    public PartialRowBuilder(@NotNull String[] requiredSchema,
                             CqlTable table,
                             boolean hasProjectedValueColumns,
                             Function<Object[], T> rowBuilder)
    {
        super(table, hasProjectedValueColumns, rowBuilder);
        this.requiredSchema = requiredSchema;
        this.columnIndex = new HashMap<>(requiredSchema.length);
        for (int i = 0; i < requiredSchema.length; i++)
        {
            columnIndex.put(requiredSchema[i], i);
        }
        Set<String> requiredColumns = Arrays.stream(requiredSchema).collect(Collectors.toSet());
        hasAllNonValueColumns = table.fields().stream()
                                     .filter(CqlField::isNonValueColumn)
                                     .map(CqlField::name)
                                     .allMatch(requiredColumns::contains);

        // Map original column position to new position in requiredSchema
        positionsMap = IntStream.range(0, table.numFields())
                                .map(position -> -1)
                                .toArray();
        int position = 0;
        for (String fieldName : requiredSchema)
        {
            CqlField field = table.getField(fieldName);
            if (field != null)  // Field might be last modified timestamp
            {
                positionsMap[field.position()] = position++;
            }
        }
    }

    @Override
    public void reset()
    {
        count = 0;
        int totalColumns = requiredSchema.length;
        if (totalColumns > 0)
        {
            result = new Object[totalColumns];
        }
        else
        {
            result = EMPTY_RESULT;
        }
    }

    @Override
    public int fieldIndex(String name)
    {
        return requiredSchema != null ? columnIndex.get(name) : super.fieldIndex(name);
    }

    @Override
    public void copyKeys(Cell cell)
    {
        if (hasAllNonValueColumns)
        {
            // Optimization if we are returning all primary key/static columns we can use the super method
            super.copyKeys(cell);
            return;
        }

        // Otherwise we need to only return columns requested and map to new position in result array
        int length = !hasProjectedValueColumns ? cell.values.length : cell.values.length - 1;
        for (int index = 0; index < length; index++)
        {
            int position = positionsMap[index];
            if (position >= 0)
            {
                result[position] = cell.values[index];
            }
        }
        count += length;
    }

    @Override
    public void copyValue(Cell cell)
    {
        // Copy the next value column mapping column to new position
        int position = positionsMap[cell.position];
        if (position >= 0)
        {
            result[position] = cell.values[cell.values.length - 1];
        }
        count++;
    }
}
