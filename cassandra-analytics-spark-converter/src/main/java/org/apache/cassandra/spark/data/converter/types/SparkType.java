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

package org.apache.cassandra.spark.data.converter.types;

import java.util.Comparator;
import java.util.function.Function;

import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

public interface SparkType extends Comparator<Object>
{
    default DataType dataType()
    {
        return dataType(BigNumberConfig.DEFAULT);
    }

    DataType dataType(BigNumberConfig bigNumberConfig);

    default Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        // All other non-overridden data types work as ordinary Java data types
        return value;
    }

    default Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int position)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    default Object sparkSqlRowValue(Row row, int position)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    default Object nativeSparkSqlRowValue(Row row, int position)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    default Object toTestRowType(Object value)
    {
        return value;
    }

    default boolean equals(Object first, Object second)
    {
        if (first == second)
        {
            return true;
        }
        else if (first == null || second == null)
        {
            return false;
        }
        return equalsTo(first, second);
    }

    default boolean equalsTo(Object first, Object second)
    {
        return compare(first, second) == 0;
    }

    default int compare(Object first, Object second)
    {
        if (first == null || second == null)
        {
            return first == second ? 0 : (first == null ? -1 : 1);
        }
        return compareTo(first, second);
    }

    static int compareArrays(Object[] first, Object[] second, Function<Integer, SparkType> types)
    {
        for (int index = 0; index < Math.min(first.length, second.length); index++)
        {
            int comparison = types.apply(index).compare(first[index], second[index]);
            if (comparison != 0)
            {
                return comparison;
            }
        }
        return Integer.compare(first.length, second.length);
    }

    static boolean equalsArrays(Object[] first, Object[] second, Function<Integer, SparkType> types)
    {
        for (int index = 0; index < Math.min(first.length, second.length); index++)
        {
            if (!types.apply(index).equals(first[index], second[index]))
            {
                return false;
            }
        }
        return first.length == second.length;
    }

    default int compareTo(Object first, Object second)
    {
        throw new NotImplementedException("compareTo not implemented");
    }
}
