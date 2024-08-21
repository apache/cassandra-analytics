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

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.data.CqlField.STRING_COMPARATOR;

public interface StringTraits extends SparkType
{
    @Override
    default Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return UTF8String.fromString(value.toString()); // UTF8String
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getString(position);
    }

    @Override
    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int position)
    {
        return row.getString(position);
    }

    @Override
    default Object toTestRowType(Object value)
    {
        if (value instanceof UTF8String)
        {
            return ((UTF8String) value).toString();
        }
        return value;
    }

    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.StringType;
    }

    @Override
    default boolean equalsTo(Object first, Object second)
    {
        // UUID comparator is particularly slow because of UUID.fromString so compare for equality as strings
        return first.equals(second);
    }

    @Override
    default int compareTo(Object first, Object second)
    {
        if (first instanceof String && second instanceof String)
        {
            return STRING_COMPARATOR.compare(first.toString(), second.toString());
        }
        return ((UTF8String) first).compare((UTF8String) second);
    }
}
