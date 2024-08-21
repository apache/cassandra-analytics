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

package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.mutable.WrappedArray;

public interface ListTrait extends CollectionTrait
{
    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createArrayType(sparkType().dataType(bigNumberConfig));
    }

    @SuppressWarnings("unchecked")
    @Override
    default Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return ArrayData.toArrayData(((Collection<Object>) value)
                                     .stream()
                                     .map(a -> sparkType().toSparkSqlType(a, isFrozen))
                                     .toArray());
    }

    @Override
    default Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        return Arrays.stream(row.getArray(position).array())
                     .map(element -> sparkType().toTestRowType(element))
                     .collect(collector());
    }

    @Override
    default Object sparkSqlRowValue(Row row, int position)
    {
        return row.getList(position).stream()
                  .map(element -> sparkType().toTestRowType(element))
                  .collect(collector());
    }

    @SuppressWarnings({ "unchecked", "RedundantCast" }) // redundant cast to (Object[]) is required
    @Override
    default Object toTestRowType(Object value)
    {
        return Stream.of((Object[]) ((WrappedArray<Object>) value).array())
                     .map(element -> sparkType().toTestRowType(element))
                     .collect(Collectors.toList());
    }

    default  <T> Collector<T, ?, ?> collector()
    {
        return Collectors.toList();
    }

    @Override
    default boolean equalsTo(Object first, Object second)
    {
        return SparkType.equalsArrays(((GenericArrayData) first).array(),
                                      ((GenericArrayData) second).array(),
                                      (position) -> sparkType());
    }

    @Override
    default int compareTo(Object first, Object second)
    {
        return SparkType.compareArrays(((GenericArrayData) first).array(), ((GenericArrayData) second).array(), (pos) -> sparkType());
    }
}
