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

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.utils.ScalaConversionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.mutable.Seq;

public class SparkList implements CollectionFeatures
{
    private final SparkSqlTypeConverter converter;
    final CqlField.CqlCollection list;

    public SparkList(SparkSqlTypeConverter converter, CqlField.CqlCollection list)
    {
        this.converter = converter;
        this.list = list;
    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createArrayType(sparkType().dataType(bigNumberConfig));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return ArrayData.toArrayData(((Collection<Object>) value)
                                     .stream()
                                     .map(element -> sparkType().toSparkSqlType(element, isFrozen))
                                     .toArray());
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        return Arrays.stream(row.getArray(position).array())
                     .map(element -> sparkType().toTestRowType(element))
                     .collect(collector());
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        return row.getList(position).stream()
                  .map(element -> sparkType().toTestRowType(element))
                  .collect(collector());
    }

    @SuppressWarnings({ "unchecked", "RedundantCast" }) // redundant cast to (Object[]) is required
    @Override
    public Object toTestRowType(Object value)
    {
        return ScalaConversionUtils.mutableSeqAsJavaList((Seq<Object>) value)
                            .stream()
                            .map(element -> sparkType().toTestRowType(element))
                            .collect(Collectors.toList());
    }

    public <T> Collector<T, ?, ?> collector()
    {
        return Collectors.toList();
    }

    @Override
    public boolean equalsTo(Object first, Object second)
    {
        return SparkType.equalsArrays(((GenericArrayData) first).array(),
                                      ((GenericArrayData) second).array(),
                                      (position) -> sparkType());
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return SparkType.compareArrays(((GenericArrayData) first).array(),
                                       ((GenericArrayData) second).array(),
                                       (position) -> sparkType());
    }

    public CqlField.CqlCollection collection()
    {
        return list;
    }

    public SparkSqlTypeConverter converter()
    {
        return converter;
    }
}
