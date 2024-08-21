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

import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.JavaConverters;

public class Map implements MapTrait
{
    private final SparkSqlTypeConverter converter;
    private final CqlField.CqlMap map;

    public Map(SparkSqlTypeConverter converter, CqlField.CqlMap map)
    {
        this.converter = converter;
        this.map = map;
    }

    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createMapType(keyType().dataType(bigNumberConfig),
                                       valueType().dataType(bigNumberConfig));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return mapToSparkSqlType((java.util.Map<Object, Object>) value, isFrozen);
    }

    private ArrayBasedMapData mapToSparkSqlType(final java.util.Map<Object, Object> map, boolean isFrozen)
    {
        final Object[] keys = new Object[map.size()];
        final Object[] values = new Object[map.size()];
        int position = 0;
        for (final java.util.Map.Entry<Object, Object> entry : map.entrySet())
        {
            keys[position] = keyType().toSparkSqlType(entry.getKey(), isFrozen);
            values[position] = valueType().toSparkSqlType(entry.getValue(), isFrozen);
            position++;
        }
        return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        final MapData map = row.getMap(position);
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        final java.util.Map<Object, Object> result = new HashMap<>(keys.numElements());
        for (int element = 0; element < keys.numElements(); element++)
        {
            final Object key = keyType().toTestRowType(keys.get(element, keyType().dataType()));
            final Object value = valueType().toTestRowType(values.get(element, valueType().dataType()));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        return row.getJavaMap(position).entrySet().stream()
                  .collect(Collectors.toMap(
                  element -> keyType().toTestRowType(element.getKey()),
                  element -> valueType().toTestRowType(element.getValue())
                  ));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toTestRowType(Object value)
    {
        return ((java.util.Map<Object, Object>) JavaConverters.mapAsJavaMapConverter(((scala.collection.immutable.Map<?, ?>) value))
                                                              .asJava()).entrySet().stream()
                                                                        .collect(Collectors.toMap(
                                                                                 element -> keyType().toTestRowType(element.getKey()),
                                                                                 element -> valueType().toTestRowType(element.getValue()))
                                                                        );
    }

    public CqlField.CqlCollection collection()
    {
        return map;
    }

    public SparkSqlTypeConverter converter()
    {
        return converter;
    }

    @Override
    public boolean equalsTo(Object first, Object second)
    {
        return SparkType.equalsArrays(((MapData) first).valueArray().array(),
                                      ((MapData) second).valueArray().array(),
                                      (position) -> valueType());
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return SparkType.compareArrays(((MapData) first).valueArray().array(),
                                       ((MapData) second).valueArray().array(),
                                       (position) -> valueType());
    }
}
