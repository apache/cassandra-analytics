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

package org.apache.cassandra.spark.data.complex;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;

public class CqlMap extends CqlCollection implements CqlField.CqlMap
{
    public CqlMap(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        super(keyType, valueType);
    }

    public CqlField.CqlType keyType()
    {
        return type();
    }

    public CqlField.CqlType valueType()
    {
        return type(1);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return MapType.getInstance(((CqlType) keyType()).dataType(), ((CqlType) valueType()).dataType(), isMultiCell);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        return mapToSparkSqlType((Map<Object, Object>) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) MapSerializer.getInstance(((CqlType) keyType()).serializer(),
                                                             ((CqlType) valueType()).serializer(),
                                                             ((CqlType) keyType()).dataType().comparatorSet);
    }

    @Override
    public boolean equals(Object first, Object second)
    {
        return CqlField.equalsArrays(((MapData) first).valueArray().array(),
                                     ((MapData) second).valueArray().array(), position -> valueType());
    }

    private ArrayBasedMapData mapToSparkSqlType(Map<Object, Object> map)
    {
        Object[] keys = new Object[map.size()];
        Object[] values = new Object[map.size()];
        int position = 0;
        for (Map.Entry<Object, Object> entry : map.entrySet())
        {
            keys[position] = keyType().toSparkSqlType(entry.getKey());
            values[position] = valueType().toSparkSqlType(entry.getValue());
            position++;
        }
        return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Map;
    }

    @Override
    public String name()
    {
        return "map";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createMapType(keyType().sparkSqlType(bigNumberConfig),
                                       valueType().sparkSqlType(bigNumberConfig));
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        MapData map = row.getMap(position);
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        Map<Object, Object> result = new LinkedHashMap<>(keys.numElements());
        for (int element = 0; element < keys.numElements(); element++)
        {
            Object key = keyType().toTestRowType(keys.get(element, keyType().sparkSqlType()));
            Object value = valueType().toTestRowType(values.get(element, valueType().sparkSqlType()));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        return row.getJavaMap(position).entrySet().stream()
                .collect(Collectors.toMap(element -> keyType().toTestRowType(element.getKey()),
                                          element -> valueType().toTestRowType(element.getValue())));
    }

    @Override
    public int compare(Object first, Object second)
    {
        return CqlField.compareArrays(((MapData) first).valueArray().array(),
                                      ((MapData) second).valueArray().array(), position -> valueType());
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return IntStream.range(0, RandomUtils.RANDOM.nextInt(16) + minCollectionSize)
                        .mapToObj(entry -> Pair.create(keyType().randomValue(minCollectionSize),
                                                       valueType().randomValue(minCollectionSize)))
                        .collect(Collectors.toMap(Pair::left, Pair::right, (first, second) -> first));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object toTestRowType(Object value)
    {
        return ((Map<Object, Object>) JavaConverters.mapAsJavaMapConverter(((scala.collection.immutable.Map<?, ?>) value))
                                                    .asJava()).entrySet().stream()
                .collect(Collectors.toMap(element -> keyType().toTestRowType(element.getKey()),
                                          element -> valueType().toTestRowType(element.getValue())));
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setMap(position, (Map<?, ?>) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.map(((CqlType) keyType()).driverDataType(isFrozen),
                                                                      ((CqlType) valueType()).driverDataType(isFrozen));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        Map<Object, Object> map = (Map<Object, Object>) value;
        return map.entrySet().stream()
                .collect(Collectors.toMap(element -> keyType().convertForCqlWriter(element.getKey(), version),
                                          element -> valueType().convertForCqlWriter(element.getValue(), version)));
    }
}
