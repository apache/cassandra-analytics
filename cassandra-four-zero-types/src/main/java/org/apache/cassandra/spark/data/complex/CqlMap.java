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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;

import static org.apache.cassandra.spark.data.CqlField.NO_TTL;

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
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) MapSerializer.getInstance(((CqlType) keyType()).serializer(),
                                                             ((CqlType) valueType()).serializer(),
                                                             ((CqlType) keyType()).dataType().comparatorSet);
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
    public Object randomValue(int minCollectionSize)
    {
        return IntStream.range(0, RandomUtils.RANDOM.nextInt(16) + minCollectionSize)
                        .mapToObj(entry -> Pair.create(keyType().randomValue(minCollectionSize),
                                                       valueType().randomValue(minCollectionSize)))
                        .collect(Collectors.toMap(Pair::left, Pair::right, (first, second) -> first));
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
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

    @Override
    public void addCell(final org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                        ColumnMetadata cd,
                        long timestamp,
                        int ttl,
                        int now,
                        Object value)
    {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
        {
            if (ttl != NO_TTL)
            {
                rowBuilder.addCell(BufferCell.expiring(cd, timestamp, ttl, now, valueType().serialize(entry.getValue()),
                                                       CellPath.create(keyType().serialize(entry.getKey()))));
            }
            else
            {
                rowBuilder.addCell(BufferCell.live(cd, timestamp, valueType().serialize(entry.getValue()),
                                                   CellPath.create(keyType().serialize(entry.getKey()))));
            }
        }
    }
}
