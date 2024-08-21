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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.utils.RandomUtils;

public class CqlList extends CqlCollection implements CqlField.CqlList
{
    public CqlList(CqlField.CqlType type)
    {
        super(type);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return ListType.getInstance(((CqlType) type()).dataType(), isMultiCell);
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.List;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) ListSerializer.getInstance(((CqlType) type()).serializer());
    }

    @Override
    public String name()
    {
        return "list";
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setList(position, (List<?>) value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return IntStream.range(0, RandomUtils.RANDOM.nextInt(16) + minCollectionSize)
                        .mapToObj(element -> type().randomValue(minCollectionSize))
                        .collect(Collectors.toList());
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.list(((CqlType) type()).driverDataType(isFrozen));
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return ((List<?>) value).stream()
                                .map(element -> type().convertForCqlWriter(element, version))
                                .collect(Collectors.toList());
    }
}
