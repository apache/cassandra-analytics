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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

@SuppressWarnings("unchecked")
public class CqlSet extends CqlList implements CqlField.CqlSet
{
    public CqlSet(CqlField.CqlType type)
    {
        super(type);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return SetType.getInstance(((CqlType) type()).dataType(), isMultiCell);
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Set;
    }

    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) SetSerializer.getInstance(((CqlType) type()).serializer(),
                                                             ((CqlType) type()).dataType().comparatorSet);
    }

    @Override
    public String name()
    {
        return "set";
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return new HashSet<>(((List<Object>) super.randomValue(minCollectionSize)));
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return new HashSet<>(((List<Object>) super.toTestRowType(value)));
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        return new HashSet<>(((List<Object>) super.sparkSqlRowValue(row, position)));
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        return new HashSet<>(((List<Object>) super.sparkSqlRowValue(row, position)));
    }

    @Override
    protected void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setSet(position, (Set<?>) value);
    }

    @Override
    public DataType driverDataType(boolean isFrozen)
    {
        return DataType.set(((CqlType) type()).driverDataType(isFrozen));
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return ((Set<?>) value).stream()
                               .map(element -> type().convertForCqlWriter(element, version))
                               .collect(Collectors.toSet());
    }
}
