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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.cql3.functions.types.TupleHelper;
import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.data.TypeConverter;
import org.apache.cassandra.spark.utils.ByteBufferUtils;

public class CqlTuple extends CqlCollection implements CqlField.CqlTuple
{
    CqlTuple(CqlField.CqlType... types)
    {
        super(types);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return new TupleType(types().stream()
                                    .map(type -> (CqlType) type)
                                    .map(CqlType::dataType)
                                    .collect(Collectors.toList()));
    }

    @Override
    public ByteBuffer serialize(Object value)
    {
        return serializeTuple((Object[]) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) new TupleSerializer(types().stream()
                                                              .map(type -> (CqlType) type)
                                                              .map(CqlType::serializer)
                                                              .collect(Collectors.toList()));
    }

    @Override
    public Object deserializeToType(TypeConverter typeConverter, ByteBuffer buffer, boolean isFrozen)
    {
        return typeConverter.convert(this, deserializeTuple(buffer, isFrozen), isFrozen);
    }

    @Override
    public Object deserializeToJavaType(ByteBuffer buffer, boolean isFrozen)
    {
        return deserializeTuple(buffer, isFrozen);
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Tuple;
    }

    @Override
    public String name()
    {
        return "tuple";
    }

    @Override
    public ByteBuffer serializeTuple(Object[] values)
    {
        List<ByteBuffer> buffers = IntStream.range(0, size())
                                            .mapToObj(index -> type(index).serialize(values[index]))
                                            .collect(Collectors.toList());
        ByteBuffer result = ByteBuffer.allocate(buffers.stream()
                                                       .map(Buffer::remaining)
                                                       .map(remaining -> remaining + 4)
                                                       .reduce(Integer::sum)
                                                       .orElse(0));
        for (ByteBuffer buffer : buffers)
        {
            result.putInt(buffer.remaining());  // Length
            result.put(buffer.duplicate());  // Value
        }
        // Cast to ByteBuffer required when compiling with Java 8
        return (ByteBuffer) result.flip();
    }

    @Override
    public Object[] deserializeTuple(ByteBuffer buffer, boolean isFrozen)
    {
        Object[] result = new Object[size()];
        int position = 0;
        for (CqlField.CqlType type : types())
        {
            if (buffer.remaining() < 4)
            {
                break;
            }
            int length = buffer.getInt();
            result[position++] = length > 0 ? type.deserializeToJavaType(ByteBufferUtils.readBytes(buffer, length), isFrozen) : null;
        }
        return result;
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setTupleValue(position, toTupleValue(CassandraVersion.FOURZERO, this, value));
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return types().stream().map(type -> type.randomValue(minCollectionSize)).toArray();
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return TupleHelper.buildTupleType(this, isFrozen);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return toTupleValue(version, this, value);
    }

    public static TupleValue toTupleValue(CassandraVersion version, CqlTuple tuple, Object value)
    {
        if (value instanceof TupleValue)
        {
            return (TupleValue) value;
        }

        TupleValue tupleValue = TupleHelper.buildTupleValue(tuple);
        Object[] array = (Object[]) value;
        for (int position = 0; position < array.length; position++)
        {
            CqlUdt.setNullableInnerValue(version, tupleValue, (CqlType) tuple.type(position), position, array[position]);
        }
        return tupleValue;
    }
}
