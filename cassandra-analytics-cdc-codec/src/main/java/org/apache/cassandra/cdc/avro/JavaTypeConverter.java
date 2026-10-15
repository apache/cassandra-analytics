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

package org.apache.cassandra.cdc.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.TypeConverter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TypeConverter} that converts CQL values into the Java values expected by
 * {@link AvroDataUtils#toAvro(Object, Schema)}.
 * <p>
 * Cassandra serializers leave UDTs as raw bytes, including inside collections and tuples. This converter
 * deserializes them into {@code Map<String, Object>} keyed by field name with {@link CqlField.CqlUdt#deserializeUdt},
 * and converts the elements of collections and tuples recursively. All other values are returned unchanged.
 */
public final class JavaTypeConverter implements TypeConverter
{
    public static final JavaTypeConverter INSTANCE = new JavaTypeConverter();

    private JavaTypeConverter()
    {
    }

    /**
     * Deserializes a column value into the Java value expected by {@link AvroDataUtils#toAvro(Object, Schema)}.
     *
     * @param type  the CQL type of the column
     * @param value the serialized column value
     * @return the deserialized value, or null if the value is null
     */
    @Nullable
    public static Object deserialize(CqlField.CqlType type, @Nullable ByteBuffer value)
    {
        if (value == null)
        {
            return null;
        }
        // only types containing UDTs need conversion; avoid the overhead for everything else
        if (type.udts().isEmpty())
        {
            return type.deserializeToJavaType(value);
        }
        return type.deserializeToType(INSTANCE, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convert(CqlField.CqlType cqlType, @NotNull Object value, boolean isFrozen)
    {
        switch (cqlType.internalType())
        {
            case Frozen:
                return convert(((CqlField.CqlFrozen) cqlType).inner(), value, true);
            case Udt:
                return value instanceof ByteBuffer
                       ? ((CqlField.CqlUdt) cqlType).deserializeUdt(this, (ByteBuffer) value, isFrozen)
                       : value;
            case Tuple:
                return convertTuple((CqlField.CqlTuple) cqlType, value);
            case List:
                CqlField.CqlType listElementType = ((CqlField.CqlCollection) cqlType).type();
                List<Object> list = new ArrayList<>(((Collection<Object>) value).size());
                for (Object element : (Collection<Object>) value)
                {
                    list.add(convertNullable(listElementType, element));
                }
                return list;
            case Set:
                CqlField.CqlType setElementType = ((CqlField.CqlCollection) cqlType).type();
                Set<Object> set = new LinkedHashSet<>();
                for (Object element : (Collection<Object>) value)
                {
                    set.add(convertNullable(setElementType, element));
                }
                return set;
            case Map:
                CqlField.CqlMap mapType = (CqlField.CqlMap) cqlType;
                Map<Object, Object> map = new LinkedHashMap<>();
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet())
                {
                    map.put(convertNullable(mapType.keyType(), entry.getKey()),
                            convertNullable(mapType.valueType(), entry.getValue()));
                }
                return map;
            default:
                return value;
        }
    }

    /**
     * Tuple elements are deserialized by {@link CqlField.CqlTuple#deserializeTuple} without a converter, so each
     * element is converted here, which also handles collections and UDTs nested in the tuple.
     */
    private Object[] convertTuple(CqlField.CqlTuple tuple, Object value)
    {
        Object[] elements = value instanceof ByteBuffer
                            ? tuple.deserializeTuple((ByteBuffer) value, true)
                            : (Object[]) value;
        Object[] result = new Object[elements.length];
        for (int index = 0; index < elements.length; index++)
        {
            result[index] = convertNullable(tuple.type(index), elements[index]);
        }
        return result;
    }

    private Object convertNullable(CqlField.CqlType cqlType, @Nullable Object value)
    {
        return value == null ? null : convert(cqlType, value, true);
    }
}
