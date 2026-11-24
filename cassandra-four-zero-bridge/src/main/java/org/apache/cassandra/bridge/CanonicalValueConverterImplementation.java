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

package org.apache.cassandra.bridge;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jetbrains.annotations.NotNull;

/**
 * Converts a column value by serializing it as {@code CQLSSTableWriter} does, sorting the sets and maps nested in
 * the serialized value, and deserializing the result with the codec that the writer uses for the column, which
 * serializes it back to the same bytes.
 * <p>
 * Only the columns that need it are converted: those whose values nest a set or a map, and the descending
 * clustering columns of a type that has components, for which the codec of the writer differs, from Cassandra
 * 5.0 on, from the codec of the type of the values that the writer takes.
 * <p>
 * Each bridge gives it the codecs that its {@code CQLSSTableWriter} uses, and the sorter of its version. It only
 * uses the parts of the Cassandra API that are the same from Cassandra 4.0 on, so that the bridges of all versions
 * share it. Thread-safe.
 */
class CanonicalValueConverterImplementation implements CanonicalValueConverter
{
    private final Map<String, Column> columns;
    private final CollectionElementSorter sorter;

    /**
     * @param columnTypes the type of each column of the table, by column name
     * @param codecs      the codec that {@code CQLSSTableWriter} uses for a column of a type
     * @param sorter      sorter of the sets and maps of the Cassandra version
     */
    CanonicalValueConverterImplementation(@NotNull Map<String, AbstractType<?>> columnTypes,
                                          @NotNull Function<AbstractType<?>, TypeCodec<Object>> codecs,
                                          @NotNull CollectionElementSorter sorter)
    {
        Map<String, Column> columns = new HashMap<>();
        columnTypes.forEach((name, type) -> {
            AbstractType<?> valueType = type instanceof ReversedType ? ((ReversedType<?>) type).baseType : type;
            boolean descendingWithComponents = valueType != type && !valueType.subTypes().isEmpty();
            if (descendingWithComponents || sorter.nestsSetOrMap(valueType))
            {
                columns.put(name, new Column(valueType, codecs.apply(valueType), codecs.apply(type)));
            }
        });
        this.columns = ImmutableMap.copyOf(columns);
        this.sorter = sorter;
    }

    @Override
    public Object convert(String columnName, Object value)
    {
        Column column = columns.get(columnName);
        // The writer takes null, empty values and its unset value, an empty buffer, as they are
        if (column == null || value == null || value instanceof ByteBuffer && !((ByteBuffer) value).hasRemaining())
        {
            return value;
        }
        try
        {
            ByteBuffer serialized = column.serialize(value);
            if (!serialized.hasRemaining())
            {
                return value;
            }
            return column.writerCodec.deserialize(sorter.sort(column.valueType, serialized), ProtocolVersion.CURRENT);
        }
        catch (RuntimeException exception)
        {
            throw new IllegalArgumentException(String.format("Unable to convert a value of %s for column %s of type %s: %s",
                                                             value.getClass().getName(), columnName,
                                                             column.valueType.asCQL3Type(), exception.getMessage()),
                                               exception);
        }
    }

    private static final class Column
    {
        // Type of the values the writer takes, which is not reversed
        final AbstractType<?> valueType;
        final TypeCodec<Object> valueCodec;
        final TypeCodec<Object> writerCodec;

        Column(AbstractType<?> valueType, TypeCodec<Object> valueCodec, TypeCodec<Object> writerCodec)
        {
            this.valueType = valueType;
            this.valueCodec = valueCodec;
            this.writerCodec = writerCodec;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        ByteBuffer serialize(Object value)
        {
            try
            {
                return valueCodec.serialize(value, ProtocolVersion.CURRENT);
            }
            catch (RuntimeException exception)
            {
                // CQLSSTableWriter falls back to the type for the values its codecs do not take, such as an Integer
                // for a date. It only does so on a ClassCastException, which the codec of a collection turns into an
                // InvalidTypeException when the class of an element does not match, so fall back on any failure.
                try
                {
                    return ((AbstractType) valueType).decompose(value);
                }
                catch (RuntimeException fallbackException)
                {
                    // Keep the failure of the codec, which usually names the class that does not match
                    fallbackException.addSuppressed(exception);
                    throw fallbackException;
                }
            }
        }
    }
}
