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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jetbrains.annotations.NotNull;

/**
 * Serializes partition keys in the same way as {@code CQLSSTableWriter#addRow(Map)}, which backs
 * {@link SSTableWriterImplementation}. Each value is serialized with the Java driver codec of its column, or with
 * {@link AbstractType#decompose} when the codec rejects the class of the value. The serialized value is then bound
 * to the column through the bind marker a prepared statement would use, which sorts the elements of sets and maps.
 * Composite keys are built with {@link CompositeType#build}, and the result is validated with
 * {@link QueryProcessor#validateKey}.
 */
class PartitionKeyEncoderImplementation implements PartitionKeyEncoder
{
    private final List<ColumnMetadata> columns;
    private final List<TypeCodec<Object>> codecs;
    private final List<Term> markers;

    PartitionKeyEncoderImplementation(@NotNull TableMetadata table)
    {
        this.columns = table.partitionKeyColumns();
        this.codecs = columns.stream()
                             .map(column -> UDHelper.codecFor(UDHelper.driverType(column.type)))
                             .collect(Collectors.toList());
        this.markers = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++)
        {
            markers.add(new AbstractMarker.Raw(index).prepare(table.keyspace, columns.get(index)));
        }
    }

    @Override
    public ByteBuffer encode(List<Object> partitionKeyValues)
    {
        Preconditions.checkArgument(partitionKeyValues.size() == columns.size(),
                                    "Expected %s partition key values but got %s",
                                    columns.size(), partitionKeyValues.size());
        List<ByteBuffer> boundValues = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++)
        {
            boundValues.add(serialize(columns.get(index), codecs.get(index), partitionKeyValues.get(index)));
        }

        try
        {
            QueryOptions options = QueryOptions.forInternalCalls(boundValues);
            ByteBuffer[] components = new ByteBuffer[columns.size()];
            for (int index = 0; index < components.length; index++)
            {
                components[index] = markers.get(index).bindAndGet(options);
            }
            ByteBuffer key = components.length == 1
                             ? components[0]
                             : CompositeType.build(ByteBufferAccessor.instance, components);
            QueryProcessor.validateKey(key);
            return key;
        }
        catch (InvalidRequestException | MarshalException exception)
        {
            throw new IllegalArgumentException(exception.getMessage(), exception);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ByteBuffer serialize(ColumnMetadata column, TypeCodec<Object> codec, Object value)
    {
        Preconditions.checkArgument(value != null, "Invalid null value for partition key column %s", column.name);
        try
        {
            return codec.serialize(value, ProtocolVersion.CURRENT);
        }
        catch (ClassCastException exception)
        {
            // Same fallback as CQLSSTableWriter, which accepts an Integer for a date column, for example
            try
            {
                return ((AbstractType) column.type).decompose(value);
            }
            catch (RuntimeException fallbackException)
            {
                throw invalidValue(column, value, fallbackException);
            }
        }
        catch (RuntimeException exception)
        {
            throw invalidValue(column, value, exception);
        }
    }

    private static IllegalArgumentException invalidValue(ColumnMetadata column, Object value, RuntimeException cause)
    {
        return new IllegalArgumentException(String.format("Unable to serialize a value of %s for partition key column %s of type %s: %s",
                                                          value.getClass().getName(), column.name, column.type.asCQL3Type(),
                                                          cause.getMessage()),
                                            cause);
    }
}
