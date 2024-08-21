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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.cql3.functions.types.UDTValue;
import org.apache.cassandra.cql3.functions.types.UserType;
import org.apache.cassandra.cql3.functions.types.UserTypeHelper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.data.TypeConverter;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CqlUdt extends CqlType implements CqlField.CqlUdt
{
    private final String keyspace;
    private final String name;
    private final List<CqlField> fields;
    private final Map<String, CqlField> fieldMap;
    private final int hashCode;

    CqlUdt(String keyspace, String name, List<CqlField> fields)
    {
        this.keyspace = keyspace;
        this.name = name;
        this.fields = Collections.unmodifiableList(fields);
        this.fieldMap = this.fields.stream().collect(Collectors.toMap(CqlField::name, Function.identity()));
        this.hashCode = Objects.hash(internalType().ordinal(), this.keyspace, this.name, this.fields);
    }

    @Override
    public Set<CqlField.CqlUdt> udts()
    {
        Set<CqlField.CqlUdt> udts = fields.stream()
                                          .map(CqlField::type)
                                          .map(type -> (CqlType) type)
                                          .map(CqlField.CqlType::udts)
                                          .flatMap(Collection::stream)
                                          .collect(Collectors.toSet());
        udts.add(this);
        return udts;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return fields().stream()
                       .collect(Collectors.toMap(CqlField::name, field -> Objects.requireNonNull(field.type().randomValue(minCollectionSize))));
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setUDTValue(position, (UDTValue) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return UserTypeHelper.newUserType(
                keyspace(),
                name(),
                isFrozen,
                fields().stream()
                        .map(field -> UserTypeHelper.newField(field.name(),
                                                              ((CqlType) field.type()).driverDataType(isFrozen)))
                        .collect(Collectors.toList()),
                ProtocolVersion.V3);
    }

    @Override
    public Object convertForCqlWriter(@NotNull Object value, CassandraVersion version)
    {
        if (value instanceof UDTValue)
        {
            return value;
        }
        return toUserTypeValue(version, this, value);
    }

    @Override
    public String toString()
    {
        return cqlName();
    }

    public CqlFrozen frozen()
    {
        return CqlFrozen.build(this);
    }

    public static Builder builder(String keyspace, String name)
    {
        return new Builder(keyspace, name);
    }

    public static class Builder implements CqlField.CqlUdtBuilder
    {
        private final String keyspace;
        private final String name;
        private final List<CqlField> fields = new ArrayList<>();

        public Builder(String keyspace, String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        @Override
        public Builder withField(String name, CqlField.CqlType type)
        {
            fields.add(new CqlField(false, false, false, name, type, fields.size()));
            return this;
        }

        @Override
        public CqlUdt build()
        {
            return new CqlUdt(keyspace, name, fields);
        }
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return dataType(true);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        // Get UserTypeSerializer from Schema instance to ensure fields are deserialized in correct order
        return Schema.instance.getKeyspaceMetadata(keyspace()).types
               .get(UTF8Serializer.instance.serialize(name()))
               .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", name())));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        // Get UserTypeSerializer from Schema instance to ensure fields are deserialized in correct order
        return (TypeSerializer<T>) Schema.instance.getKeyspaceMetadata(keyspace()).types
                .get(UTF8Serializer.instance.serialize(name()))
                .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", name())))
                .getSerializer();
    }

    @Override
    public Object deserializeToType(TypeConverter converter, ByteBuffer buffer, boolean isFrozen)
    {
        Object value = deserializeUdt(converter, buffer, isFrozen);
        return value != null ? converter.convert(this, value, isFrozen) : null;
    }

    @Override
    public Map<String, Object> deserializeUdt(TypeConverter typeConverter, ByteBuffer buffer, boolean isFrozen)
    {
        if (!isFrozen)
        {
            int fieldCount = buffer.getInt();
            Preconditions.checkArgument(fieldCount == size(),
                    String.format("Unexpected number of fields deserializing UDT '%s', expected %d fields but %d found",
                                  cqlName(), size(), fieldCount));
        }

        Map<String, Object> result = new LinkedHashMap<>(size());
        for (CqlField field : fields())
        {
            if (buffer.remaining() < 4)
            {
                break;
            }
            int length = buffer.getInt();
            result.put(field.name(), length > 0 ? field.deserializeToType(typeConverter, ByteBufferUtils.readBytes(buffer, length), isFrozen) : null);
        }

        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ByteBuffer serialize(Object value)
    {
        return serializeUdt((Map<String, Object>) value);
    }

    @Override
    public ByteBuffer serializeUdt(Map<String, Object> values)
    {
        List<ByteBuffer> buffers = fields().stream()
                                           .map(field -> field.serialize(values.get(field.name())))
                                           .collect(Collectors.toList());

        ByteBuffer result = ByteBuffer.allocate(4 + buffers.stream()
                                                           .map(Buffer::remaining)
                                                           .map(remaining -> remaining + 4)
                                                           .reduce(Integer::sum)
                                                           .orElse(0));
        result.putInt(buffers.size());  // Number of fields
        for (ByteBuffer buffer : buffers)
        {
            result.putInt(buffer.remaining());  // Length
            result.put(buffer.duplicate());  // Value
        }
        // Cast to ByteBuffer required when compiling with Java 8
        return (ByteBuffer) result.flip();
    }

    public InternalType internalType()
    {
        return InternalType.Udt;
    }

    @Override
    public String createStatement(CassandraTypes cassandraTypes, String keyspace)
    {
        return String.format("CREATE TYPE %s.%s (%s);",
                             cassandraTypes.maybeQuoteIdentifier(keyspace),
                             cassandraTypes.maybeQuoteIdentifier(name),
                             fieldsString(cassandraTypes));
    }

    private String fieldsString(CassandraTypes cassandraTypes)
    {
        return fields.stream()
                     .map(field -> fieldString(cassandraTypes, field))
                     .collect(Collectors.joining(", "));
    }

    private static String fieldString(CassandraTypes cassandraTypes, CqlField field)
    {
        return String.format("%s %s", cassandraTypes.maybeQuoteIdentifier(field.name()), field.type().cqlName());
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String name()
    {
        return name;
    }

    public int size()
    {
        return fields.size();
    }

    public List<CqlField> fields()
    {
        return fields;
    }

    public CqlField field(String name)
    {
        return fieldMap.get(name);
    }

    public CqlField field(int position)
    {
        return fields.get(position);
    }

    public CqlField.CqlType type(int position)
    {
        return field(position).type();
    }

    public String cqlName()
    {
        return name;
    }

    public static CqlUdt read(Input input, CassandraTypes cassandraTypes)
    {
        Builder builder = CqlUdt.builder(input.readString(), input.readString());
        int numFields = input.readInt();
        for (int field = 0; field < numFields; field++)
        {
            builder.withField(input.readString(), CqlField.CqlType.read(input, cassandraTypes));
        }
        return builder.build();
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeString(this.keyspace);
        output.writeString(this.name);
        output.writeInt(this.fields.size());
        for (CqlField field : this.fields)
        {
            output.writeString(field.name());
            field.type().write(output);
        }
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CqlUdt that = (CqlUdt) other;
        return this.internalType() == that.internalType()
               && Objects.equals(this.keyspace, that.keyspace)
               && Objects.equals(this.name, that.name)
               && Objects.equals(this.fields, that.fields);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlUdt>
    {
        private final CassandraTypes cassandraTypes;

        public Serializer(CassandraTypes cassandraTypes)
        {
            this.cassandraTypes = cassandraTypes;
        }

        @Override
        public CqlUdt read(Kryo kryo, Input input, Class type)
        {
            return CqlUdt.read(input, cassandraTypes);
        }

        @Override
        public void write(Kryo kryo, Output output, CqlUdt udt)
        {
            udt.write(output);
        }
    }

    @SuppressWarnings("unchecked")
    public static UDTValue toUserTypeValue(CassandraVersion version, CqlUdt udt, @NotNull Object value)
    {
        Map<String, Object> values = (Map<String, Object>) value;
        UDTValue udtValue = UserTypeHelper.newUDTValue(toUserType(udt));
        int position = 0;
        for (CqlField field : udt.fields())
        {
            setNullableInnerValue(version, udtValue, (CqlType) field.type(), position++, values.get(field.name()));
        }
        return udtValue;
    }

    // Set inner value for UDTs or Tuples
    public static void setNullableInnerValue(CassandraVersion version,
                                             SettableByIndexData<?> udtValue,
                                             CqlType type,
                                             int position,
                                             @Nullable Object value)
    {
        type.setInnerValue(udtValue, position, value == null ? null : type.convertForCqlWriter(value, version));
    }

    public static UserType toUserType(CqlUdt udt)
    {
        List<UserType.Field> fields = udt.fields().stream()
                .map(field -> UserTypeHelper.newField(field.name(),
                                                      ((CqlType) field.type()).driverDataType()))
                .collect(Collectors.toList());
        return UserTypeHelper.newUserType(udt.keyspace(), udt.name(), true, fields, ProtocolVersion.V3);
    }
}
