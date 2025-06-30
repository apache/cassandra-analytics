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

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.spark.utils.Preconditions;
import javax.annotation.Nonnull;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Type conversion from the source type to the target type.
 *
 * @param <T> target type
 */
public interface TypeConversion<T>
{
    /**
     * Type mapping from the source type to the target type.
     *
     * @return the type mapping.
     */
    TypeMapping typeMapping();

    /**
     * Convert value from the source type to the target type.
     * Runtime exceptions are thrown on invalid argument or conversion failure.
     *
     * @param fieldSchema avro schema to loop up the source schema
     * @param fieldValue  value in the source type. The value cannot be null.
     * @return the converted value
     */
    T convert(Schema fieldSchema, @Nonnull Object fieldValue);

    /**
     * Registry for {@link TypeConversion} to perform register and lookup
     */
    interface Registry
    {
        /**
         * Register a {@link TypeConversion}
         *
         * @param typeConversion
         */
        void register(TypeConversion<?> typeConversion);

        /**
         * Look up {@link TypeConversion} based on the field schema.
         * It uses type and logical type info defined the field schema to look up
         *
         * @param fieldSchema
         * @return a registered {@link TypeConversion} or null if none is found
         */
        TypeConversion<?> lookup(Schema fieldSchema);
    }

    /**
     * A simple data class that represents the type mapping between the source type and target type.
     * A type mapping is used as the key for {@link TypeConversion} lookup.
     */
    class TypeMapping
    {
        public final String sourceTypeName;
        public final String targetTypeName;
        public final String cqlTypeName;

        public static TypeMapping of(String sourceTypeName, String targetTypeName, String cqlTypeName)
        {
            return new TypeMapping(sourceTypeName, targetTypeName, cqlTypeName);
        }

        public static TypeMapping of(String sourceTypeName, String targetTypeName)
        {
            return of(sourceTypeName, targetTypeName, targetTypeName);
        }

        private TypeMapping(String sourceTypeName, String targetTypeName, String cqlTypeName)
        {
            Preconditions.checkNotNull(sourceTypeName);
            Preconditions.checkNotNull(targetTypeName);
            Preconditions.checkNotNull(cqlTypeName);
            this.sourceTypeName = sourceTypeName;
            this.targetTypeName = targetTypeName;
            this.cqlTypeName = cqlTypeName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceTypeName, targetTypeName, cqlTypeName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj instanceof TypeMapping))
            {
                return false;
            }

            TypeMapping that = (TypeMapping) obj;
            return Objects.equals(this.sourceTypeName, that.sourceTypeName)
                   && Objects.equals(this.targetTypeName, that.targetTypeName)
                   && Objects.equals(this.cqlTypeName, that.cqlTypeName);
        }
    }

    /**
     * Validate the input is the expected type
     *
     * @param conversionName class name of type
     * @param input          value to convert
     * @param expectedType   expected class of type
     * @throws IllegalArgumentException if type mismatches
     */
    static void ensureInputValueType(String conversionName, Object input, Class<?> expectedType)
    {
        Class<?> inputType = input.getClass();
        // input type cannot cast into the target type
        if (!expectedType.isAssignableFrom(inputType))
        {
            throw new IllegalArgumentException(String.format("%s expects %s input, but has %s",
                                                             conversionName,
                                                             expectedType.getSimpleName(),
                                                             inputType.getSimpleName()));
        }
    }

    /**
     * Converts either {@link String} or {@link Utf8} to {@link UUID}.
     */
    class UUIDConversion implements TypeConversion<UUID>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("string", "uuid");

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public UUID convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            String uuidStr;
            if (fieldValue instanceof Utf8)
            {
                uuidStr = ((Utf8) fieldValue).toString();
            }
            else if (fieldValue instanceof String)
            {
                uuidStr = (String) fieldValue;
            }
            else
            {
                throw new IllegalArgumentException("UUIDConversion expects String input, but has " +
                                                   fieldValue.getClass().getSimpleName());
            }

            return UUID.fromString(uuidStr);
        }
    }

    /**
     * Converts date value in {@link Integer} to {@link LocalDate}.
     * The date value represents epoch days.
     */
    class DateConversion implements TypeConversion<LocalDate>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("int", "date", "date");

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public LocalDate convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, Integer.class);
            Integer dateValue = (Integer) fieldValue;
            return LocalDate.ofEpochDay(dateValue);
        }
    }

    class InetAddressConversion implements TypeConversion<InetAddress>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("bytes", "inet", "inet");

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public InetAddress convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, ByteBuffer.class);

            ByteBuffer bb = (ByteBuffer) fieldValue;
            try
            {
                return InetAddress.getByAddress(getArray(bb));
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    static byte[] getArray(ByteBuffer buffer)
    {
        return getArray(buffer, buffer.position(), buffer.remaining());
    }

    static byte[] getArray(ByteBuffer buffer, int position, int length)
    {
        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + position;
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }

        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        ByteBuffer dup = buffer.duplicate();
        dup.position(position).limit(position + length);
        dup.get(bytes);
        return bytes;
    }

    /**
     * Converts {@link GenericFixed} to {@link BigInteger}.
     */
    class VarIntConversion implements TypeConversion<BigInteger>
    {
        // The avro schema of varint has the type "FIXED", logical type "decimal" and cqlType "varint"
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("fixed", "decimal", "varint");

        private final DecimalConversion decimalConversion = new DecimalConversion();

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public BigInteger convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            int scale = ((LogicalTypes.Decimal) fieldSchema.getLogicalType()).getScale();
            Preconditions.checkState(scale == 0, "Not a valid varint type");
            return decimalConversion.convert(fieldSchema, fieldValue).toBigInteger();
        }
    }

    /**
     * Converts {@link GenericFixed} to {@link BigDecimal}.
     */
    class DecimalConversion implements TypeConversion<BigDecimal>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("fixed", "decimal", "decimal");

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public BigDecimal convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, GenericFixed.class);

            GenericFixed fixed = (GenericFixed) fieldValue;
            int scale = ((LogicalTypes.Decimal) fieldSchema.getLogicalType()).getScale();
            return new BigDecimal(new BigInteger(fixed.bytes()), scale);
        }
    }

    /**
     * Converts {@link Long} to {@link Date}.
     */
    class TimestampConversion implements TypeConversion<Date>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("long", "timestamp-micros", "timestamp");

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public Date convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, Long.class);
            Long epochTimeInMicros = (Long) fieldValue;
            // timestamp represents epoch in milliseconds
            return new Date(TimeUnit.MICROSECONDS.toMillis(epochTimeInMicros));
        }
    }

    /**
     * Converts {@link GenericData.Array} (, which is a {@link List},) into {@link Set}
     * The conversion is done recursively, meaning the elements of the input are converted too.
     */
    class SetConversion implements TypeConversion<Set<?>>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("array",
                                                                       AvroConstants.ARRAY_BASED_SET_NAME);

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public Set<?> convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, List.class);
            return ((List<?>) fieldValue).stream()
                                         .map(value -> RecordReader.get()
                                                               .convert(fieldSchema.getElementType(), value))
                                         .collect(toSet());
        }
    }

    /**
     * Converts {@link GenericData.Array} (, which is a {@link List},) of {@link GenericRecord} into {@link Map}
     * The contained {@link GenericRecord} should be an avro key-value record.
     * The conversion is done recursively, meaning the keys and values of the input are converted too.
     */
    class MapConversion implements TypeConversion<Map<?, ?>>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("array",
                                                                       AvroConstants.ARRAY_BASED_MAP_NAME);

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public Map<?, ?> convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, List.class);

            Schema subschema = fieldSchema.getElementType();
            Preconditions.checkArgument(subschema.getType() == Schema.Type.RECORD
                                        && subschema.getField(AvroConstants.ARRAY_BASED_MAP_KEY_NAME) != null
                                        && subschema.getField(AvroConstants.ARRAY_BASED_MAP_VALUE_NAME) != null,
                                        "MapConversion expects List to contain key value pairs, but has %s",
                                        subschema);

            // The second check assures that the subtype is a GenericRecord, i.e. RECORD
            List<GenericRecord> input = (List<GenericRecord>) fieldValue;
            return input.stream().collect(toMap(this::readKey, this::readValue));
        }

        private Object readKey(GenericRecord kv)
        {
            return readKvRec(kv, AvroConstants.ARRAY_BASED_MAP_KEY_NAME);
        }

        private Object readValue(GenericRecord kv)
        {
            return readKvRec(kv, AvroConstants.ARRAY_BASED_MAP_VALUE_NAME);
        }

        private Object readKvRec(GenericRecord kv, String name)
        {
            return RecordReader.get().read(kv, name);
        }
    }

    /**
     * Converts a UDT {@link GenericRecord} into {@link Map}
     * The {@link GenericRecord} should be an avro key-value record.
     * The conversion is done recursively, meaning the values of the input are converted too, the keys should always be strings.
     */
    class UdtConversion implements TypeConversion<Map<?, ?>>
    {
        private static final TypeMapping TYPE_MAPPING = TypeMapping.of("record",
                                                                       AvroConstants.RECORD_BASED_UDT_NAME);

        @Override
        public TypeMapping typeMapping()
        {
            return TYPE_MAPPING;
        }

        @Override
        public Map<?, ?> convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, GenericRecord.class);

            Preconditions.checkArgument(fieldSchema.getType() == Schema.Type.RECORD
                                        && fieldValue instanceof GenericRecord,
                                        "UdtConversion expects a GenericRecord, but has %s",
                                        fieldSchema);

            GenericRecord record = (GenericRecord) fieldValue;
            return fieldSchema.getFields().stream().collect(toMap(Schema.Field::name, field -> readValue(record, field.name())));
        }

        private Object readValue(GenericRecord kv, String name)
        {
            return RecordReader.get().read(kv, name);
        }
    }

    /**
     * Converts {@link GenericData.Array} (, which is a {@link List},) into {@link List}
     * The conversion is identity. It is basically from list to list.
     * The conversion is done recursively, meaning the elements of the input are converted too.
     */
    class ListConversion implements TypeConversion<List<?>>
    {
        public static final TypeMapping LIST_IDENTITY_MAPPING = TypeMapping.of("array",
                                                                               "array");

        @Override
        public TypeMapping typeMapping()
        {
            return LIST_IDENTITY_MAPPING;
        }

        @Override
        public List<?> convert(Schema fieldSchema, @Nonnull Object fieldValue)
        {
            TypeConversion.ensureInputValueType(getClass().getSimpleName(), fieldValue, List.class);

            return ((List<?>) fieldValue).stream()
                                         .map(value -> RecordReader.get()
                                                               .convert(fieldSchema.getElementType(), value))
                                         .collect(toList());
        }
    }
}
