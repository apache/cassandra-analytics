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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.BridgeUdtValue;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.UUIDs;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.Tuple2;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.asJavaIterable;

@SuppressWarnings("unchecked")
public final class SqlToCqlTypeConverter implements Serializable
{
    public static final String ASCII = "ascii";
    public static final String BIGINT = "bigint";
    public static final String BLOB = "blob";
    public static final String BOOLEAN = "boolean";
    public static final String COUNTER = "counter";
    public static final String CUSTOM = "custom";
    public static final String DATE = "date";
    public static final String DECIMAL = "decimal";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String FROZEN = "frozen";
    public static final String INET = "inet";
    public static final String INT = "int";
    public static final String LIST = "list";
    public static final String MAP = "map";
    public static final String SET = "set";
    public static final String SMALLINT = "smallint";
    public static final String TEXT = "text";
    public static final String TIME = "time";
    public static final String UUID = "uuid";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIMEUUID = "timeuuid";
    public static final String TINYINT = "tinyint";
    public static final String TUPLE = "tuple";
    public static final String UDT = "udt";
    public static final String VARCHAR = "varchar";
    public static final String VARINT = "varint";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlToCqlTypeConverter.class);
    private static final NoOp<Object> NO_OP_CONVERTER = new NoOp<>();
    private static final LongConverter LONG_CONVERTER = new LongConverter();
    private static final BytesConverter BYTES_CONVERTER = new BytesConverter();
    private static final BigDecimalConverter BIG_DECIMAL_CONVERTER = new BigDecimalConverter();
    private static final IntegerConverter INTEGER_CONVERTER = new IntegerConverter();
    public static final TimestampConverter TIMESTAMP_CONVERTER = new TimestampConverter();
    private static final MicroSecondsTimestampConverter MICROSECONDS_TIMESTAMP_CONVERTER =
    new MicroSecondsTimestampConverter();
    public static final TimeConverter TIME_CONVERTER = new TimeConverter();
    private static final UUIDConverter UUID_CONVERTER = new UUIDConverter();
    private static final BigIntegerConverter BIG_INTEGER_CONVERTER = new BigIntegerConverter();
    private static final TimeUUIDConverter TIME_UUID_CONVERTER = new TimeUUIDConverter();
    private static final InetAddressConverter INET_ADDRESS_CONVERTER = new InetAddressConverter();
    public static final DateConverter DATE_CONVERTER = new DateConverter();

    private SqlToCqlTypeConverter()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Method to get appropriate converter for a given CQL datatype
     *
     * @param cqlType the Cassandra data type from which a converter is needed
     * @return a type converter that knows how to handle the appropriate type
     */
    public static Converter<?> getConverter(CqlField.CqlType cqlType)
    {
        String cqlName = cqlType.name().toLowerCase();
        switch (cqlName)
        {
            case CUSTOM:
                return determineCustomConvert((CqlField.CqlCustom) cqlType);
            case ASCII:
                return NO_OP_CONVERTER;
            case BIGINT:
                return LONG_CONVERTER;
            case BLOB:
                return BYTES_CONVERTER;
            case BOOLEAN:
                return NO_OP_CONVERTER;
            case COUNTER:
                return NO_OP_CONVERTER;
            case DECIMAL:
                return BIG_DECIMAL_CONVERTER;
            case DOUBLE:
                return NO_OP_CONVERTER;
            case FLOAT:
                return NO_OP_CONVERTER;
            case FROZEN:
                assert cqlType instanceof CqlField.CqlFrozen;
                CqlField.CqlFrozen frozen = (CqlField.CqlFrozen) cqlType;
                return getConverter(frozen.inner());
            case INT:
                return INTEGER_CONVERTER;
            case TEXT:
                return NO_OP_CONVERTER;
            case TIMESTAMP:
                return TIMESTAMP_CONVERTER;
            case TIME:
                return TIME_CONVERTER;
            case UUID:
                return UUID_CONVERTER;
            case VARCHAR:
                return NO_OP_CONVERTER;
            case VARINT:
                return BIG_INTEGER_CONVERTER;
            case TIMEUUID:
                return TIME_UUID_CONVERTER;
            case INET:
                return INET_ADDRESS_CONVERTER;
            case DATE:
                return DATE_CONVERTER;
            case SMALLINT:
                return NO_OP_CONVERTER;
            case TINYINT:
                return NO_OP_CONVERTER;
            case LIST:
                return new ListConverter<>((CqlField.CqlCollection) cqlType);
            case MAP:
                assert cqlType instanceof CqlField.CqlMap;
                return new MapConverter<>((CqlField.CqlMap) cqlType);
            case SET:
                return new SetConverter<>((CqlField.CqlCollection) cqlType);
            case TUPLE:
                return NO_OP_CONVERTER;
            default:
                if (cqlType.internalType() == CqlField.CqlType.InternalType.Udt)
                {
                    assert cqlType instanceof CqlField.CqlUdt;
                    return new UdtConverter((CqlField.CqlUdt) cqlType);
                }
                LOGGER.warn("Unable to match type={}. Defaulting to NoOp Converter", cqlName);
                return NO_OP_CONVERTER;
        }
    }

    public static Converter<?> integerConverter()
    {
        return INTEGER_CONVERTER;
    }

    public static Converter<?> microsecondsTimestampConverter()
    {
        return MICROSECONDS_TIMESTAMP_CONVERTER;
    }

    static boolean canConvertToLong(Object object)
    {
        return object instanceof Long
               || object instanceof Integer
               || object instanceof Short
               || object instanceof Byte;
    }

    static long convertToLong(Object object)
    {
        return ((Number) object).longValue();
    }

    private static Converter<?> determineCustomConvert(CqlField.CqlCustom customType)
    {
        Preconditions.checkArgument(customType.name().equalsIgnoreCase(CUSTOM), "Non-custom types are not supported");
        if (customType.customTypeClassName().equalsIgnoreCase("org.apache.cassandra.db.marshal.DateType"))
        {
            return TIMESTAMP_CONVERTER;
        }
        else
        {
            return NO_OP_CONVERTER;
        }
    }

    abstract static class Converter<T> implements Serializable
    {
        public T convert(Object object)
        {
            return convertInternal(object);
        }

        abstract T convertInternal(Object object);
    }

    private abstract static class NullableConverter<T> extends Converter<T>
    {
        @Override
        public T convert(Object object)
        {
            return object != null ? super.convert(object) : null;
        }
    }

    static class NoOp<T> extends NullableConverter<T>
    {
        @Override
        public T convertInternal(Object object)
        {
            return (T) object;
        }

        @Override
        public String toString()
        {
            return "NoOp";
        }
    }

    static class LongConverter extends NullableConverter<Long>
    {
        @Override
        public Long convertInternal(Object object)
        {
            if (canConvertToLong(object))
            {
                return convertToLong(object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for LONG from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Long";
        }
    }

    static class BytesConverter extends NullableConverter<ByteBuffer>
    {
        @Override
        public ByteBuffer convertInternal(Object object)
        {
            if (object instanceof ByteBuffer)
            {
                return (ByteBuffer) object;
            }
            else if (object instanceof byte[])
            {
                return ByteBuffer.wrap((byte[]) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for BYTES from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Bytes";
        }
    }

    static class BigDecimalConverter extends NullableConverter<BigDecimal>
    {
        @Override
        public BigDecimal convertInternal(Object object)
        {
            if (object instanceof BigDecimal)
            {
                return (BigDecimal) object;
            }
            else if (object instanceof scala.math.BigDecimal)
            {
                return ((scala.math.BigDecimal) object).bigDecimal();
            }
            else if (object instanceof String)
            {
                return new BigDecimal((String) object);
            }
            else if (object instanceof Double)
            {
                return BigDecimal.valueOf((Double) object);
            }
            else if (object instanceof Float)
            {
                return BigDecimal.valueOf((Float) object);
            }
            else if (object instanceof Long)
            {
                return BigDecimal.valueOf((Long) object);
            }
            else if (object instanceof Integer)
            {
                return BigDecimal.valueOf((Integer) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for DECIMAL from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Decimal";
        }
    }

    static class InetAddressConverter extends NullableConverter<InetAddress>
    {
        @Override
        @SuppressWarnings("UnstableApiUsage")
        public InetAddress convertInternal(Object object)
        {
            if (object instanceof InetAddress)
            {
                return (InetAddress) object;
            }
            else if (object instanceof String)
            {
                return InetAddresses.forString((String) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for INET from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Inet";
        }
    }

    static class IntegerConverter extends NullableConverter<Integer>
    {
        @Override
        public Integer convertInternal(Object object)
        {
            if (object instanceof Integer
                || object instanceof Short
                || object instanceof Byte)
            {
                return ((Number) object).intValue();
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for INTEGER from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Integer";
        }
    }

    static class BigIntegerConverter extends NullableConverter<BigInteger>
    {
        @Override
        public BigInteger convertInternal(Object object)
        {
            if (object instanceof BigInteger)
            {
                return (BigInteger) object;
            }
            else if (object instanceof scala.math.BigInt)
            {
                return ((scala.math.BigInt) object).bigInteger();
            }
            else if (object instanceof String)
            {
                return new BigInteger((String) object);
            }
            else if (object instanceof Long)
            {
                return BigInteger.valueOf((Long) object);
            }
            else if (object instanceof Integer)
            {
                return BigInteger.valueOf((Integer) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for VARCHAR from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "VarChar";
        }
    }

    @SuppressWarnings("serial")
    static class MicroSecondsTimestampConverter extends Converter<Long>
    {
        /**
         * Returns the time since epoch (January 1, 1970) in microseconds, as specified by
         * <a
         * href="https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlInsert.html#cqlInsert__timestamp-value">
         * the documentation</a>.
         *
         * @param object the input object to convert
         * @return the time since epoch in microseconds
         * @throws RuntimeException when the object cannot be converted to timestamp
         */
        @Override
        public Long convertInternal(Object object)
        {
            if (object instanceof Date)
            {
                Instant instant = ((Date) object).toInstant();
                return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
                       + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
            }
            else if (canConvertToLong(object))
            {
                return convertToLong(object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for TIMESTAMP from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Timestamp";
        }
    }

    public static class TimestampConverter extends NullableConverter<Date>
    {
        /**
         * Returns a Date representing the number of milliseconds since the standard base time known as the epoch
         * (January 1 1970 at 00:00:00 GMT), as specified by <a
         * href="https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/timestamp_type_r.html">the
         * documentation</a>.
         *
         * @param object the input object to convert
         * @return the Date
         * @throws RuntimeException when the object cannot be converted to timestamp
         */
        @Override
        public Date convertInternal(Object object)
        {
            if (object instanceof Date)
            {
                return (Date) object;
            }
            else if (object instanceof Long)
            {
                return new Date((Long) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for DATE from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Timestamp";
        }
    }

    public static class DateConverter extends NullableConverter<Integer>
    {
        @Override
        public String toString()
        {
            return "Date";
        }

        @Override
        public Integer convertInternal(Object object)
        {
            if (object instanceof Date)
            {
                return fromDate((Date) object);
            }
            else if (object instanceof Long)
            {
                return fromMillisSinceEpoch((Long) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for DATE from " + object.getClass().getTypeName());
            }
        }

        protected int fromDate(Date value)
        {
            long millisSinceEpoch = value.getTime();
            return fromMillisSinceEpoch(millisSinceEpoch);
        }

        protected int fromMillisSinceEpoch(long millisSinceEpoch)
        {
            // NOTE: This code is lifted from org.apache.cassandra.serializers.SimpleDateSerializer#timeInMillisToDay.
            //       Reproduced here due to the difficulties of referencing classes from specific versions of Cassandra
            //       in the SBW.
            int result = (int) TimeUnit.MILLISECONDS.toDays(millisSinceEpoch);
            result -= Integer.MIN_VALUE;
            return result;
        }
    }

    public static class TimeConverter extends NullableConverter<Long>
    {
        @Override
        @SuppressWarnings("deprecation")
        public Long convertInternal(Object object)
        {
            if (object instanceof Long)
            {
                long result = (Long) object;
                if (result < 0 || result >= TimeUnit.DAYS.toNanos(1))
                {
                    throw new IllegalArgumentException("Input value out of bounds for Cassandra Time field: " + result);
                }
                return (long) object;
            }
            else if (object instanceof Timestamp)
            {
                // Here, we truncate the date information and just use the time
                Timestamp timestamp = (Timestamp) object;
                long rawTime = 0;
                rawTime += TimeUnit.HOURS.toNanos(timestamp.getHours());
                rawTime += TimeUnit.MINUTES.toNanos(timestamp.getMinutes());
                rawTime += TimeUnit.SECONDS.toNanos(timestamp.getSeconds());
                rawTime += timestamp.getNanos();
                return rawTime;
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for TIME from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Time";
        }
    }

    static class UUIDConverter extends NullableConverter<UUID>
    {
        @Override
        public UUID convertInternal(Object object)
        {
            if (object instanceof UUID)
            {
                return (UUID) object;
            }
            else if (object instanceof String)
            {
                return java.util.UUID.fromString((String) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for UUID from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "UUID";
        }
    }

    static class TimeUUIDConverter extends NullableConverter<UUID>
    {
        @Override
        public UUID convertInternal(Object object)
        {
            UUID result;
            if (object instanceof UUID)
            {
                result = (UUID) object;
            }
            else if (object instanceof String)
            {
                result = java.util.UUID.fromString((String) object);
            }
            else if (object instanceof Long)
            {
                result = UUIDs.startOf((Long) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for TIMEUUID from " + object.getClass().getTypeName());
            }
            if (result.version() == 1)
            {
                return result;
            }
            else
            {
                throw new RuntimeException("Attempted to convert a non-Timestamp UUID to a TimeUUID - UUID was " + object);
            }
        }

        @Override
        public String toString()
        {
            return "TimeUUID";
        }
    }

    static class ListConverter<E> extends NullableConverter<List<E>>
    {
        private final Converter<?> innerConverter;

        ListConverter(CqlField.CqlCollection cqlType)
        {
            innerConverter = getConverter(cqlType.type());
        }

        @Override
        public List<E> convertInternal(Object object)
        {
            if (object instanceof scala.collection.Iterable)
            {
                return makeList(asJavaIterable((scala.collection.Iterable<?>) object));
            }
            else if (object instanceof Iterable)
            {
                return makeList((Iterable<?>) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for LIST from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "List";
        }

        private List<E> makeList(Iterable<?> iterable)
        {
            List<E> list = new ArrayList<>();
            for (Object object : iterable)
            {
                list.add((E) innerConverter.convert(object));
            }
            return list;
        }
    }

    static class SetConverter<E> extends NullableConverter<Set<E>>
    {
        private final Converter<?> innerConverter;

        SetConverter(CqlField.CqlCollection cqlType)
        {
            innerConverter = getConverter(cqlType.type());
        }

        @Override
        public Set<E> convertInternal(Object object)
        {
            if (object instanceof scala.collection.Iterable)
            {
                return makeSet(asJavaIterable((scala.collection.Iterable<?>) object));
            }
            else if (object instanceof Iterable)
            {
                return makeSet((Iterable<?>) object);
            }
            else
            {
                throw new RuntimeException("Unsupported conversion for SET from " + object.getClass().getTypeName());
            }
        }

        @Override
        public String toString()
        {
            return "Set<" + innerConverter.toString() + ">";
        }

        private Set<E> makeSet(Iterable<?> iterable)
        {
            Set<E> set = new HashSet<>();
            for (Object object : iterable)
            {
                set.add((E) innerConverter.convert(object));
            }
            return set;
        }
    }

    static class MapConverter<K, V> extends NullableConverter<Map<K, V>>
    {
        private final Converter<?> keyConverter;
        private final Converter<?> valConverter;

        MapConverter(CqlField.CqlMap cqlType)
        {
            keyConverter = getConverter(cqlType.keyType());
            valConverter = getConverter(cqlType.valueType());
        }

        @Override
        public Map<K, V> convertInternal(Object object)
        {
            if (object instanceof scala.collection.Iterable)
            {
                return makeMap(asJavaIterable((scala.collection.Iterable<?>) object));
            }
            else if (object instanceof Iterable)
            {
                return makeMap((Iterable<?>) object);
            }
            else if (object instanceof Map)
            {
                return makeMap(((Map<K, V>) object).entrySet());
            }
            throw new RuntimeException("Unsupported conversion for MAP from " + object.getClass().getTypeName());
        }

        @Override
        public String toString()
        {
            return "Map<" + keyConverter.toString() + ", " + valConverter.toString() + '>';
        }

        private Map<K, V> makeMap(Iterable<?> iterable)
        {
            Object key;
            Object value;
            Map<K, V> map = new HashMap<>();
            for (Object object : iterable)
            {
                if (object instanceof Map.Entry)
                {
                    key = ((Map.Entry<K, V>) object).getKey();
                    value = ((Map.Entry<K, V>) object).getValue();
                }
                else if (object instanceof scala.Tuple2)
                {
                    key = ((Tuple2<K, V>) object)._1();
                    value = ((Tuple2<K, V>) object)._2();
                }
                else
                {
                    throw new RuntimeException("Unsupported conversion for key/value pair in MAP from " + object.getClass().getTypeName());
                }
                map.put((K) keyConverter.convert(key), (V) valConverter.convert(value));
            }
            return map;
        }
    }

    public static class UdtConverter extends NullableConverter<BridgeUdtValue>
    {
        private final String name;
        private final HashMap<String, Converter<?>> converters;

        UdtConverter(CqlField.CqlUdt udt)
        {
            this.name = udt.cqlName();
            this.converters = new HashMap<>();
            for (CqlField f : udt.fields())
            {
                converters.put(f.name(), getConverter(f.type()));
            }
        }

        @Override
        public BridgeUdtValue convertInternal(Object object)
        {
            if (object instanceof GenericRowWithSchema)
            {
                Map<String, Object> udtMap = makeUdtMap((GenericRowWithSchema) object);
                return new BridgeUdtValue(name, udtMap);
            }
            throw new RuntimeException("Unsupported conversion for UDT from " + object.getClass().getTypeName());
        }

        @Override
        public String toString()
        {
            return String.format("UDT[%s]", name);
        }

        // Unfortunately, we don't have easy access to the bridge here.
        // Rather than trying to create an actual UDTValue here, we will push
        // that responsibility down to the SSTableWriter Implementation
        private Map<String, Object> makeUdtMap(GenericRowWithSchema row)
        {
            Map<String, Object> result = new HashMap<>();
            for (String fieldName : row.schema().fieldNames())
            {
                Converter<?> converter = converters.get(fieldName);
                Object val = row.get(row.fieldIndex(fieldName));
                result.put(fieldName, converter.convert(val));
            }
            return result;
        }
    }
}
