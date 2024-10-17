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

package org.apache.cassandra.spark.reader;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.getSparkSql;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.bigDecimals;
import static org.quicktheories.generators.SourceDSL.bigIntegers;
import static org.quicktheories.generators.SourceDSL.dates;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;
import static org.quicktheories.generators.SourceDSL.strings;

public class DataTypeSerializationTests
{
    private static final int MAX_TESTS = 1000;

    @Test
    public void testVarInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Decimal.class, toVarInt(bridge, BigInteger.valueOf(500L)));
            assertEquals(Decimal.apply(500), toVarInt(bridge, BigInteger.valueOf(500L)));
            assertNotSame(Decimal.apply(501), toVarInt(bridge, BigInteger.valueOf(500L)));
            assertEquals(Decimal.apply(-1), toVarInt(bridge, BigInteger.valueOf(-1L)));
            assertEquals(Decimal.apply(Long.MAX_VALUE), toVarInt(bridge, BigInteger.valueOf(Long.MAX_VALUE)));
            assertEquals(Decimal.apply(Long.MIN_VALUE), toVarInt(bridge, BigInteger.valueOf(Long.MIN_VALUE)));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), toVarInt(bridge, BigInteger.valueOf(Integer.MAX_VALUE)));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), toVarInt(bridge, BigInteger.valueOf(Integer.MIN_VALUE)));
            BigInteger veryLargeValue = BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), toVarInt(bridge, veryLargeValue));
            qt().withExamples(MAX_TESTS)
                .forAll(bigIntegers().ofBytes(128))
                .checkAssert(integer -> assertEquals(Decimal.apply(integer), toVarInt(bridge, integer)));
        });
    }

    @Test
    public void testInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Integer.class, toInt(bridge, 5));
            assertEquals(999, bridge.aInt().deserializeToType(getSparkSql(bridge), ByteBuffer.allocate(4).putInt(0, 999)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals(integer, toInt(bridge, integer)));
        });
    }

    @Test
    public void testBoolean()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Boolean.class, toBool(bridge, true));
            assertTrue((Boolean) toBool(bridge, true));
            assertFalse((Boolean) toBool(bridge, false));
        });
    }

    @Test
    public void testTimeUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(UTF8String.class, toTimeUUID(bridge, RandomUtils.getRandomTimeUUIDForTesting()));
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = RandomUtils.getRandomTimeUUIDForTesting();
                assertEquals(expected.toString(), toTimeUUID(bridge, expected).toString());
            }
        });
    }

    @Test
    public void testUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(UTF8String.class, toUUID(bridge, UUID.randomUUID()));
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = UUID.randomUUID();
                assertEquals(expected.toString(), toUUID(bridge, expected).toString());
            }
        });
    }

    @Test
    public void testLong()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Long.class, toBigInt(bridge, Long.MAX_VALUE));
            assertEquals(Long.MAX_VALUE, bridge.bigint().deserializeToType(getSparkSql(bridge),
                                                                           ByteBuffer.allocate(8).putLong(0, Long.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((long) integer, toBigInt(bridge, (long) integer)));
            assertEquals(Long.MAX_VALUE, toJavaType(bridge, CassandraBridge::bigint, Long.MAX_VALUE));
            assertEquals(Long.MIN_VALUE, toJavaType(bridge, CassandraBridge::bigint, Long.MIN_VALUE));
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertEquals(aLong, toBigInt(bridge, aLong)));
        });
    }

    @Test
    public void testDecimal()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Decimal.class, toDecimal(bridge, BigDecimal.valueOf(500L)));
            assertEquals(Decimal.apply(500), toDecimal(bridge, BigDecimal.valueOf(500L)));
            assertNotSame(Decimal.apply(501), toDecimal(bridge, BigDecimal.valueOf(500L)));
            assertEquals(Decimal.apply(-1), toDecimal(bridge, BigDecimal.valueOf(-1L)));
            assertEquals(Decimal.apply(Long.MAX_VALUE), toDecimal(bridge, BigDecimal.valueOf(Long.MAX_VALUE)));
            assertEquals(Decimal.apply(Long.MIN_VALUE), toDecimal(bridge, BigDecimal.valueOf(Long.MIN_VALUE)));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), toDecimal(bridge, BigDecimal.valueOf(Integer.MAX_VALUE)));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), toDecimal(bridge, BigDecimal.valueOf(Integer.MIN_VALUE)));
            BigDecimal veryLargeValue = BigDecimal.valueOf(Integer.MAX_VALUE).multiply(BigDecimal.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), toDecimal(bridge, veryLargeValue));
            qt().withExamples(MAX_TESTS)
                .forAll(bigDecimals().ofBytes(128).withScale(10))
                .checkAssert(decimal -> assertEquals(Decimal.apply(decimal), bridge.decimal().deserializeToType(getSparkSql(bridge),
                                                                                                                bridge.decimal().serialize(decimal))));
        });
    }

    @Test
    public void testFloat()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Float.class, toFloat(bridge, Float.MAX_VALUE));
            assertEquals(Float.MAX_VALUE, bridge.aFloat().deserializeToType(getSparkSql(bridge),
                                                                            ByteBuffer.allocate(4).putFloat(0, Float.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((float) integer, toFloat(bridge, (float) integer)));
            assertEquals(Float.MAX_VALUE, toFloat(bridge, Float.MAX_VALUE));
            assertEquals(Float.MIN_VALUE, toFloat(bridge, Float.MIN_VALUE));
            qt().withExamples(MAX_TESTS)
                .forAll(floats().any())
                .checkAssert(aFloat -> assertEquals(aFloat, toFloat(bridge, aFloat)));
        });
    }

    @Test
    public void testDouble()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Double.class, toDouble(bridge, Double.MAX_VALUE));
            assertEquals(Double.MAX_VALUE, bridge.aDouble().deserializeToType(getSparkSql(bridge),
                                                                              ByteBuffer.allocate(8).putDouble(0, Double.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((double) integer, toDouble(bridge, (double) integer)));
            assertEquals(Double.MAX_VALUE, toDouble(bridge, Double.MAX_VALUE));
            assertEquals(Double.MIN_VALUE, toDouble(bridge, Double.MIN_VALUE));
            qt().withExamples(MAX_TESTS)
                .forAll(doubles().any())
                .checkAssert(aDouble -> assertEquals(aDouble, toDouble(bridge, aDouble)));
        });
    }

    @Test
    public void testAscii()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(UTF8String.class, toAscii(bridge, "abc"));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, toAscii(bridge, string).toString()));
        });
    }

    @Test
    public void testText()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(UTF8String.class, toText(bridge, "abc"));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, toText(bridge, string).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, toText(bridge, string).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertEquals(string, toText(bridge, string).toString()));
        });
    }

    @Test
    public void testVarchar()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(UTF8String.class, toVarChar(bridge, "abc"));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, toVarChar(bridge, string).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, toVarChar(bridge, string).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertEquals(string, toVarChar(bridge, string).toString()));
        });
    }

    @Test
    public void testInet()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(byte[].class, toInet(bridge, RandomUtils.randomInet()));
            for (int test = 0; test < MAX_TESTS; test++)
            {
                InetAddress expected = RandomUtils.randomInet();
                assertArrayEquals(expected.getAddress(), (byte[]) toInet(bridge, expected));
            }
        });
    }

    @Test
    public void testDate()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Integer.class, toDate(bridge, 5));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals(integer - Integer.MIN_VALUE, toDate(bridge, integer)));
        });
    }

    @Test
    public void testTime()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Long.class, toTime(bridge, Long.MAX_VALUE));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((long) integer, toTime(bridge, (long) integer)));
            assertEquals(Long.MAX_VALUE, toTime(bridge, Long.MAX_VALUE));
            assertEquals(Long.MIN_VALUE, toTime(bridge, Long.MIN_VALUE));
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertEquals(aLong, toTime(bridge, aLong)));
        });
    }

    @Test
    public void testTimestamp()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            Date now = new Date();
            assertInstanceOf(Long.class, toTimestamp(bridge, now));
            assertEquals(java.sql.Timestamp.from(now.toInstant()).getTime() * 1000L, toTimestamp(bridge, now));
            qt().withExamples(MAX_TESTS)
                .forAll(dates().withMillisecondsBetween(0, Long.MAX_VALUE))
                .checkAssert(date -> assertEquals(java.sql.Timestamp.from(date.toInstant()).getTime() * 1000L, toTimestamp(bridge, date)));
        });
    }

    @Test
    public void testBlob()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(byte[].class, toBlob(bridge, ByteBuffer.wrap(RandomUtils.randomBytes(5))));
            for (int test = 0; test < MAX_TESTS; test++)
            {
                int size = RandomUtils.RANDOM.nextInt(1024);
                byte[] expected = RandomUtils.randomBytes(size);
                assertArrayEquals(expected, (byte[]) toBlob(bridge, ByteBuffer.wrap(expected)));
            }
        });
    }

    @Test
    public void testEmpty()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge ->
                                                     assertNull(toEmpty(bridge, null))
        );
    }

    @Test
    public void testSmallInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Short.class, toSmallInt(bridge, (short) 5));
            qt().forAll(integers().between(Short.MIN_VALUE, Short.MAX_VALUE))
                .checkAssert(integer -> {
                    short expected = integer.shortValue();
                    assertEquals(expected, toSmallInt(bridge, expected));
                });
        });
    }

    @Test
    public void testTinyInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertInstanceOf(Byte.class, toTinyInt(bridge, RandomUtils.randomByte()));
            for (int test = 0; test < MAX_TESTS; test++)
            {
                byte expected = RandomUtils.randomByte();
                assertEquals(expected, toTinyInt(bridge, expected));
            }
        });
    }

    @Test
    public void testSerialization()
    {
        // CassandraBridge.serialize is mostly used for unit tests
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            // BLOB,  VARINT
            assertEquals("ABC", toAscii(bridge, "ABC").toString());
            assertEquals(500L, toBigInt(bridge, 500L));
            assertEquals(true, toBool(bridge, true));
            assertEquals(false, toBool(bridge, false));

            byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
            ByteBuffer buffer = bridge.blob().serialize(ByteBuffer.wrap(bytes));
            byte[] result = new byte[4];
            buffer.get(result);
            assertArrayEquals(bytes, result);

            assertEquals(500 + Integer.MIN_VALUE, toDate(bridge, 500));
            assertEquals(Decimal.apply(500000.2038484), toDecimal(bridge, BigDecimal.valueOf(500000.2038484)));
            assertEquals(123211.023874839, toDouble(bridge, 123211.023874839));
            assertEquals(58383.23737832839f, toFloat(bridge, 58383.23737832839f));
            try
            {
                assertEquals(InetAddress.getByName("www.apache.org"),
                             InetAddress.getByAddress((byte[]) toInet(bridge, InetAddress.getByName("www.apache.org"))));
            }
            catch (UnknownHostException exception)
            {
                throw new RuntimeException(exception);
            }
            assertEquals(283848498, toInt(bridge, 283848498));
            assertEquals((short) 29, toSmallInt(bridge, (short) 29));
            assertEquals("hello world", toAscii(bridge, "hello world").toString());
            assertEquals(5002839L, toTime(bridge, 5002839L));
            Date now = new Date();
            assertEquals(now.getTime() * 1000L, toTimestamp(bridge, now));
            UUID timeUuid = RandomUtils.getRandomTimeUUIDForTesting();
            assertEquals(timeUuid, UUID.fromString(toTimeUUID(bridge, timeUuid).toString()));
            assertEquals((byte) 100, toTinyInt(bridge, (byte) 100));
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, UUID.fromString(toUUID(bridge, uuid).toString()));
            assertEquals("ABCDEFG", toVarChar(bridge, "ABCDEFG").toString());
            assertEquals(Decimal.apply(12841924), toVarInt(bridge, BigInteger.valueOf(12841924)));
        });
    }

    @Test
    public void testList()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> {
                    CqlField.CqlList list = bridge.list(type);
                    SparkType sparkType = getSparkSql(bridge).toSparkType(type);
                    List<Object> expected = IntStream.range(0, 128)
                                                     .mapToObj(index -> type.randomValue())
                                                     .collect(Collectors.toList());
                    ByteBuffer buffer = list.serialize(expected);
                    List<Object> actual = Arrays.asList(((ArrayData) list.deserializeToType(getSparkSql(bridge), buffer)).array());
                    assertEquals(expected.size(), actual.size());
                    for (int index = 0; index < expected.size(); index++)
                    {
                        assertEquals(expected.get(index), sparkType.toTestRowType(actual.get(index)));
                    }
                }));
    }

    @Test
    public void testSet()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> {
                    CqlField.CqlSet set = bridge.set(type);
                    SparkType sparkType = getSparkSql(bridge).toSparkType(type);
                    Set<Object> expected = IntStream.range(0, 128)
                                                    .mapToObj(integer -> type.randomValue())
                                                    .collect(Collectors.toSet());
                    ByteBuffer buffer = set.serialize(expected);
                    Set<Object> actual = new HashSet<>(Arrays.asList(((ArrayData) set.deserializeToType(getSparkSql(bridge), buffer)).array()));
                    assertEquals(expected.size(), actual.size());
                    for (Object value : actual)
                    {
                        assertTrue(expected.contains(sparkType.toTestRowType(value)));
                    }
                }));
    }

    @Test
    public void testMap()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((keyType, valueType) -> {
                    CqlField.CqlMap map = bridge.map(keyType, valueType);
                    SparkType keySparkType = getSparkSql(bridge).toSparkType(keyType);
                    SparkType valueSparkType = getSparkSql(bridge).toSparkType(valueType);

                    int count = keyType.cardinality(128);
                    Map<Object, Object> expected = new HashMap<>(count);
                    for (int entry = 0; entry < count; entry++)
                    {
                        Object key = null;
                        while (key == null || expected.containsKey(key))
                        {
                            key = keyType.randomValue();
                        }
                        expected.put(key, valueType.randomValue());
                    }
                    ByteBuffer buffer = map.serialize(expected);
                    ArrayBasedMapData mapData = ((ArrayBasedMapData) map.deserializeToType(getSparkSql(bridge), buffer));
                    ArrayData keys = mapData.keyArray();
                    ArrayData values = mapData.valueArray();
                    Map<Object, Object> actual = new HashMap<>(keys.numElements());
                    for (int index = 0; index < keys.numElements(); index++)
                    {
                        Object key = keySparkType.toTestRowType(keys.get(index, getSparkSql(bridge).sparkSqlType(keyType)));
                        Object value = valueSparkType.toTestRowType(values.get(index, getSparkSql(bridge).sparkSqlType(valueType)));
                        actual.put(key, value);
                    }
                    assertEquals(expected.size(), actual.size());
                    for (Map.Entry<Object, Object> entry : expected.entrySet())
                    {
                        assertEquals(entry.getValue(), actual.get(entry.getKey()));
                    }
                }));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUdts()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((firstType, secondType) -> {
                    CqlField.CqlUdt udt = bridge.udt("keyspace", "testudt")
                                                .withField("a", firstType)
                                                .withField("b", bridge.ascii())
                                                .withField("c", secondType)
                                                .build();
                    Map<String, Object> expected = (Map<String, Object>) udt.randomValue();
                    assert expected != null;
                    ByteBuffer buffer = udt.serializeUdt(expected);
                    Map<String, Object> actual = udt.deserializeUdt(getSparkSql(bridge), buffer, false);
                    assertEquals(expected.size(), actual.size());
                    for (Map.Entry<String, Object> entry : expected.entrySet())
                    {
                        SparkType sparkType = getSparkSql(bridge).toSparkType(udt.field(entry.getKey()).type());
                        assertEquals(entry.getValue(),
                                     sparkType.toTestRowType(actual.get(entry.getKey())));
                    }
                }));
    }

    @Test
    public void testTuples()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((firstType, secondType) -> {
                    CqlField.CqlTuple tuple = bridge.tuple(firstType,
                                                           bridge.ascii(),
                                                           secondType,
                                                           bridge.timestamp(),
                                                           bridge.uuid(),
                                                           bridge.varchar());
                    Object[] expected = (Object[]) tuple.randomValue();
                    assert expected != null;
                    ByteBuffer buffer = tuple.serializeTuple(expected);
                    GenericInternalRow row = (GenericInternalRow) getSparkSql(bridge).convert(tuple, tuple.deserializeTuple(buffer, false), false);
                    Object[] actual = row.values();
                    assertEquals(expected.length, actual.length);
                    for (int index = 0; index < expected.length; index++)
                    {
                        SparkType sparkType = getSparkSql(bridge).toSparkType(tuple.type(index));
                        assertEquals(expected[index], sparkType.toTestRowType(actual[index]));
                    }
                }));
    }

    // test utilities

    static Object toInt(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::aInt, value);
    }

    static Object toSmallInt(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::smallint, value);
    }

    static Object toTinyInt(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::tinyint, value);
    }

    static Object toBool(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::bool, value);
    }

    static Object toBigInt(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::bigint, value);
    }

    static Object toText(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::text, value);
    }

    static Object toAscii(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::ascii, value);
    }

    static Object toVarChar(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::varchar, value);
    }

    static Object toInet(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::inet, value);
    }

    static Object toDouble(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::aDouble, value);
    }

    static Object toFloat(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::aFloat, value);
    }

    static Object toUUID(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::uuid, value);
    }

    static Object toTimeUUID(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::timeuuid, value);
    }

    static Object toDecimal(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::decimal, value);
    }

    static Object toVarInt(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::varint, value);
    }

    static Object toDate(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::date, value);
    }

    static Object toTime(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::time, value);
    }

    static Object toTimestamp(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::timestamp, value);
    }

    static Object toBlob(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::blob, value);
    }

    static Object toEmpty(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::empty, value);
    }

    static Object toNative(CassandraBridge bridge,
                           Function<CassandraBridge, CqlField.NativeType> typeMapper,
                           Object value)
    {
        CqlField.NativeType nativeType = typeMapper.apply(bridge);
        return nativeType.deserializeToType(getSparkSql(bridge),
                                            nativeType.serialize(value));
    }

    static Object toJavaType(CassandraBridge bridge,
                             Function<CassandraBridge, CqlField.NativeType> typeMapper,
                             Object value)
    {
        CqlField.NativeType nativeType = typeMapper.apply(bridge);
        return typeMapper.apply(bridge).deserializeToJavaType(nativeType.serialize(value));
    }
}
