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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
            assertTrue(bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(500L))) instanceof Decimal);
            assertEquals(Decimal.apply(500), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(500L))));
            assertNotSame(Decimal.apply(501), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(500L))));
            assertEquals(Decimal.apply(-1), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(-1L))));
            assertEquals(Decimal.apply(Long.MAX_VALUE), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(Long.MAX_VALUE))));
            assertEquals(Decimal.apply(Long.MIN_VALUE), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(Long.MIN_VALUE))));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(Integer.MAX_VALUE))));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(Integer.MIN_VALUE))));
            BigInteger veryLargeValue = BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), bridge.varint().deserialize(
                    bridge.varint().serialize(veryLargeValue)));
            qt().withExamples(MAX_TESTS)
                .forAll(bigIntegers().ofBytes(128))
                .checkAssert(integer -> assertEquals(Decimal.apply(integer), bridge.varint().deserialize(
                        bridge.varint().serialize(integer))));
        });
    }

    @Test
    public void testInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aInt().deserialize(bridge.aInt().serialize(5)) instanceof Integer);
            assertEquals(999, bridge.aInt().deserialize(ByteBuffer.allocate(4).putInt(0, 999)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals(integer, bridge.aInt().deserialize(
                        bridge.aInt().serialize(integer))));
        });
    }

    @Test
    public void testBoolean()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.bool().deserialize(bridge.bool().serialize(true)) instanceof Boolean);
            assertTrue((Boolean) bridge.bool().deserialize(bridge.bool().serialize(true)));
            assertFalse((Boolean) bridge.bool().deserialize(bridge.bool().serialize(false)));
        });
    }

    @Test
    public void testTimeUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.timeuuid().deserialize(
                    bridge.timeuuid().serialize(RandomUtils.getRandomTimeUUIDForTesting())) instanceof UTF8String);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = RandomUtils.getRandomTimeUUIDForTesting();
                assertEquals(expected.toString(), bridge.timeuuid().deserialize(
                        bridge.timeuuid().serialize(expected)).toString());
            }
        });
    }

    @Test
    public void testUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.uuid().deserialize(bridge.uuid().serialize(UUID.randomUUID())) instanceof UTF8String);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = UUID.randomUUID();
                assertEquals(expected.toString(), bridge.uuid().deserialize(
                        bridge.uuid().serialize(expected)).toString());
            }
        });
    }

    @Test
    public void testLong()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.bigint().deserialize(bridge.bigint().serialize(Long.MAX_VALUE)) instanceof Long);
            assertEquals(Long.MAX_VALUE, bridge.bigint().deserialize(
                    ByteBuffer.allocate(8).putLong(0, Long.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((long) integer, bridge.bigint().deserialize(
                        bridge.bigint().serialize((long) integer))));
            assertEquals(Long.MAX_VALUE, bridge.bigint().deserialize(bridge.bigint().serialize(Long.MAX_VALUE)));
            assertEquals(Long.MIN_VALUE, bridge.bigint().deserialize(bridge.bigint().serialize(Long.MIN_VALUE)));
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertEquals(aLong, bridge.bigint().deserialize(
                        bridge.bigint().serialize(aLong))));
        });
    }

    @Test
    public void testDecimal()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(500L))) instanceof Decimal);
            assertEquals(Decimal.apply(500), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(500L))));
            assertNotSame(Decimal.apply(501), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(500L))));
            assertEquals(Decimal.apply(-1), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(-1L))));
            assertEquals(Decimal.apply(Long.MAX_VALUE), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(Long.MAX_VALUE))));
            assertEquals(Decimal.apply(Long.MIN_VALUE), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(Long.MIN_VALUE))));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(Integer.MAX_VALUE))));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(Integer.MIN_VALUE))));
            BigDecimal veryLargeValue = BigDecimal.valueOf(Integer.MAX_VALUE).multiply(BigDecimal.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), bridge.decimal().deserialize(
                    bridge.decimal().serialize(veryLargeValue)));
            qt().withExamples(MAX_TESTS)
                .forAll(bigDecimals().ofBytes(128).withScale(10))
                .checkAssert(decimal -> assertEquals(Decimal.apply(decimal), bridge.decimal().deserialize(
                        bridge.decimal().serialize(decimal))));
        });
    }

    @Test
    public void testFloat()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aFloat().deserialize(bridge.aFloat().serialize(Float.MAX_VALUE)) instanceof Float);
            assertEquals(Float.MAX_VALUE, bridge.aFloat().deserialize(
                    ByteBuffer.allocate(4).putFloat(0, Float.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((float) integer, bridge.aFloat().deserialize(
                        bridge.aFloat().serialize((float) integer))));
            assertEquals(Float.MAX_VALUE, bridge.aFloat().deserialize(bridge.aFloat().serialize(Float.MAX_VALUE)));
            assertEquals(Float.MIN_VALUE, bridge.aFloat().deserialize(bridge.aFloat().serialize(Float.MIN_VALUE)));
            qt().withExamples(MAX_TESTS)
                .forAll(floats().any())
                .checkAssert(aFloat -> assertEquals(aFloat, bridge.aFloat().deserialize(
                        bridge.aFloat().serialize(aFloat))));
        });
    }

    @Test
    public void testDouble()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aDouble().deserialize(bridge.aDouble().serialize(Double.MAX_VALUE)) instanceof Double);
            assertEquals(Double.MAX_VALUE, bridge.aDouble().deserialize(
                    ByteBuffer.allocate(8).putDouble(0, Double.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((double) integer, bridge.aDouble().deserialize(
                        bridge.aDouble().serialize((double) integer))));
            assertEquals(Double.MAX_VALUE, bridge.aDouble().deserialize(bridge.aDouble().serialize(Double.MAX_VALUE)));
            assertEquals(Double.MIN_VALUE, bridge.aDouble().deserialize(bridge.aDouble().serialize(Double.MIN_VALUE)));
            qt().withExamples(MAX_TESTS)
                .forAll(doubles().any())
                .checkAssert(aDouble -> assertEquals(aDouble, bridge.aDouble().deserialize(
                        bridge.aDouble().serialize(aDouble))));
        });
    }

    @Test
    public void testAscii()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.ascii().deserialize(bridge.ascii().serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, bridge.ascii().deserialize(
                        bridge.ascii().serialize(string)).toString()));
        });
    }

    @Test
    public void testText()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.text().deserialize(bridge.text().serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, bridge.text().deserialize(
                        bridge.text().serialize(string)).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, bridge.text().deserialize(
                        bridge.text().serialize(string)).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertEquals(string, bridge.text().deserialize(
                        bridge.text().serialize(string)).toString()));
        });
    }

    @Test
    public void testVarchar()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.varchar().deserialize(bridge.varchar().serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, bridge.varchar().deserialize(
                        bridge.varchar().serialize(string)).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertEquals(string, bridge.varchar().deserialize(
                        bridge.varchar().serialize(string)).toString()));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertEquals(string, bridge.varchar().deserialize(
                        bridge.varchar().serialize(string)).toString()));
        });
    }

    @Test
    public void testInet()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.inet().deserialize(bridge.inet().serialize(RandomUtils.randomInet())) instanceof byte[]);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                InetAddress expected = RandomUtils.randomInet();
                assertArrayEquals(expected.getAddress(), (byte[]) bridge.inet().deserialize(
                        bridge.inet().serialize(expected)));
            }
        });
    }

    @Test
    public void testDate()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.date().deserialize(bridge.date().serialize(5)) instanceof Integer);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals(integer - Integer.MIN_VALUE,
                                                     bridge.date().deserialize(bridge.date().serialize(integer))));
        });
    }

    @Test
    public void testTime()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.time().deserialize(bridge.time().serialize(Long.MAX_VALUE)) instanceof Long);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertEquals((long) integer, bridge.time().deserialize(
                        bridge.time().serialize((long) integer))));
            assertEquals(Long.MAX_VALUE, bridge.time().deserialize(bridge.time().serialize(Long.MAX_VALUE)));
            assertEquals(Long.MIN_VALUE, bridge.time().deserialize(bridge.time().serialize(Long.MIN_VALUE)));
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertEquals(aLong, bridge.time().deserialize(bridge.time().serialize(aLong))));
        });
    }

    @Test
    public void testTimestamp()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            Date now = new Date();
            assertTrue(bridge.timestamp().deserialize(bridge.timestamp().serialize(now)) instanceof Long);
            assertEquals(java.sql.Timestamp.from(now.toInstant()).getTime() * 1000L,
                         bridge.timestamp().deserialize(bridge.timestamp().serialize(now)));
            qt().withExamples(MAX_TESTS)
                .forAll(dates().withMillisecondsBetween(0, Long.MAX_VALUE))
                .checkAssert(date -> assertEquals(java.sql.Timestamp.from(date.toInstant()).getTime() * 1000L,
                                                  bridge.timestamp().deserialize(bridge.timestamp().serialize(date))));
        });
    }

    @Test
    public void testBlob()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.blob().deserialize(
                    bridge.blob().serialize(ByteBuffer.wrap(RandomUtils.randomBytes(5)))) instanceof byte[]);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                int size = RandomUtils.RANDOM.nextInt(1024);
                byte[] expected = RandomUtils.randomBytes(size);
                assertArrayEquals(expected, (byte[]) bridge.blob().deserialize(
                        bridge.blob().serialize(ByteBuffer.wrap(expected))));
            }
        });
    }

    @Test
    public void testEmpty()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge ->
            assertNull(bridge.empty().deserialize(bridge.empty().serialize(null)))
        );
    }

    @Test
    public void testSmallInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.smallint().deserialize(bridge.smallint().serialize((short) 5)) instanceof Short);
            qt().forAll(integers().between(Short.MIN_VALUE, Short.MAX_VALUE))
                .checkAssert(integer -> {
                    short expected = integer.shortValue();
                    assertEquals(expected, bridge.smallint().deserialize(bridge.smallint().serialize(expected)));
                });
        });
    }

    @Test
    public void testTinyInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.tinyint().deserialize(
                    bridge.tinyint().serialize(RandomUtils.randomByte())) instanceof Byte);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                byte expected = RandomUtils.randomByte();
                assertEquals(expected, bridge.tinyint().deserialize(bridge.tinyint().serialize(expected)));
            }
        });
    }

    @Test
    public void testSerialization()
    {
        // CassandraBridge.serialize is mostly used for unit tests
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            // BLOB,  VARINT
            assertEquals("ABC", bridge.ascii().deserialize(bridge.ascii().serialize("ABC")).toString());
            assertEquals(500L, bridge.bigint().deserialize(bridge.bigint().serialize(500L)));
            assertEquals(true, bridge.bool().deserialize(bridge.bool().serialize(true)));
            assertEquals(false, bridge.bool().deserialize(bridge.bool().serialize(false)));

            byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
            ByteBuffer buffer = bridge.blob().serialize(ByteBuffer.wrap(bytes));
            byte[] result = new byte[4];
            buffer.get(result);
            assertArrayEquals(bytes, result);

            assertEquals(500 + Integer.MIN_VALUE, bridge.date().deserialize(bridge.date().serialize(500)));
            assertEquals(Decimal.apply(500000.2038484), bridge.decimal().deserialize(
                    bridge.decimal().serialize(BigDecimal.valueOf(500000.2038484))));
            assertEquals(123211.023874839, bridge.aDouble().deserialize(bridge.aDouble().serialize(123211.023874839)));
            assertEquals(58383.23737832839f, bridge.aFloat().deserialize(
                    bridge.aFloat().serialize(58383.23737832839f)));
            try
            {
                assertEquals(InetAddress.getByName("www.apache.org"),
                             InetAddress.getByAddress((byte[]) bridge.inet().deserialize(
                                    bridge.inet().serialize(InetAddress.getByName("www.apache.org")))));
            }
            catch (UnknownHostException exception)
            {
                throw new RuntimeException(exception);
            }
            assertEquals(283848498, bridge.aInt().deserialize(bridge.aInt().serialize(283848498)));
            assertEquals((short) 29, bridge.smallint().deserialize(bridge.smallint().serialize((short) 29)));
            assertEquals("hello world", bridge.ascii().deserialize(bridge.text().serialize("hello world")).toString());
            assertEquals(5002839L, bridge.time().deserialize(bridge.time().serialize(5002839L)));
            Date now = new Date();
            assertEquals(now.getTime() * 1000L, bridge.timestamp().deserialize(bridge.timestamp().serialize(now)));
            UUID timeUuid = RandomUtils.getRandomTimeUUIDForTesting();
            assertEquals(timeUuid, UUID.fromString(bridge.timeuuid().deserialize(
                    bridge.timeuuid().serialize(timeUuid)).toString()));
            assertEquals((byte) 100, bridge.tinyint().deserialize(bridge.tinyint().serialize((byte) 100)));
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, UUID.fromString(bridge.uuid().deserialize(bridge.uuid().serialize(uuid)).toString()));
            assertEquals("ABCDEFG", bridge.varchar().deserialize(bridge.varchar().serialize("ABCDEFG")).toString());
            assertEquals(Decimal.apply(12841924), bridge.varint().deserialize(
                    bridge.varint().serialize(BigInteger.valueOf(12841924))));
        });
    }

    @Test
    public void testList()
    {
        runTest((partitioner, directory, bridge) ->
            qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> {
                CqlField.CqlList list = bridge.list(type);
                List<Object> expected = IntStream.range(0, 128)
                                                 .mapToObj(index -> RandomUtils.randomValue(type))
                                                 .collect(Collectors.toList());
                ByteBuffer buffer = list.serialize(expected);
                List<Object> actual = Arrays.asList(((ArrayData) list.deserialize(buffer)).array());
                assertEquals(expected.size(), actual.size());
                for (int index = 0; index < expected.size(); index++)
                {
                    assertEquals(expected.get(index), type.toTestRowType(actual.get(index)));
                }
            }));
    }

    @Test
    public void testSet()
    {
        runTest((partitioner, directory, bridge) ->
            qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> {
                CqlField.CqlSet set = bridge.set(type);
                Set<Object> expected = IntStream.range(0, 128)
                                                .mapToObj(integer -> RandomUtils.randomValue(type))
                                                .collect(Collectors.toSet());
                ByteBuffer buffer = set.serialize(expected);
                Set<Object> actual = new HashSet<>(Arrays.asList(((ArrayData) set.deserialize(buffer)).array()));
                assertEquals(expected.size(), actual.size());
                for (Object value : actual)
                {
                    assertTrue(expected.contains(type.toTestRowType(value)));
                }
            }));
    }

    @Test
    public void testMap()
    {
        runTest((partitioner, directory, bridge) ->
            qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((keyType, valueType) -> {
                CqlField.CqlMap map = bridge.map(keyType, valueType);
                int count = keyType.cardinality(128);
                Map<Object, Object> expected = new HashMap<>(count);
                for (int entry = 0; entry < count; entry++)
                {
                    Object key = null;
                    while (key == null || expected.containsKey(key))
                    {
                        key = RandomUtils.randomValue(keyType);
                    }
                    expected.put(key, RandomUtils.randomValue(valueType));
                }
                ByteBuffer buffer = map.serialize(expected);
                ArrayBasedMapData mapData = ((ArrayBasedMapData) map.deserialize(buffer));
                ArrayData keys = mapData.keyArray();
                ArrayData values = mapData.valueArray();
                Map<Object, Object> actual = new HashMap<>(keys.numElements());
                for (int index = 0; index < keys.numElements(); index++)
                {
                    Object key = keyType.toTestRowType(keys.get(index, keyType.sparkSqlType(BigNumberConfig.DEFAULT)));
                    Object value = valueType.toTestRowType(values.get(index,
                                                                      valueType.sparkSqlType(BigNumberConfig.DEFAULT)));
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
                Map<String, Object> expected = (Map<String, Object>) RandomUtils.randomValue(udt);
                assert expected != null;
                ByteBuffer buffer = udt.serializeUdt(expected);
                Map<String, Object> actual = udt.deserializeUdt(buffer, false);
                assertEquals(expected.size(), actual.size());
                for (Map.Entry<String, Object> entry : expected.entrySet())
                {
                    assertEquals(entry.getValue(),
                                 udt.field(entry.getKey()).type().toTestRowType(actual.get(entry.getKey())));
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
                Object[] expected = (Object[]) RandomUtils.randomValue(tuple);
                assert expected != null;
                ByteBuffer buffer = tuple.serializeTuple(expected);
                Object[] actual = tuple.deserializeTuple(buffer, false);
                assertEquals(expected.length, actual.length);
                for (int index = 0; index < expected.length; index++)
                {
                    assertEquals(expected[index], tuple.type(index).toTestRowType(actual[index]));
                }
            }));
    }
}
