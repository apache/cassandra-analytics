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
import org.apache.cassandra.bridge.type.InternalDuration;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.spark.utils.SparkTypeUtils;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.getSparkSql;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.assertj.core.api.Assertions.assertThat;
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
            assertThat(toVarInt(bridge, BigInteger.valueOf(500L))).isInstanceOf(Decimal.class);
            assertThat(toVarInt(bridge, BigInteger.valueOf(500L))).isEqualTo(Decimal.apply(500));
            assertThat(toVarInt(bridge, BigInteger.valueOf(500L))).isNotEqualTo(Decimal.apply(501));
            assertThat(toVarInt(bridge, BigInteger.valueOf(-1L))).isEqualTo(Decimal.apply(-1));
            assertThat(toVarInt(bridge, BigInteger.valueOf(Long.MAX_VALUE))).isEqualTo(Decimal.apply(Long.MAX_VALUE));
            assertThat(toVarInt(bridge, BigInteger.valueOf(Long.MIN_VALUE))).isEqualTo(Decimal.apply(Long.MIN_VALUE));
            assertThat(toVarInt(bridge, BigInteger.valueOf(Integer.MAX_VALUE))).isEqualTo(Decimal.apply(Integer.MAX_VALUE));
            assertThat(toVarInt(bridge, BigInteger.valueOf(Integer.MIN_VALUE))).isEqualTo(Decimal.apply(Integer.MIN_VALUE));
            BigInteger veryLargeValue = BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.valueOf(5));
            assertThat(toVarInt(bridge, veryLargeValue)).isEqualTo(Decimal.apply(veryLargeValue));
            qt().withExamples(MAX_TESTS)
                .forAll(bigIntegers().ofBytes(128))
                .checkAssert(integer -> assertThat(toVarInt(bridge, integer)).isEqualTo(Decimal.apply(integer)));
        });
    }

    @Test
    public void testInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toInt(bridge, 5)).isInstanceOf(Integer.class);
            assertThat(bridge.aInt().deserializeToType(getSparkSql(bridge), ByteBuffer.allocate(4).putInt(0, 999))).isEqualTo(999);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toInt(bridge, integer)).isEqualTo(integer));
        });
    }

    @Test
    public void testBoolean()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toBool(bridge, true)).isInstanceOf(Boolean.class);
            assertThat((Boolean) toBool(bridge, true)).isTrue();
            assertThat((Boolean) toBool(bridge, false)).isFalse();
        });
    }

    @Test
    public void testTimeUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toTimeUUID(bridge, RandomUtils.getRandomTimeUUIDForTesting())).isInstanceOf(UTF8String.class);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = RandomUtils.getRandomTimeUUIDForTesting();
                assertThat(toTimeUUID(bridge, expected).toString()).isEqualTo(expected.toString());
            }
        });
    }

    @Test
    public void testUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toUUID(bridge, UUID.randomUUID())).isInstanceOf(UTF8String.class);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                UUID expected = UUID.randomUUID();
                assertThat(toUUID(bridge, expected).toString()).isEqualTo(expected.toString());
            }
        });
    }

    @Test
    public void testLong()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toBigInt(bridge, Long.MAX_VALUE)).isInstanceOf(Long.class);
            assertThat(bridge.bigint().deserializeToType(getSparkSql(bridge),
                                                                           ByteBuffer.allocate(8).putLong(0, Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toBigInt(bridge, (long) integer)).isEqualTo((long) integer));
            assertThat(toJavaType(bridge, CassandraBridge::bigint, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
            assertThat(toJavaType(bridge, CassandraBridge::bigint, Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertThat(toBigInt(bridge, aLong)).isEqualTo(aLong));
        });
    }

    @Test
    public void testDecimal()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toDecimal(bridge, BigDecimal.valueOf(500L))).isInstanceOf(Decimal.class);
            assertThat(toDecimal(bridge, BigDecimal.valueOf(500L))).isEqualTo(Decimal.apply(500));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(500L))).isNotEqualTo(Decimal.apply(501));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(-1L))).isEqualTo(Decimal.apply(-1));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(Long.MAX_VALUE))).isEqualTo(Decimal.apply(Long.MAX_VALUE));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(Long.MIN_VALUE))).isEqualTo(Decimal.apply(Long.MIN_VALUE));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(Integer.MAX_VALUE))).isEqualTo(Decimal.apply(Integer.MAX_VALUE));
            assertThat(toDecimal(bridge, BigDecimal.valueOf(Integer.MIN_VALUE))).isEqualTo(Decimal.apply(Integer.MIN_VALUE));
            BigDecimal veryLargeValue = BigDecimal.valueOf(Integer.MAX_VALUE).multiply(BigDecimal.valueOf(5));
            assertThat(toDecimal(bridge, veryLargeValue)).isEqualTo(Decimal.apply(veryLargeValue));
            qt().withExamples(MAX_TESTS)
                .forAll(bigDecimals().ofBytes(128).withScale(10))
                .checkAssert(decimal -> assertThat(bridge.decimal().deserializeToType(getSparkSql(bridge), bridge.decimal().serialize(decimal)))
                                        .isEqualTo(Decimal.apply(decimal)));
        });
    }

    @Test
    public void testFloat()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toFloat(bridge, Float.MAX_VALUE)).isInstanceOf(Float.class);
            assertThat(bridge.aFloat().deserializeToType(getSparkSql(bridge),
                                                                            ByteBuffer.allocate(4).putFloat(0, Float.MAX_VALUE))).isEqualTo(Float.MAX_VALUE);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toFloat(bridge, (float) integer)).isEqualTo((float) integer));
            assertThat(toFloat(bridge, Float.MAX_VALUE)).isEqualTo(Float.MAX_VALUE);
            assertThat(toFloat(bridge, Float.MIN_VALUE)).isEqualTo(Float.MIN_VALUE);
            qt().withExamples(MAX_TESTS)
                .forAll(floats().any())
                .checkAssert(aFloat -> assertThat(toFloat(bridge, aFloat)).isEqualTo(aFloat));
        });
    }

    @Test
    public void testDouble()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toDouble(bridge, Double.MAX_VALUE)).isInstanceOf(Double.class);
            assertThat(bridge.aDouble().deserializeToType(getSparkSql(bridge), ByteBuffer.allocate(8).putDouble(0, Double.MAX_VALUE)))
            .isEqualTo(Double.MAX_VALUE);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toDouble(bridge, (double) integer)).isEqualTo((double) integer));
            assertThat(toDouble(bridge, Double.MAX_VALUE)).isEqualTo(Double.MAX_VALUE);
            assertThat(toDouble(bridge, Double.MIN_VALUE)).isEqualTo(Double.MIN_VALUE);
            qt().withExamples(MAX_TESTS)
                .forAll(doubles().any())
                .checkAssert(aDouble -> assertThat(toDouble(bridge, aDouble)).isEqualTo(aDouble));
        });
    }

    @Test
    public void testAscii()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toAscii(bridge, "abc")).isInstanceOf(UTF8String.class);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertThat(toAscii(bridge, string).toString()).isEqualTo(string));
        });
    }

    @Test
    public void testText()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toText(bridge, "abc")).isInstanceOf(UTF8String.class);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertThat(toText(bridge, string).toString()).isEqualTo(string));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertThat(toText(bridge, string).toString()).isEqualTo(string));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertThat(toText(bridge, string).toString()).isEqualTo(string));
        });
    }

    @Test
    public void testVarchar()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toVarChar(bridge, "abc")).isInstanceOf(UTF8String.class);
            qt().withExamples(MAX_TESTS)
                .forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(string -> assertThat(toVarChar(bridge, string).toString()).isEqualTo(string));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(string -> assertThat(toVarChar(bridge, string).toString()).isEqualTo(string));
            qt().withExamples(MAX_TESTS)
                .forAll(strings().numeric())
                .checkAssert(string -> assertThat(toVarChar(bridge, string).toString()).isEqualTo(string));
        });
    }

    @Test
    public void testInet()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toInet(bridge, RandomUtils.randomInet())).isInstanceOf(byte[].class);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                InetAddress expected = RandomUtils.randomInet();
                assertThat((byte[]) toInet(bridge, expected)).isEqualTo(expected.getAddress());
            }
        });
    }

    @Test
    public void testDate()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toDate(bridge, 5)).isInstanceOf(Integer.class);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toDate(bridge, integer)).isEqualTo(integer - Integer.MIN_VALUE));
        });
    }

    @Test
    public void testTime()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toTime(bridge, Long.MAX_VALUE)).isInstanceOf(Long.class);
            qt().forAll(integers().all())
                .checkAssert(integer -> assertThat(toTime(bridge, (long) integer)).isEqualTo((long) integer));
            assertThat(toTime(bridge, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
            assertThat(toTime(bridge, Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
            qt().withExamples(MAX_TESTS)
                .forAll(longs().all())
                .checkAssert(aLong -> assertThat(toTime(bridge, aLong)).isEqualTo(aLong));
        });
    }

    @Test
    public void testDuration()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            CalendarInterval value = SparkTypeUtils.convertDuration(new InternalDuration(1, 2, 7000000000L));
            Object converted = SqlToCqlTypeConverter.getConverter(bridge.duration()).convert(value);
            Object deserialized = toDuration(bridge, converted);
            assertThat(deserialized).isInstanceOf(CalendarInterval.class);
            assertThat(deserialized).isEqualTo(value);
        });
    }

    @Test
    public void testTimestamp()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            Date now = new Date();
            assertThat(toTimestamp(bridge, now)).isInstanceOf(Long.class);
            assertThat(toTimestamp(bridge, now)).isEqualTo(java.sql.Timestamp.from(now.toInstant()).getTime() * 1000L);
            qt().withExamples(MAX_TESTS)
                .forAll(dates().withMillisecondsBetween(0, Long.MAX_VALUE))
                .checkAssert(date -> assertThat(toTimestamp(bridge, date)).isEqualTo(java.sql.Timestamp.from(date.toInstant()).getTime() * 1000L));
        });
    }

    @Test
    public void testBlob()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toBlob(bridge, ByteBuffer.wrap(RandomUtils.randomBytes(5)))).isInstanceOf(byte[].class);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                int size = RandomUtils.RANDOM.nextInt(1024);
                byte[] expected = RandomUtils.randomBytes(size);
                assertThat((byte[]) toBlob(bridge, ByteBuffer.wrap(expected))).isEqualTo(expected);
            }
        });
    }

    @Test
    public void testEmpty()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge ->
                                                     assertThat(toEmpty(bridge, null)).isNull()
        );
    }

    @Test
    public void testSmallInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toSmallInt(bridge, (short) 5)).isInstanceOf(Short.class);
            qt().forAll(integers().between(Short.MIN_VALUE, Short.MAX_VALUE))
                .checkAssert(integer -> {
                    short expected = integer.shortValue();
                    assertThat(toSmallInt(bridge, expected)).isEqualTo(expected);
                });
        });
    }

    @Test
    public void testTinyInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertThat(toTinyInt(bridge, RandomUtils.randomByte())).isInstanceOf(Byte.class);
            for (int test = 0; test < MAX_TESTS; test++)
            {
                byte expected = RandomUtils.randomByte();
                assertThat(toTinyInt(bridge, expected)).isEqualTo(expected);
            }
        });
    }

    @Test
    public void testSerialization()
    {
        // CassandraBridge.serialize is mostly used for unit tests
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            // BLOB,  VARINT
            assertThat(toAscii(bridge, "ABC").toString()).isEqualTo("ABC");
            assertThat(toBigInt(bridge, 500L)).isEqualTo(500L);
            assertThat((Boolean) toBool(bridge, true)).isTrue();
            assertThat((Boolean) toBool(bridge, false)).isFalse();

            byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
            ByteBuffer buffer = bridge.blob().serialize(ByteBuffer.wrap(bytes));
            byte[] result = new byte[4];
            buffer.get(result);
            assertThat(result).isEqualTo(bytes);

            assertThat(toDate(bridge, 500)).isEqualTo(500 + Integer.MIN_VALUE);
            assertThat(toDecimal(bridge, BigDecimal.valueOf(500000.2038484))).isEqualTo(Decimal.apply(500000.2038484));
            assertThat(toDouble(bridge, 123211.023874839)).isEqualTo(123211.023874839);
            assertThat(toFloat(bridge, 58383.23737832839f)).isEqualTo(58383.23737832839f);
            try
            {
                assertThat(InetAddress.getByAddress((byte[]) toInet(bridge, InetAddress.getByName("www.apache.org"))))
                .isEqualTo(InetAddress.getByName("www.apache.org"));
            }
            catch (UnknownHostException exception)
            {
                throw new RuntimeException(exception);
            }
            assertThat(toInt(bridge, 283848498)).isEqualTo(283848498);
            assertThat(toSmallInt(bridge, (short) 29)).isEqualTo((short) 29);
            assertThat(toAscii(bridge, "hello world").toString()).isEqualTo("hello world");
            assertThat(toTime(bridge, 5002839L)).isEqualTo(5002839L);
            Date now = new Date();
            assertThat(toTimestamp(bridge, now)).isEqualTo(now.getTime() * 1000L);
            UUID timeUuid = RandomUtils.getRandomTimeUUIDForTesting();
            assertThat(UUID.fromString(toTimeUUID(bridge, timeUuid).toString())).isEqualTo(timeUuid);
            assertThat(toTinyInt(bridge, (byte) 100)).isEqualTo((byte) 100);
            UUID uuid = UUID.randomUUID();
            assertThat(UUID.fromString(toUUID(bridge, uuid).toString())).isEqualTo(uuid);
            assertThat(toVarChar(bridge, "ABCDEFG").toString()).isEqualTo("ABCDEFG");
            assertThat(toVarInt(bridge, BigInteger.valueOf(12841924))).isEqualTo(Decimal.apply(12841924));
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
                    assertThat(actual.size()).isEqualTo(expected.size());
                    for (int index = 0; index < expected.size(); index++)
                    {
                        assertThat(sparkType.toTestRowType(actual.get(index))).isEqualTo(expected.get(index));
                    }
                }));
    }

    @Test
    public void testSet()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge)).assuming(CqlField.CqlType::supportedAsSetElement).checkAssert(type -> {
                    CqlField.CqlSet set = bridge.set(type);
                    SparkType sparkType = getSparkSql(bridge).toSparkType(type);
                    Set<Object> expected = IntStream.range(0, 128)
                                                    .mapToObj(integer -> type.randomValue())
                                                    .collect(Collectors.toSet());
                    ByteBuffer buffer = set.serialize(expected);
                    Set<Object> actual = new HashSet<>(Arrays.asList(((ArrayData) set.deserializeToType(getSparkSql(bridge), buffer)).array()));
                    assertThat(actual.size()).isEqualTo(expected.size());
                    for (Object value : actual)
                    {
                        assertThat(expected.contains(sparkType.toTestRowType(value))).isTrue();
                    }
                }));
    }

    @Test
    public void testMap()
    {
        runTest((partitioner, directory, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
                    .assuming((keyType, valueType) -> keyType.supportedAsMapKey())
                    .checkAssert((keyType, valueType) -> {
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
                        assertThat(actual.size()).isEqualTo(expected.size());
                        for (Map.Entry<Object, Object> entry : expected.entrySet())
                        {
                            assertThat(actual.get(entry.getKey())).isEqualTo(entry.getValue());
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
                    assertThat(actual.size()).isEqualTo(expected.size());
                    for (Map.Entry<String, Object> entry : expected.entrySet())
                    {
                        SparkType sparkType = getSparkSql(bridge).toSparkType(udt.field(entry.getKey()).type());
                        assertThat(sparkType.toTestRowType(actual.get(entry.getKey()))).isEqualTo(entry.getValue());
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
                    assertThat(actual.length).isEqualTo(expected.length);
                    for (int index = 0; index < expected.length; index++)
                    {
                        SparkType sparkType = getSparkSql(bridge).toSparkType(tuple.type(index));
                        assertThat(sparkType.toTestRowType(actual[index])).isEqualTo(expected[index]);
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

    static Object toDuration(CassandraBridge bridge, Object value)
    {
        return toNative(bridge, CassandraBridge::duration, value);
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
