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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.common.schema.ListType;
import org.apache.cassandra.spark.common.schema.MapType;
import org.apache.cassandra.spark.common.schema.SetType;
import org.apache.cassandra.spark.data.BridgeUdtValue;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static java.util.AbstractMap.SimpleEntry;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.ASCII;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BIGINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BLOB;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BOOLEAN;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DECIMAL;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DOUBLE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.FLOAT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INET;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.LIST;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.SET;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.SMALLINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TEXT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIME;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMESTAMP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMEUUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TINYINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.UUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARINT;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.buildSchema;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCollectionCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockListCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockMapCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockSetCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockUdtCqlType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class TableSchemaNormalizeTest
{
    @Test
    public void testAsciiNormalization()
    {
        assertNormalized("ascii", mockCqlType(ASCII), ColumnTypes.STRING, "ascii", "ascii", DataTypes.StringType);
    }

    @Test
    public void testBigIntNormalization()
    {
        assertNormalized("bigint", mockCqlType(BIGINT), ColumnTypes.INT, 1, 1L, DataTypes.IntegerType);
    }

    @Test
    public void testBlobNormalization()
    {
        assertNormalized("blob", mockCqlType(BLOB), ColumnTypes.BYTES,
                         new byte[]{1, 1, 1, 1}, ByteBuffer.wrap(new byte[]{1, 1, 1, 1}), DataTypes.BinaryType);
    }

    @Test
    public void testBooleanNormalization()
    {
        assertNormalized("boolean", mockCqlType(BOOLEAN), ColumnTypes.BOOLEAN, false, false, DataTypes.BooleanType);
    }

    @Test
    public void testDecimalNormalization()
    {
        assertNormalized("decimal", mockCqlType(DECIMAL), ColumnTypes.DOUBLE,
                         BigDecimal.valueOf(1.1), BigDecimal.valueOf(1.1), DataTypes.createDecimalType());
    }

    @Test
    public void testDoubleNormalization()
    {
        assertNormalized("double", mockCqlType(DOUBLE), ColumnTypes.DOUBLE, 1.1, 1.1, DataTypes.DoubleType);
    }

    @Test
    public void testFloatNormalization()
    {
        assertNormalized("float", mockCqlType(FLOAT), ColumnTypes.DOUBLE, 1.1f, 1.1f, DataTypes.FloatType);
    }

    @Test
    public void testInetNormalization()
    {
        assertNormalized("inet", mockCqlType(INET), ColumnTypes.STRING,
                         "192.168.1.1", InetAddresses.forString("192.168.1.1"), DataTypes.StringType);
    }

    @Test
    public void testIntNormalization()
    {
        assertNormalized("int", mockCqlType(INT), ColumnTypes.INT, 1, 1, DataTypes.IntegerType);
    }

    @Test
    public void testTextNormalization()
    {
        assertNormalized("text", mockCqlType(TEXT), ColumnTypes.BYTES, "text", "text", DataTypes.StringType);
    }

    @Test
    public void testTimestampNormalization()
    {
        assertNormalized("timestamp", mockCqlType(TIMESTAMP), ColumnTypes.LONG,
                         new Date(1), new Date(1), DataTypes.DateType);
    }

    @Test
    public void testUuidNormalization()
    {
        assertNormalized("uuid", mockCqlType(UUID), ColumnTypes.UUID,
                         "382d3b34-22af-4b2a-97a3-ae7dbf9e6abe",
                         java.util.UUID.fromString("382d3b34-22af-4b2a-97a3-ae7dbf9e6abe"), DataTypes.StringType);
    }

    @Test
    public void testVarcharNormalization()
    {
        assertNormalized("varchar", mockCqlType(VARCHAR), ColumnTypes.STRING,
                         "varchar", "varchar", DataTypes.StringType);
    }

    @Test
    public void testVarIntNormalization()
    {
        assertNormalized("varint", mockCqlType(VARINT), ColumnTypes.INT,
                         "1", BigInteger.valueOf(1), DataTypes.createDecimalType(38, 0));
    }

    @Test
    public void testTimeUuidNormalization()
    {
        assertNormalized("timeuuid", mockCqlType(TIMEUUID), ColumnTypes.UUID,
                         java.util.UUID.fromString("0846b690-ce35-11e7-8871-79b4d1aa8ef2"),
                         java.util.UUID.fromString("0846b690-ce35-11e7-8871-79b4d1aa8ef2"), DataTypes.StringType);
    }

    @Test
    public void testSetNormalization()
    {
        Set<String> set = new HashSet<>();
        set.add("A");
        set.add("B");
        set.add("C");

        assertNormalized("set", mockSetCqlType(TEXT), new SetType<>(ColumnTypes.STRING),
                         set, set, DataTypes.createArrayType(DataTypes.StringType));
    }

    @Test
    public void testListNormalization()
    {
        assertNormalized("list", mockListCqlType(INT), new ListType<>(ColumnTypes.INT),
                         Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3),
                         DataTypes.createArrayType(DataTypes.IntegerType));
    }

    @Test
    public void testMapNormalization()
    {
        Map<String, String> map = Stream.of(new SimpleEntry<>("Foo", "Bar"))
                                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        assertNormalized("map", mockMapCqlType(TEXT, TEXT), new MapType<>(ColumnTypes.STRING, ColumnTypes.STRING),
                         map, map, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    }

    @Test
    public void testSmallIntNormalization()
    {
        assertNormalized("smallint", mockCqlType(SMALLINT), ColumnTypes.INT, (short) 2, (short) 2, DataTypes.ShortType);
    }

    @Test
    public void testTinyIntNormalization()
    {
        assertNormalized("tiny", mockCqlType(TINYINT), ColumnTypes.INT, (byte) 3, (byte) 3, DataTypes.ByteType);
    }

    @Test
    public void testDateNormalization()
    {
        assertNormalized("date", mockCqlType(DATE), ColumnTypes.LONG, new Date(2), -2147483648, DataTypes.DateType);
    }

    @Test
    public void testTimeNormalizationFromTimestamp()
    {
        Timestamp timestamp = new Timestamp(0, 0, 0, 0, 0, 0, 3);
        assertNormalized("time", mockCqlType(TIME), ColumnTypes.LONG, timestamp, 3L, DataTypes.TimestampType);
    }

    @Test
    public void testTimeNormalizationFromLong()
    {
        assertNormalized("time", mockCqlType(TIME), ColumnTypes.LONG, 7L, 7L, DataTypes.LongType);
    }

    @Test
    public void testByteArrayListNormalization()
    {
        assertNormalized("byte_array_list", mockListCqlType(BLOB), new ListType<>(ColumnTypes.BYTES),
                         Arrays.asList(new byte[]{1}, new byte[]{2}, new byte[]{3}),
                         Arrays.asList(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2}), ByteBuffer.wrap(new byte[]{3})),
                         DataTypes.createArrayType(DataTypes.BinaryType));
    }

    @Test
    public void testByteArrayMapNormalization()
    {
        byte[] bytes = {'B', 'a', 'r'};

        Map<String, byte[]> source = Stream.of(new SimpleEntry<>("Foo", bytes))
                                           .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        Map<String, ByteBuffer> expected = Stream.of(new SimpleEntry<>("Foo", ByteBuffer.wrap(bytes)))
                                                 .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        assertNormalized("mapWithBytes", mockMapCqlType(TEXT, BLOB), new MapType<>(ColumnTypes.STRING, ColumnTypes.STRING),
                         source, expected, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    }

    @Test
    public void testByteArraySetNormalization()
    {
        byte[] bytes = {'B', 'a', 'r'};

        Set<byte[]> source = new HashSet<>(Arrays.asList(new byte[][]{bytes}));
        Set<ByteBuffer> expected = new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes)));
        assertNormalized("setWithBytes", mockSetCqlType(BLOB), new SetType<>(ColumnTypes.BYTES),
                         source, expected, DataTypes.createArrayType(DataTypes.BinaryType));
    }

    @Test
    public void testNestedNormalization()
    {
        byte[] bytes = {'B', 'a', 'r'};

        Map<String, List<Set<byte[]>>> source = new HashMap<>();
        source.put("Foo1", Arrays.asList(new HashSet<>(Arrays.asList(new byte[][]{bytes})),
                                         new HashSet<>(Arrays.asList(new byte[][]{bytes}))));
        source.put("Foo2", Arrays.asList(new HashSet<>(Arrays.asList(new byte[][]{bytes})),
                                         new HashSet<>(Arrays.asList(new byte[][]{bytes}))));

        Map<String, List<Set<ByteBuffer>>> expected = new HashMap<>();
        expected.put("Foo1", Arrays.asList(new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes))),
                                           new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes)))));
        expected.put("Foo2", Arrays.asList(new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes))),
                                           new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes)))));

        CqlField.CqlMap cqlType = mockMapCqlType(mockCqlType(TEXT),
                                                 mockCollectionCqlType(LIST, mockCollectionCqlType(SET, mockCqlType(BLOB))));
        assertNormalized("byte_array_list", cqlType, new MapType<>(ColumnTypes.STRING, new ListType<>(ColumnTypes.BYTES)),
                         source, expected, DataTypes.createMapType(DataTypes.StringType,
                                                                   DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.BinaryType))));
    }

    @Test
    public void testUdtNormalization()
    {
        StructType structType = new StructType()
                                .add(new StructField("f1", DataTypes.IntegerType, false, Metadata.empty()))
                                .add(new StructField("f2", DataTypes.StringType, false, Metadata.empty()));

        GenericRowWithSchema source = new GenericRowWithSchema(new Object[]{1, "course"}, structType);
        // NOTE: UDT Types carry their type name around, so the use of `udt_field` consistently here is a bit
        // "wrong" for the real-world, but is tested by integration tests elsewhere and is correct for the way
        // the asserts in this test work.
        BridgeUdtValue udtValue = new BridgeUdtValue("udt_field", ImmutableMap.of("f1", 1, "f2", "course"));

        CqlField.CqlUdt cqlType = mockUdtCqlType("udt_field", "f1", INT, "f2", TEXT);
        assertNormalized("udt_field", cqlType, new MapType<>(ColumnTypes.STRING, new ListType<>(ColumnTypes.BYTES)),
                         source, udtValue, structType);
    }

    private void assertNormalized(String field,
                                  CqlField.CqlType cqlType,
                                  ColumnType<?> columnType,
                                  Object sourceVal,
                                  Object expectedVal,
                                  org.apache.spark.sql.types.DataType sparkType)
    {
        org.apache.spark.sql.types.DataType[] sparkTypes = new org.apache.spark.sql.types.DataType[]{sparkType};
        String[] fieldNames = {field};
        ColumnType<?>[] cqlTypes = {columnType};
        TableSchema schema = buildSchema(fieldNames, sparkTypes, new CqlField.CqlType[]{cqlType}, fieldNames, cqlTypes, fieldNames);
        Object[] source = new Object[]{sourceVal};
        Object[] expected = new Object[]{expectedVal};
        Object[] normalized = schema.normalize(source);
        assertThat(normalized, is(equalTo(expected)));
    }
}
