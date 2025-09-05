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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testAsciiNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "ascii", mockCqlType(ASCII), ColumnTypes.STRING, "ascii", "ascii", DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testBigIntNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "bigint", mockCqlType(BIGINT), ColumnTypes.INT, 1, 1L, DataTypes.IntegerType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testBlobNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "blob", mockCqlType(BLOB), ColumnTypes.BYTES,
                         new byte[]{1, 1, 1, 1}, ByteBuffer.wrap(new byte[]{1, 1, 1, 1}), DataTypes.BinaryType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testBooleanNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "boolean", mockCqlType(BOOLEAN), ColumnTypes.BOOLEAN, false, false, DataTypes.BooleanType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testDecimalNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "decimal", mockCqlType(DECIMAL), ColumnTypes.DOUBLE,
                         BigDecimal.valueOf(1.1), BigDecimal.valueOf(1.1), DataTypes.createDecimalType());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testDoubleNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "double", mockCqlType(DOUBLE), ColumnTypes.DOUBLE, 1.1, 1.1, DataTypes.DoubleType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testFloatNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "float", mockCqlType(FLOAT), ColumnTypes.DOUBLE, 1.1f, 1.1f, DataTypes.FloatType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInetNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "inet", mockCqlType(INET), ColumnTypes.STRING,
                         "192.168.1.1", InetAddresses.forString("192.168.1.1"), DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testIntNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "int", mockCqlType(INT), ColumnTypes.INT, 1, 1, DataTypes.IntegerType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTextNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "text", mockCqlType(TEXT), ColumnTypes.BYTES, "text", "text", DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTimestampNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "timestamp", mockCqlType(TIMESTAMP), ColumnTypes.LONG,
                         new Date(1), new Date(1), DataTypes.DateType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testUuidNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "uuid", mockCqlType(UUID), ColumnTypes.UUID,
                         "382d3b34-22af-4b2a-97a3-ae7dbf9e6abe",
                         java.util.UUID.fromString("382d3b34-22af-4b2a-97a3-ae7dbf9e6abe"), DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testVarcharNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "varchar", mockCqlType(VARCHAR), ColumnTypes.STRING,
                         "varchar", "varchar", DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testVarIntNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "varint", mockCqlType(VARINT), ColumnTypes.INT,
                         "1", BigInteger.valueOf(1), DataTypes.createDecimalType(38, 0));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTimeUuidNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "timeuuid", mockCqlType(TIMEUUID), ColumnTypes.UUID,
                         java.util.UUID.fromString("0846b690-ce35-11e7-8871-79b4d1aa8ef2"),
                         java.util.UUID.fromString("0846b690-ce35-11e7-8871-79b4d1aa8ef2"), DataTypes.StringType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testSetNormalization(String cassandraVersion)
    {
        Set<String> set = new HashSet<>();
        set.add("A");
        set.add("B");
        set.add("C");

        assertNormalized(cassandraVersion, "set", mockSetCqlType(TEXT), new SetType<>(ColumnTypes.STRING),
                         set, set, DataTypes.createArrayType(DataTypes.StringType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testListNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "list", mockListCqlType(INT), new ListType<>(ColumnTypes.INT),
                         Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3),
                         DataTypes.createArrayType(DataTypes.IntegerType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMapNormalization(String cassandraVersion)
    {
        Map<String, String> map = Stream.of(new SimpleEntry<>("Foo", "Bar"))
                                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        assertNormalized(cassandraVersion, "map", mockMapCqlType(TEXT, TEXT), new MapType<>(ColumnTypes.STRING, ColumnTypes.STRING),
                         map, map, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testSmallIntNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "smallint", mockCqlType(SMALLINT), ColumnTypes.INT, (short) 2, (short) 2, DataTypes.ShortType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTinyIntNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "tiny", mockCqlType(TINYINT), ColumnTypes.INT, (byte) 3, (byte) 3, DataTypes.ByteType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testDateNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "date", mockCqlType(DATE), ColumnTypes.LONG, new Date(2), -2147483648, DataTypes.DateType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTimeNormalizationFromTimestamp(String cassandraVersion)
    {
        Timestamp timestamp = new Timestamp(0, 0, 0, 0, 0, 0, 3);
        assertNormalized(cassandraVersion, "time", mockCqlType(TIME), ColumnTypes.LONG, timestamp, 3L, DataTypes.TimestampType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testTimeNormalizationFromLong(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "time", mockCqlType(TIME), ColumnTypes.LONG,
                         7L, 7L, DataTypes.LongType);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testByteArrayListNormalization(String cassandraVersion)
    {
        assertNormalized(cassandraVersion, "byte_array_list", mockListCqlType(BLOB), new ListType<>(ColumnTypes.BYTES),
                         Arrays.asList(new byte[]{1}, new byte[]{2}, new byte[]{3}),
                         Arrays.asList(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2}), ByteBuffer.wrap(new byte[]{3})),
                         DataTypes.createArrayType(DataTypes.BinaryType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testByteArrayMapNormalization(String cassandraVersion)
    {
        byte[] bytes = {'B', 'a', 'r'};

        Map<String, byte[]> source = Stream.of(new SimpleEntry<>("Foo", bytes))
                                           .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        Map<String, ByteBuffer> expected = Stream.of(new SimpleEntry<>("Foo", ByteBuffer.wrap(bytes)))
                                                 .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        assertNormalized(cassandraVersion, "mapWithBytes", mockMapCqlType(TEXT, BLOB),
                         new MapType<>(ColumnTypes.STRING, ColumnTypes.STRING),
                         source, expected, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testByteArraySetNormalization(String cassandraVersion)
    {
        byte[] bytes = {'B', 'a', 'r'};

        Set<byte[]> source = new HashSet<>(Arrays.asList(new byte[][]{bytes}));
        Set<ByteBuffer> expected = new HashSet<>(Collections.singletonList(ByteBuffer.wrap(bytes)));
        assertNormalized(cassandraVersion, "setWithBytes", mockSetCqlType(BLOB),
                         new SetType<>(ColumnTypes.BYTES),
                         source, expected, DataTypes.createArrayType(DataTypes.BinaryType));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testNestedNormalization(String cassandraVersion)
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
        assertNormalized(cassandraVersion, "byte_array_list", cqlType,
                         new MapType<>(ColumnTypes.STRING, new ListType<>(ColumnTypes.BYTES)),
                         source, expected, DataTypes.createMapType(DataTypes.StringType,
                                                                   DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.BinaryType))));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testUdtNormalization(String cassandraVersion)
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
        assertNormalized(cassandraVersion, "udt_field", cqlType,
                         new MapType<>(ColumnTypes.STRING, new ListType<>(ColumnTypes.BYTES)),
                         source, udtValue, structType);
    }

    private void assertNormalized(String cassandraVersion,
                                  String field,
                                  CqlField.CqlType cqlType,
                                  ColumnType<?> columnType,
                                  Object sourceVal,
                                  Object expectedVal,
                                  org.apache.spark.sql.types.DataType sparkType)
    {
        org.apache.spark.sql.types.DataType[] sparkTypes = new org.apache.spark.sql.types.DataType[]{sparkType};
        String[] fieldNames = {field};
        ColumnType<?>[] cqlTypes = {columnType};
        TableSchema schema = buildSchema(cassandraVersion, fieldNames, sparkTypes, new CqlField.CqlType[]{cqlType}, fieldNames, cqlTypes, fieldNames);
        Object[] source = new Object[]{sourceVal};
        Object[] expected = new Object[]{expectedVal};
        Object[] normalized = schema.normalize(source);
        assertThat(normalized, is(equalTo(expected)));
    }
}
