/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.ScalaConversionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.Seq;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createMapType;
import static org.assertj.core.api.Assertions.assertThatException;

/**
 * Tests bulk writes with different data types. More types can be added to the test
 * as we support more types.
 */
class BulkWriteDataTypesTest extends SharedClusterSparkIntegrationTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("testArguments")
    void testType(String tableName, TypeTestSetup typeTestSetup)
    {
        SparkSession spark = getOrCreateSparkSession();

        Dataset<Row> df = generateDataset(spark, typeTestSetup);

        QualifiedName table = new QualifiedName(TEST_KEYSPACE, tableName);
        if (typeTestSetup.expectedFailureMessage != null)
        {
            assertThatException()
            .isThrownBy(() -> bulkWriterDataFrameWriter(df, table).save())
            .withMessageContaining(typeTestSetup.expectedFailureMessage);
        }
        else
        {
            bulkWriterDataFrameWriter(df, table).save();
            sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(table),
                                          typeTestSetup.columnMapperValidation, typeTestSetup.rowMapperValidation);
        }
    }

    Dataset<Row> generateDataset(SparkSession spark, TypeTestSetup typeTestSetup)
    {
        StructType schema = new StructType();
        for (int i = 0; i < typeTestSetup.columns.size(); i++)
        {
            schema = schema.add(typeTestSetup.columns.get(i), typeTestSetup.columnTypes.get(i), false);
        }

        List<Row> rows = IntStream.range(0, typeTestSetup.numRows)
                                  .mapToObj(recordNum -> {
                                      List<Object> values = new ArrayList<>(typeTestSetup.columns.size());
                                      for (Function<Integer, Object> fn : typeTestSetup.valueFunction)
                                      {
                                          values.add(fn.apply(recordNum));
                                      }
                                      return RowFactory.create(values.toArray());
                                  }).collect(Collectors.toList());
        return spark.createDataFrame(rows, schema);
    }

    /**
     * Initialize required schemas for the tests upfront before the test starts
     */
    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        typesToTest().forEach(typeTestSetup -> {
            QualifiedName tableName = new QualifiedName(TEST_KEYSPACE, typeTestSetup.tableName);
            createTestTable(tableName, typeTestSetup.createTableSchema);
        });
    }

    /**
     * @return cartesian product of the list of consistency levels and instance down
     */
    static Stream<Arguments> testArguments()
    {
        return typesToTest().stream().map(typeTestSetup -> Arguments.of(typeTestSetup.tableName, typeTestSetup));
    }

    static List<TypeTestSetup> typesToTest()
    {
        List<TypeTestSetup> types = new ArrayList<>();

        // Simple schema with a bigint column.
        types.add(simpleBigIntSchemaSetup());

        // Simple schema with Date as column.
        types.add(simpleDateSchemaSetup());

        // Simple schema with integers and strings as columns.
        types.add(integersAndStringsSchemaSetup());

        // Simple schema with TimeUUID as column.
        types.add(timeUUIDSchemaSetup());

        // Simple schema with TimeUUID as column, attempt to populate with random UUID.
        types.add(randomUUIDFailureSchemaSetup());

        // Schema with byte array as columns.
        types.add(byteArrayColumnSetup());

        // Schema with List of int as columns.
        types.add(intListSchemaSetup());

        // Schema with List of bytes as column.
        types.add(byteListSchemaSetup());

        // Schema with set of lists as column.
        types.add(stringSetSchemaSetup());

        // Schema with Map of bytes as column
        // NOTE: Also testing CASSANDRA-17623 (map serialization) with this test, thus the primary key of id and mapdata
        types.add(mapByteSchemaSetup());

        // Schema with nested datatypes as column
        types.add(mapListSchemaSetup());

        // Schema with nested datatypes as column
        types.add(nestedDataTypesSchemaSetup());

        // Time with Timestamp as source.
        // Note that Spark only supports milliseconds in its timestamp column,
        // so we only test to millisecond precision here
        types.add(timeSchemaTimestampSetup());

        // Time with Long as source.
        types.add(timeWithLongSourceSchemaSetup());

        // Old tables (created in C* 1.2) with Timestamp columns from 1.2 were changed in 2.0 to return CUSTOM(DATE).
        // The SBW handles these by detecting CUSTOM columns with a date type and internally treating them like timestamps.
        // Note that Spark only supports milliseconds in its timestamp column, so we only test to millisecond precision here
        types.add(customDateTypeSchemaSetup());

        return types;
    }

    static TypeTestSetup simpleBigIntSchemaSetup()
    {
        return new TypeTestSetup("bigint_schema",
                                 Arrays.asList("id", "marks"),
                                 Arrays.asList(IntegerType, LongType),
                                 Arrays.asList(INTEGER_MAPPER, LONG_MAPPER),
                                 "CREATE TABLE %s (id int, marks bigint, PRIMARY KEY (id))");
    }

    static TypeTestSetup simpleDateSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("date_schema",
                                                Arrays.asList("id", "course"),
                                                Arrays.asList(IntegerType, DateType),
                                                Arrays.asList(INTEGER_MAPPER, DATE_MAPPER),
                                                "CREATE TABLE %s (id int, course date, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> String.format("%s:%s", row.get(0), SqlToCqlTypeConverter.DATE_CONVERTER.convertInternal(row.get(1)));
        return setup;
    }

    static TypeTestSetup integersAndStringsSchemaSetup()
    {
        return new TypeTestSetup("simple_schema",
                                 Arrays.asList("id", "course", "marks"),
                                 Arrays.asList(IntegerType, StringType, IntegerType),
                                 Arrays.asList(INTEGER_MAPPER, STRING_MAPPER, INTEGER_MAPPER),
                                 "CREATE TABLE %s (id int, course text, marks int, PRIMARY KEY (id))");
    }

    static TypeTestSetup timeUUIDSchemaSetup()
    {
        return new TypeTestSetup("timeuuid_schema",
                                 Arrays.asList("id", "course"),
                                 Arrays.asList(IntegerType, StringType),
                                 Arrays.asList(INTEGER_MAPPER, TIME_UUID_MAPPER),
                                 "CREATE TABLE %s (id int, course timeuuid, PRIMARY KEY (id))");
    }

    static TypeTestSetup randomUUIDFailureSchemaSetup()
    {
        return new TypeTestSetup("timeuuid_schema_bad_uuid",
                                 Arrays.asList("id", "course"),
                                 Arrays.asList(IntegerType, StringType),
                                 Arrays.asList(INTEGER_MAPPER, RANDOM_UUID_MAPPER),
                                 "CREATE TABLE %s (id int, course timeuuid, PRIMARY KEY (id))",
                                 "Bulk Write to Cassandra has failed");
    }

    static TypeTestSetup byteArrayColumnSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("bytearray_column",
                                                Arrays.asList("id", "binarydata"),
                                                Arrays.asList(IntegerType, BinaryType),
                                                Arrays.asList(INTEGER_MAPPER, BINARY_MAPPER),
                                                "CREATE TABLE %s (id int, binarydata blob, PRIMARY KEY (id))");
        setup.rowMapperValidation =
        row -> String.format("%s:%s", row.get(0), new String((byte[]) row.get(1), StandardCharsets.UTF_8));
        setup.columnMapperValidation = columns -> {
            Object col1 = new String(ByteBufferUtils.getArray((ByteBuffer) columns[1]), StandardCharsets.UTF_8);
            return String.format("%s:%s", columns[0], col1);
        };
        return setup;
    }

    static TypeTestSetup intListSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("list_column",
                                                Arrays.asList("id", "listdata"),
                                                Arrays.asList(IntegerType, createArrayType(IntegerType)),
                                                Arrays.asList(INTEGER_MAPPER, INTEGER_ARRAY_MAPPER),
                                                "CREATE TABLE %s (id int, listdata LIST<int>, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> String.format("%s:%s", row.get(0), row.getList(1));
        return setup;
    }

    @SuppressWarnings("unchecked")
    static TypeTestSetup byteListSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("byte_list_column",
                                                Arrays.asList("id", "listdata"),
                                                Arrays.asList(IntegerType, createArrayType(BinaryType)),
                                                Arrays.asList(INTEGER_MAPPER, BYTE_ARRAY_MAPPER),
                                                "CREATE TABLE %s (id int, listdata LIST<blob>, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> {
            List<byte[]> byteList = row.getList(1);
            return String.format("%s:%s", row.get(0), byteList.stream()
                                                              .map(b -> new String(b, StandardCharsets.UTF_8))
                                                              .collect(Collectors.toList()));
        };
        setup.columnMapperValidation = columns -> {
            List<ByteBuffer> byteBufferList = (List<ByteBuffer>) columns[1];
            return String.format("%s:%s", columns[0], byteBufferList.stream()
                                                                    .map(b -> new String(ByteBufferUtils.getArray(b), StandardCharsets.UTF_8))
                                                                    .collect(Collectors.toList()));
        };
        return setup;
    }

    static TypeTestSetup stringSetSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("set_list_column",
                                                Arrays.asList("id", "setdata"),
                                                Arrays.asList(IntegerType, createArrayType(StringType)),
                                                Arrays.asList(INTEGER_MAPPER, STRING_SET_MAPPER),
                                                "CREATE TABLE %s (id int, setdata set<text>, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> String.format("%s:%s", row.get(0), row.getList(1));
        return setup;
    }

    @SuppressWarnings("unchecked")
    static TypeTestSetup mapByteSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("map_byte_column",
                                                Arrays.asList("id", "mapdata"),
                                                Arrays.asList(IntegerType, createMapType(StringType, BinaryType)),
                                                Arrays.asList(INTEGER_MAPPER, STRING_BINARY_MAP_MAPPER),
                                                "CREATE TABLE %s (id int, mapdata frozen<map<text,blob>>, PRIMARY KEY (id, mapdata))");
        setup.rowMapperValidation = row -> {
            Map<?, ?> map = row.getJavaMap(1);
            String value = map.entrySet().stream()
                              .map(entry -> String.format("%s=%s", entry.getKey(),
                                                          new String((byte[]) entry.getValue(), StandardCharsets.UTF_8)))
                              .collect(Collectors.joining(", ", "[", "]"));
            return String.format("%s:%s", row.get(0), value);
        };
        setup.columnMapperValidation = columns -> {
            Map<String, ByteBuffer> map = (Map<String, ByteBuffer>) columns[1];
            String value = map.entrySet().stream()
                              .map(entry -> String.format("%s=%s", entry.getKey(),
                                                          new String(ByteBufferUtils.getArray(entry.getValue()), StandardCharsets.UTF_8)))
                              .collect(Collectors.joining(", ", "[", "]"));
            return String.format("%s:%s", columns[0], value);
        };
        return setup;
    }

    @SuppressWarnings("unchecked")
    static TypeTestSetup mapListSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("map_list_column",
                                                Arrays.asList("id", "mapdata"),
                                                Arrays.asList(IntegerType, createMapType(StringType, createArrayType(IntegerType))),
                                                Arrays.asList(INTEGER_MAPPER, STRING_ARRAY_INTEGER_MAP_MAPPER),
                                                "CREATE TABLE %s (id int, mapdata map<text,frozen<list<int>>>, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> {
            Map<?, ?> map = row.getJavaMap(1);
            Map<String, List<Integer>> value = map.entrySet()
                                                  .stream()
                                                  .sorted(Comparator.comparing(e -> (String) e.getKey()))
                                                  .collect(Collectors.toMap(e -> (String) e.getKey(),
                                                                            e -> ScalaConversionUtils.mutableSeqAsJavaList((Seq<Integer>) e.getValue()),
                                                                            (x, y) -> y,
                                                                            LinkedHashMap::new));
            return String.format("%s:%s", row.get(0), value);
        };
        return setup;
    }

    @SuppressWarnings("unchecked")
    static TypeTestSetup nestedDataTypesSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("map_list_byte_column",
                                                Arrays.asList("id", "mapdata"),
                                                Arrays.asList(IntegerType, createMapType(StringType, createArrayType(BinaryType))),
                                                Arrays.asList(INTEGER_MAPPER, STRING_ARRAY_BINARY_MAP_MAPPER),
                                                "CREATE TABLE %s (id int, mapdata map<text,frozen<list<blob>>>, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> {
            Map<String, Seq<byte[]>> map = row.getJavaMap(1);
            Function<byte[], String> unwrapBytes = b -> new String(b, StandardCharsets.UTF_8);
            String value = map.entrySet().stream()
                              .map(entry -> String.format("%s=%s", entry.getKey(),
                                                          ScalaConversionUtils.mutableSeqAsJavaList(entry.getValue())
                                                                              .stream()
                                                                              .map(unwrapBytes)
                                                                              .collect(Collectors.toList())))
                              .collect(Collectors.joining(", ", "[", "]"));
            return String.format("%s:%s", row.get(0), value);
        };
        setup.columnMapperValidation = columns -> {
            Map<String, List<ByteBuffer>> map = (Map<String, List<ByteBuffer>>) columns[1];
            Function<ByteBuffer, String> unwrapBytes = b -> new String(ByteBufferUtils.getArray(b), StandardCharsets.UTF_8);
            String value = map.entrySet().stream()
                              .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue().stream().map(unwrapBytes).collect(Collectors.toList())))
                              .collect(Collectors.joining(", ", "[", "]"));
            return String.format("%s:%s", columns[0], value);
        };
        return setup;
    }

    static TypeTestSetup timeSchemaTimestampSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("time_schema_timestamp",
                                                Arrays.asList("id", "course"),
                                                Arrays.asList(IntegerType, TimestampType),
                                                Arrays.asList(INTEGER_MAPPER, TIMESTAMP_MAPPER),
                                                "CREATE TABLE %s (id int, course time, PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> String.format("%s:%s", row.get(0), SqlToCqlTypeConverter.TIME_CONVERTER.convertInternal(row.get(1)));

        return setup;
    }

    static TypeTestSetup timeWithLongSourceSchemaSetup()
    {
        return new TypeTestSetup("time_schema_long",
                                 Arrays.asList("id", "course"),
                                 Arrays.asList(IntegerType, LongType),
                                 Arrays.asList(INTEGER_MAPPER, LONG_MAPPER),
                                 "CREATE TABLE %s (id int, course time, PRIMARY KEY (id))");
    }

    static TypeTestSetup customDateTypeSchemaSetup()
    {
        TypeTestSetup setup = new TypeTestSetup("c_12_ts_as_custom_schema_timestamp",
                                                Arrays.asList("id", "course"),
                                                Arrays.asList(IntegerType, LongType),
                                                Arrays.asList(INTEGER_MAPPER, LONG_MAPPER),
                                                "CREATE TABLE %s (id int, course 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (id))");
        setup.rowMapperValidation = row -> String.format("%s:%s", row.get(0), SqlToCqlTypeConverter.TIMESTAMP_CONVERTER.convertInternal(row.get(1)));
        return setup;
    }

    static final Function<Integer, Object> INTEGER_MAPPER = recordNumber -> recordNumber;
    static final Function<Integer, Object> INTEGER_ARRAY_MAPPER = recordNumber -> Arrays.asList(recordNumber, recordNumber, recordNumber);
    static final Function<Integer, Object> BYTE_ARRAY_MAPPER = recordNumber -> Arrays.asList(("course" + recordNumber).getBytes(StandardCharsets.UTF_8),
                                                                                             ("course" + recordNumber).getBytes(StandardCharsets.UTF_8));
    static final Function<Integer, Object> STRING_SET_MAPPER = recordNumber -> ImmutableSet.of(String.format("course%06d", recordNumber),
                                                                                               String.format("course%06d", recordNumber + 1));
    static final Function<Integer, Object> STRING_BINARY_MAP_MAPPER
    = recordNumber -> ImmutableMap.of(String.format("course%06d", recordNumber), ("course" + recordNumber).getBytes(StandardCharsets.UTF_8),
                                      String.format("course%06d", recordNumber + 1), ("course" + (recordNumber + 1)).getBytes(StandardCharsets.UTF_8));
    static final Function<Integer, Object> STRING_ARRAY_INTEGER_MAP_MAPPER
    = recordNumber -> ImmutableMap.of(String.format("course%06d", recordNumber), Collections.singletonList(recordNumber),
                                      String.format("course%06d", recordNumber + 1), Collections.singletonList(recordNumber + 1));
    static final Function<Integer, Object> STRING_ARRAY_BINARY_MAP_MAPPER
    = recordNumber -> ImmutableMap.of(String.format("course%06d", recordNumber),
                                      Collections.singletonList(("course" + recordNumber).getBytes(StandardCharsets.UTF_8)),
                                      String.format("course%06d", recordNumber + 1),
                                      Collections.singletonList(("course" + (recordNumber + 1)).getBytes(StandardCharsets.UTF_8)));
    static final Function<Integer, Object> LONG_MAPPER = recordNumber -> (long) recordNumber;
    static final Function<Integer, Object> STRING_MAPPER = recordNumber -> "course" + recordNumber;
    static final Function<Integer, Object> BINARY_MAPPER = recordNumber -> ("course" + recordNumber).getBytes(StandardCharsets.UTF_8);
    static final Function<Integer, Object> TIME_UUID_MAPPER = recordNumber -> UUIDs.timeBased().toString();
    static final Function<Integer, Object> RANDOM_UUID_MAPPER = recordNumber -> UUID.randomUUID().toString();
    static final Function<Integer, Object> TIMESTAMP_MAPPER
    = recordNumber -> Timestamp.from(new Date(1731457509115L).toInstant().plus(recordNumber, ChronoUnit.SECONDS));
    static final Function<Integer, Object> DATE_MAPPER
    = recordNumber -> java.sql.Date.valueOf(((Timestamp) TIMESTAMP_MAPPER.apply(recordNumber)).toLocalDateTime().toLocalDate());

    static class TypeTestSetup
    {
        final String tableName;
        final List<String> columns;
        final List<DataType> columnTypes;
        final List<Function<Integer, Object>> valueFunction;
        final String createTableSchema;
        final String expectedFailureMessage;
        final int numRows = 10_000;
        Function<Object[], String> columnMapperValidation =
        columns -> String.format(String.join(":", Collections.nCopies(columns.length, "%s")), columns);
        Function<Row, String> rowMapperValidation = row -> {
            int size = row.size();
            Object[] data = new Object[size];
            for (int i = 0; i < size; i++)
            {
                data[i] = row.get(i);
            }
            return String.format(String.join(":", Collections.nCopies(size, "%s")), data);
        };

        TypeTestSetup(String tableName,
                      List<String> columns,
                      List<DataType> columnTypes,
                      List<Function<Integer, Object>> valueFunction,
                      String createTableSchema)
        {
            this.tableName = tableName;
            this.columns = columns;
            this.columnTypes = columnTypes;
            this.valueFunction = valueFunction;
            this.createTableSchema = createTableSchema;
            this.expectedFailureMessage = null;
        }

        TypeTestSetup(String tableName,
                      List<String> columns,
                      List<DataType> columnTypes,
                      List<Function<Integer, Object>> valueFunction,
                      String createTableSchema,
                      String expectedFailureMessage)
        {
            this.tableName = tableName;
            this.columns = columns;
            this.columnTypes = columnTypes;
            this.valueFunction = valueFunction;
            this.createTableSchema = createTableSchema;
            this.expectedFailureMessage = expectedFailureMessage;
        }
    }
}
