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

package org.apache.cassandra.analytics;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.ASCII;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BIGINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BLOB;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BOOLEAN;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DOUBLE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TEXT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMESTAMP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMEUUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.UUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that performs bulk reads and writes using different types for the partition key to test
 * type mapping for the partition key
 */
class PartitionKeyIntegrationTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    static final List<String> REDUCED_DATASET_FOR_BOOLEAN = Arrays.asList("a", "b");
    static final String CREATE_TEST_TABLE_STATEMENT =
    "CREATE TABLE IF NOT EXISTS %s (id %s, course text, marks int, PRIMARY KEY (id)) WITH read_repair='NONE';";

    // Reference timestamp
    static final long CURRENT_TIMESTAMP = System.currentTimeMillis();

    @ParameterizedTest(name = "{index} => CQL Type for Partition Key={0}")
    @MethodSource("mappings")
    void allTypesInPartitionKey(String typeName, Function<Integer, String> ignored,
                                Function<Object[], String> columnsMapper,
                                BiFunction<Row, Integer, Object> extractKeyFromRowFn)
    {
        QualifiedName sourceTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_source");
        QualifiedName targetTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_target");

        Dataset<Row> data = bulkReaderDataFrame(sourceTableName).load();
        if (typeName.equals(BOOLEAN))
        {
            assertThat(data.count()).isEqualTo(REDUCED_DATASET_FOR_BOOLEAN.size());
        }
        else
        {
            assertThat(data.count()).isEqualTo(DATASET.size());
        }
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getInt(2)))
                                .collect(Collectors.toList());

        bulkWriterDataFrameWriter(data, targetTableName).save();
        sparkTestUtils.validateWrites(rowList, queryAllData(targetTableName), columnsMapper, extractKeyFromRowFn);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        mappings().forEach(arguments -> {
            Object[] args = arguments.get();
            Object typeName = args[0];
            QualifiedName sourceTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_source");
            QualifiedName targetTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_target");
            createTestTable(sourceTableName, String.format(CREATE_TEST_TABLE_STATEMENT, "%s", typeName));
            createTestTable(targetTableName, String.format(CREATE_TEST_TABLE_STATEMENT, "%s", typeName));

            if (typeName.equals(BOOLEAN))
            {
                populateTable(sourceTableName, REDUCED_DATASET_FOR_BOOLEAN, (Function<Integer, String>) args[1]);
            }
            else
            {
                populateTable(sourceTableName, DATASET, (Function<Integer, String>) args[1]);
            }
        });
    }

    void populateTable(QualifiedName tableName, List<String> values, Function<Integer, String> typeToRowValueFn)
    {
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            String query = String.format("INSERT INTO %s (id, course, marks) VALUES (%s,'%s',%d) ",
                                         tableName, typeToRowValueFn.apply(i), "course_" + value, 80 + i);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }
    }

    Stream<Arguments> mappings()
    {
        return Stream.of(
        Arguments.of(BIGINT, (Function<Integer, String>) String::valueOf, null,
                     (BiFunction<Row, Integer, Object>) Row::getLong),
        Arguments.of(BLOB, (Function<Integer, String>) value -> String.format("bigintAsBlob(%d)", value),
                     (Function<Object[], String>) ((Object[] columns) -> {
                         Object column = new BigInteger(((ByteBuffer) columns[0]).array()).toString();
                         return String.format("%s:%s:%s",
                                              column,
                                              columns[1],
                                              columns[2]);
                     }),
                     (BiFunction<Row, Integer, Object>) (row1, index) -> {
                         byte[] object = (byte[]) row1.get(index);
                         return new BigInteger(object).toString();
                     }),
        Arguments.of(DOUBLE, (Function<Integer, String>) String::valueOf, null,
                     (BiFunction<Row, Integer, Object>) Row::getDouble),
        Arguments.of(INT, (Function<Integer, String>) String::valueOf, null,
                     (BiFunction<Row, Integer, Object>) Row::getInt),
        Arguments.of(BOOLEAN, (Function<Integer, String>) value -> value % 2 == 0 ? "true" : "false", null,
                     (BiFunction<Row, Integer, Object>) Row::getBoolean),
        Arguments.of(TEXT, (Function<Integer, String>) value -> String.format("'%d'", value), null,
                     (BiFunction<Row, Integer, Object>) Row::getString),
        Arguments.of(TIMESTAMP, (Function<Integer, String>) offset -> String.valueOf(CURRENT_TIMESTAMP + offset), null,
                     (BiFunction<Row, Integer, Object>) (row, i) -> new Date(row.getTimestamp(i).getTime())),
        Arguments.of(UUID, (Function<Integer, String>) value -> new java.util.UUID(0, value).toString(), null,
                     (BiFunction<Row, Integer, Object>) Row::get),
        Arguments.of(VARCHAR, (Function<Integer, String>) value -> String.format("'%d'", value), null,
                     (BiFunction<Row, Integer, Object>) Row::getString),
        Arguments.of(ASCII, (Function<Integer, String>) value -> String.format("'%d'", value), null,
                     (BiFunction<Row, Integer, Object>) Row::getString),
        Arguments.of(TIMEUUID, (Function<Integer, String>) offset -> UUIDs.startOf(CURRENT_TIMESTAMP + offset).toString(),
                     null, (BiFunction<Row, Integer, Object>) Row::get)
        );
    }
}
