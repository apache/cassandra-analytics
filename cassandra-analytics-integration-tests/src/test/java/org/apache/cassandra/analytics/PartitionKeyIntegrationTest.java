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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
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
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.analytics.SparkTestUtils.VALIDATION_DEFAULT_COLUMNS_MAPPER;
import static org.apache.cassandra.analytics.SparkTestUtils.VALIDATION_DEFAULT_ROW_MAPPER;
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
    static final String CREATE_TEST_TABLE_SINGLE_PK_STATEMENT =
    "CREATE TABLE IF NOT EXISTS %s (id %s, course text, marks int, PRIMARY KEY (id)) WITH read_repair='NONE';";
    static final String CREATE_TEST_TABLE_COMPOSITE_PK_STATEMENT =
    "CREATE TABLE IF NOT EXISTS %s (id %s, course %s, marks int, PRIMARY KEY ((id,course))) WITH read_repair='NONE';";

    // Reference timestamp
    static final long CURRENT_TIMESTAMP = System.currentTimeMillis();

    @ParameterizedTest(name = "CQL Type for Partition Key={0}")
    @MethodSource("mappings")
    void allTypesInSinglePartitionKey(String typeName,
                                      Function<Integer, String> ignored,
                                      Function<Object[], String> columnsMapper,
                                      Function<Row, String> rowMapper)
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
        sparkTestUtils.validateWrites(rowList, queryAllData(targetTableName), columnsMapper, rowMapper);
    }

    @ParameterizedTest(name = "CQL Type for Composite Partition Key={0}-{0}")
    @MethodSource("mappings")
    void allTypesInCompositePartitionKey(String typeName,
                                         Function<Integer, String> ignored,
                                         Function<Object[], String> columnsMapper,
                                         Function<Row, String> rowMapper)
    {
        QualifiedName compositeSourceTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_composite_source");
        QualifiedName compositeTargetTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_composite_target");

        Dataset<Row> data = bulkReaderDataFrame(compositeSourceTableName).load();
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

        bulkWriterDataFrameWriter(data, compositeTargetTableName).save();
        sparkTestUtils.validateWrites(rowList, queryAllData(compositeTargetTableName), columnsMapper, rowMapper);
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
            createTestTable(sourceTableName, String.format(CREATE_TEST_TABLE_SINGLE_PK_STATEMENT, "%s", typeName));
            createTestTable(targetTableName, String.format(CREATE_TEST_TABLE_SINGLE_PK_STATEMENT, "%s", typeName));

            QualifiedName compositeSourceTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_composite_source");
            QualifiedName compositeTargetTableName = new QualifiedName(TEST_KEYSPACE, typeName + "_composite_target");

            createTestTable(compositeSourceTableName, String.format(CREATE_TEST_TABLE_COMPOSITE_PK_STATEMENT, "%s", typeName, typeName));
            createTestTable(compositeTargetTableName, String.format(CREATE_TEST_TABLE_COMPOSITE_PK_STATEMENT, "%s", typeName, typeName));

            Function<Integer, String> typeToRowValueFn = (Function<Integer, String>) args[1];
            if (typeName.equals(BOOLEAN))
            {
                populateTable(sourceTableName, REDUCED_DATASET_FOR_BOOLEAN, typeToRowValueFn);
                populateCompositePKTable(compositeSourceTableName, REDUCED_DATASET_FOR_BOOLEAN, typeToRowValueFn);
            }
            else
            {
                populateTable(sourceTableName, DATASET, typeToRowValueFn);
                populateCompositePKTable(compositeSourceTableName, DATASET, typeToRowValueFn);
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

    void populateCompositePKTable(QualifiedName tableName, List<String> values, Function<Integer, String> typeToRowValueFn)
    {
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        for (int i = 0; i < values.size(); i++)
        {
            String value = typeToRowValueFn.apply(i);
            String query = String.format("INSERT INTO %s (id, course, marks) VALUES (%s,%s,%d) ",
                                         tableName, value, value, 80 + i);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }
    }

    Stream<Arguments> mappings()
    {
        return Stream.of(
        Arguments.of(BIGINT, (Function<Integer, String>) String::valueOf,
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(BLOB, (Function<Integer, String>) value -> String.format("bigintAsBlob(%d)", value),
                     (Function<Object[], String>) ((Object[] columns) -> {
                         Object col0 = new BigInteger(ByteBufferUtils.getArray((ByteBuffer) columns[0])).toString();
                         if (columns[1] instanceof ByteBuffer)
                         {
                             Object col1 = new BigInteger(ByteBufferUtils.getArray((ByteBuffer) columns[1])).toString();
                             return String.format("%s:%s:%s", col0, col1, columns[2]);
                         }
                         else
                         {
                             return String.format("%s:%s:%s", col0, columns[1], columns[2]);
                         }
                     }),
                     (Function<Row, String>) row -> {
                         byte[] col0 = (byte[]) row.get(0);
                         Object col1 = row.get(1);

                         if (col1 instanceof byte[])
                         {
                             return String.format("%s:%s:%d", new BigInteger(col0), new BigInteger((byte[]) col1), row.getInt(2));
                         }
                         else
                         {
                             return String.format("%s:%s:%d", new BigInteger(col0), col1, row.getInt(2));
                         }
                     }),
        Arguments.of(DOUBLE, (Function<Integer, String>) String::valueOf,
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(INT, (Function<Integer, String>) String::valueOf,
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(BOOLEAN, (Function<Integer, String>) value -> value % 2 == 0 ? "true" : "false",
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(TEXT, (Function<Integer, String>) value -> String.format("'%d'", value),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(TIMESTAMP, (Function<Integer, String>) offset -> String.valueOf(CURRENT_TIMESTAMP + offset),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER,
                     (Function<Row, String>) row -> {
                         Date col0 = new Date(row.getTimestamp(0).getTime());
                         Object col1 = row.get(1);
                         if (col1 instanceof Timestamp)
                         {
                             return String.format("%s:%s:%d", col0, new Date(((Timestamp) col1).getTime()), row.getInt(2));
                         }
                         return String.format("%s:%s:%d", col0, row.get(1), row.getInt(2));
                     }),
        Arguments.of(UUID, (Function<Integer, String>) value -> new java.util.UUID(0, value).toString(),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(VARCHAR, (Function<Integer, String>) value -> String.format("'%d'", value),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(ASCII, (Function<Integer, String>) value -> String.format("'%d'", value),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER),
        Arguments.of(TIMEUUID, (Function<Integer, String>) offset -> UUIDs.startOf(CURRENT_TIMESTAMP + offset).toString(),
                     VALIDATION_DEFAULT_COLUMNS_MAPPER, VALIDATION_DEFAULT_ROW_MAPPER)
        );
    }
}
