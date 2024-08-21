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

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteUdtTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName UDT_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_udt");
    static final QualifiedName NESTED_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_nested_udt");
    public static final String TWO_FIELD_UDT_NAME = "two_field_udt";
    public static final String NESTED_FIELD_UDT_NAME = "nested_udt";
    public static final String UDT_TABLE_CREATE = "CREATE TABLE " + UDT_TABLE_NAME + " (\n"
                                                  + "          id BIGINT PRIMARY KEY,\n"
                                                  + "          udtfield " + TWO_FIELD_UDT_NAME + ");";
    public static final String TWO_FIELD_UDT_DEF = "CREATE TYPE " + UDT_TABLE_NAME.keyspace() + "."
                                                   + TWO_FIELD_UDT_NAME + " (\n"
                                                   + "            f1 text,\n"
                                                   + "            f2 int);";
    public static final String NESTED_UDT_DEF = "CREATE TYPE " + NESTED_TABLE_NAME.keyspace() + "."
                                                + NESTED_FIELD_UDT_NAME + " (\n"
                                                + "            n1 BIGINT,\n"
                                                + "            n2 frozen<" + TWO_FIELD_UDT_NAME + ">"
                                                + ");";
    public static final String NESTED_TABLE_CREATE = "CREATE TABLE " + NESTED_TABLE_NAME + "(\n"
                                                     + "           id BIGINT PRIMARY KEY,\n"
                                                     + "           nested " + NESTED_FIELD_UDT_NAME + ");";

    @Test
    void testWriteWithUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Predicate<Integer> nullSetter = index -> index % 2 == 0;
        Dataset<Row> df = DataGenerationUtils.generateUdtData(spark, ROW_COUNT, nullSetter);

        bulkWriterDataFrameWriter(df, UDT_TABLE_NAME).save();

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + UDT_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWritesWithDriverResultSet(df.collectAsList(),
                                          queryAllDataWithDriver(UDT_TABLE_NAME),
                                          BulkWriteUdtTest::defaultRowFormatter);
    }

    @Test
    void testWriteWithNestedUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Predicate<Integer> nullSetter = index -> index % 2 == 0;
        Dataset<Row> df = DataGenerationUtils.generateNestedUdtData(spark, ROW_COUNT, nullSetter);

        bulkWriterDataFrameWriter(df, NESTED_TABLE_NAME).save();

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + NESTED_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWritesWithDriverResultSet(df.collectAsList(),
                                          queryAllDataWithDriver(NESTED_TABLE_NAME),
                                          BulkWriteUdtTest::defaultRowFormatter);
    }

    @NotNull
    public static String defaultRowFormatter(com.datastax.driver.core.Row row)
    {
        // Formats as field:value with no whitespaces, and strings quoted
        // Driver Codec writes "NULL" for null value. Spark DF writes "null".
        return row.getLong(0) +
               ":" +
               row.getUDTValue(1).toString().replace("NULL", "null");
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(UDT_TABLE_NAME, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances(TWO_FIELD_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_TABLE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_TABLE_CREATE);
    }
}
