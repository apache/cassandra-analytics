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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.analytics.DataGenerationUtils.generateCourseData;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestKeyspaceQuotedTableFullName;
import static org.apache.cassandra.testing.TestUtils.uniqueTestQuotedKeyspaceTableFullName;

/**
 * Tests the bulk writer behavior when requiring quoted identifiers for keyspace, table name, and column names.
 *
 * <p>These tests exercise a full integration test, which includes testing Sidecar behavior when dealing with quoted
 * identifiers.
 */
class QuoteIdentifiersWriteTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<QualifiedName> TABLE_NAMES =
    Arrays.asList(uniqueTestQuotedKeyspaceTableFullName("QuOtEd_KeYsPaCe"),
                  uniqueTestQuotedKeyspaceTableFullName("keyspace"), // keyspace is a reserved word
                  uniqueTestKeyspaceQuotedTableFullName(TEST_KEYSPACE, "QuOtEd_TaBlE"),
                  new QualifiedName(TEST_KEYSPACE, "table", false, true)); // table is a reserved word

    @ParameterizedTest(name = "{index} => table={0}")
    @MethodSource("testInputs")
    void testQuoteIdentifiersBulkWrite(QualifiedName tableName)
    {
        SparkSession spark = getOrCreateSparkSession();
        // Generates course data from and renames the dataframe columns to use case-sensitive and reserved
        // words in the dataframe
        Dataset<Row> df = generateCourseData(spark, ROW_COUNT).toDF("IdEnTiFiEr", // case-sensitive struct
                                                                    "course",
                                                                    "limit"); // limit is a reserved word in Cassandra
        bulkWriterDataFrameWriter(df, tableName).option(WriterOptions.QUOTE_IDENTIFIERS.name(), "true")
                                                .save();
//        validateWritesWithDriverResultSet(df.collectAsList(),
//                                          queryAllDataWithDriver(cluster, tableName),
//                                          udfData ?
//                                          QuoteIdentifiersWriteTest::rowWithUdtFormatter :
//                                          QuoteIdentifiersWriteTest::defaultRowFormatter);
    }

    public static String defaultRowFormatter(com.datastax.driver.core.Row row)
    {
        return row.getInt("IdEnTiFiEr") +
               ":'" +
               row.getString("course") +
               "':" +
               row.getInt("limit");
    }

    @NotNull
    private static String rowWithUdtFormatter(com.datastax.driver.core.Row row)
    {
        return row.getInt("IdEnTiFiEr") +
               ":'" +
               row.getString("course") +
               "':" +
               row.getInt("limit") +
               ":" +
               row.getUDTValue("User_Defined_Type");
    }

    static Stream<Arguments> testInputs()
    {
        return TABLE_NAMES.stream().map(Arguments::of);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s " +
                                      "(\"IdEnTiFiEr\" int, course text, \"limit\" int," +
                                      " PRIMARY KEY(\"IdEnTiFiEr\"));";

        TABLE_NAMES.forEach(name -> {
            createTestKeyspace(name, DC1_RF1);
            createTestTable(name, createTableStatement);
        });
    }
}
