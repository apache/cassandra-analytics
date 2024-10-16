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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * A simple test that runs a sample read/write Cassandra Analytics job.
 */
class CassandraAnalyticsSimpleTest extends SharedClusterSparkIntegrationTestBase
{
    @ParameterizedTest
    @MethodSource("options")
    void runSampleJob(Integer ttl, Long timestamp, QualifiedName tableName)
    {
        createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);

        // The table is created in cassandra. However, sidecar keyspace endpoint relies on java driver to refresh schema,
        // which introduces certain delay (1 second refresh interval).
        // Wait for a while here to ensure sidecar is aware of schema.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        Map<String, String> writerOptions = new HashMap<>();
        if (ttl != null)
        {
            writerOptions.put(WriterOptions.TTL.name(), "ttl");
        }
        if (timestamp != null)
        {
            writerOptions.put(WriterOptions.TIMESTAMP.name(), "timestamp");
        }

        SparkSession spark = getOrCreateSparkSession();

        // Generate some data
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT, false, ttl, timestamp);

        // Write the data using Bulk Writer
        bulkWriterDataFrameWriter(dfWrite, tableName, writerOptions).save();

        // Validate using CQL
        sparkTestUtils.validateWrites(dfWrite.collectAsList(), queryAllData(tableName));

        // Remove columns from write DF to perform validations
        Dataset<Row> written = writeToReadDfFunc(ttl != null, timestamp != null).apply(dfWrite);

        // Read data back using Bulk Reader
        Dataset<Row> read = bulkReaderDataFrame(tableName).load();

        // Validate that written and read dataframes are the same
        checkSmallDataFrameEquality(written, read);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    static Stream<Arguments> options()
    {
        return Stream.of(
        Arguments.of(null, null, new QualifiedName(TEST_KEYSPACE, "test_no_tll_timestamp")),
        Arguments.of(1000, null, new QualifiedName(TEST_KEYSPACE, "test_ttl_1000")),
        Arguments.of(null, 1432815430948567L, new QualifiedName(TEST_KEYSPACE, "test_timestamp")),
        Arguments.of(1000, 1432815430948567L, new QualifiedName(TEST_KEYSPACE, "test_ttl_1000_timestamp"))
        );
    }

    // Because the read part of the integration test job doesn't read ttl and timestamp columns, we need to remove them
    // from the Dataset after it's saved.
    static Function<Dataset<Row>, Dataset<Row>> writeToReadDfFunc(boolean addedTTLColumn, boolean addedTimestampColumn)
    {
        return (Dataset<Row> df) -> {
            if (addedTTLColumn)
            {
                df = df.drop("ttl");
            }
            if (addedTimestampColumn)
            {
                df = df.drop("timestamp");
            }
            return df;
        };
    }
}
