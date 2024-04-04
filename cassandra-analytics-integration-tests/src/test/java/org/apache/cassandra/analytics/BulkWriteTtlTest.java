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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.TTLOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteTtlTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName DEFAULT_TTL_NAME = new QualifiedName(TEST_KEYSPACE, "test_default_ttl");
    static final QualifiedName CONSTANT_TTL_NAME = new QualifiedName(TEST_KEYSPACE, "test_ttl_constant");
    static final QualifiedName PER_ROW_TTL_NAME = new QualifiedName(TEST_KEYSPACE, "test_ttl_per_row");

    @Test
    void testTableDefaultTtl()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(df, DEFAULT_TTL_NAME).save();

        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + DEFAULT_TTL_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void testTtlOptionConstant()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(df, CONSTANT_TTL_NAME).option(WriterOptions.TTL.name(), TTLOption.constant(1))
                                                        .save();
        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + CONSTANT_TTL_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void testTtlOptionPerRow()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, 1, null, ROW_COUNT);

        bulkWriterDataFrameWriter(df, PER_ROW_TTL_NAME).option(WriterOptions.TTL.name(), TTLOption.perRow("ttl"))
                                                       .save();
        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + PER_ROW_TTL_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isFalse();
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
        createTestKeyspace(DEFAULT_TTL_NAME, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + DEFAULT_TTL_NAME + " (\n"
                                                     + "          id BIGINT PRIMARY KEY,\n"
                                                     + "          course TEXT,\n"
                                                     + "          marks BIGINT\n"
                                                     + "     )  WITH default_time_to_live = 1;"
        );
        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + CONSTANT_TTL_NAME + " (\n"
                                                     + "          id BIGINT PRIMARY KEY,\n"
                                                     + "          course TEXT,\n"
                                                     + "          marks BIGINT\n"
                                                     + "     );"
        );
        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + PER_ROW_TTL_NAME + " (\n"
                                                     + "          id BIGINT PRIMARY KEY,\n"
                                                     + "          course TEXT,\n"
                                                     + "          marks BIGINT\n"
                                                     + "     );"
        );
    }
}
