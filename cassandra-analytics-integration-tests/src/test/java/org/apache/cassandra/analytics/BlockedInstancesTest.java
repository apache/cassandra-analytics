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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Collection of in-jvm-dtest based integration tests to validate the bulk write path when blocked instances
 * are configured.
 *
 * Note: Since these tests are derived from {@link SharedClusterSparkIntegrationTestBase}, the tests reuse the same
 * cluster with a unique table per test case. The implicit mapping from the test case to the table is made by
 * creating the tables using the method name in {@link BlockedInstancesTest#initializeSchemaForTest()}.
 */
public class BlockedInstancesTest extends ResiliencyTestBase
{
    Dataset<Row> df;
    Map<IInstance, Set<String>> expectedInstanceData;

    @Test
    void testSingleBlockedNodeSucceeds(TestInfo testInfo)
    {
        TestConsistencyLevel cl = TestConsistencyLevel.of(QUORUM, QUORUM);
        QualifiedName table = new QualifiedName(TEST_KEYSPACE, testInfo.getTestMethod().get().getName().toLowerCase());
        bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                            .option(WriterOptions.BLOCKED_CASSANDRA_INSTANCES.name(), "127.0.0.2")
                                            .save();
        expectedInstanceData.entrySet()
                            .stream()
                            .filter(e -> e.getKey().broadcastAddress().getAddress().getHostAddress().equals("127.0.0.2"))
                            .forEach(e -> e.setValue(Collections.emptySet()));
        validateNodeSpecificData(table, expectedInstanceData);
        validateData(table, cl.readCL, ROW_COUNT);
    }

    @Test
    void testSingleBlockedNodeWithWriteFailure(TestInfo testInfo)
    {
        TestConsistencyLevel cl = TestConsistencyLevel.of(ONE, ALL);
        QualifiedName table = new QualifiedName(TEST_KEYSPACE, testInfo.getTestMethod().get().getName().toLowerCase());
        Throwable thrown = catchThrowable(() ->
                                          bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                                                              .option(WriterOptions.BLOCKED_CASSANDRA_INSTANCES.name(), "127.0.0.2")
                                                                              .save());
        validateFailedJob(table, cl, thrown);
    }

    @Test
    void testMultipleBlockedNodesWithWriteFailure(TestInfo testInfo)
    {
        TestConsistencyLevel cl = TestConsistencyLevel.of(QUORUM, QUORUM);
        QualifiedName table = new QualifiedName(TEST_KEYSPACE, testInfo.getTestMethod().get().getName().toLowerCase());
        Throwable thrown = catchThrowable(() ->
                                          bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                                                              .option(WriterOptions.BLOCKED_CASSANDRA_INSTANCES.name(), "127.0.0.2,127.0.0.3")
                                                                              .save());
        validateFailedJob(table, cl, thrown);
    }

    private void validateFailedJob(QualifiedName table, TestConsistencyLevel cl, Throwable thrown)
    {
        assertThat(thrown).isInstanceOf(RuntimeException.class)
                          .hasMessageContaining("java.lang.RuntimeException: Bulk Write to Cassandra has failed");

        Throwable cause = thrown;
        while (cause != null && !StringUtils.contains(cause.getMessage(), "Failed to load"))
        {
            cause = cause.getCause();
        }

        assertThat(cause).isNotNull()
                         .hasMessageFindingMatch(String.format("Failed to load (\\d+) ranges with %s for " +
                                                 "job ([a-zA-Z0-9-]+) in phase Environment Validation.", cl.writeCL));

        expectedInstanceData.entrySet()
                            .stream()
                            .forEach(e -> e.setValue(Collections.emptySet()));
        validateNodeSpecificData(table, expectedInstanceData);
        validateData(table, cl.readCL, 0);

    }


    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .requestFeature(Feature.NETWORK);
    }

    @Override
    protected void beforeTestStart()
    {
        SparkSession spark = getOrCreateSparkSession();
        // Generate some artificial data for the test
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        // Generate the expected data for the new instances
        expectedInstanceData = generateExpectedInstanceData(cluster, Collections.emptyList(), ROW_COUNT);
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        return clusterBuilder(testClusterConfiguration(), testVersion);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        Method[] methods = this.getClass().getDeclaredMethods();
        for (Method method : methods)
        {
            if (method.isAnnotationPresent(Test.class))
            {
                createTestTable(new QualifiedName(TEST_KEYSPACE, method.getName().toLowerCase()), CREATE_TEST_TABLE_STATEMENT);
            }
        }
    }
}
