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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static org.apache.cassandra.analytics.ResiliencyTestBase.uniqueTestTableFullName;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk writes in different scenarios when Cassandra instances are down
 */
class BulkWriteDownInstanceTest extends SharedClusterSparkIntegrationTestBase
{
    Set<IInstance> downInstances = new HashSet<>();

    @ParameterizedTest(name = "{index} => instanceDownCount={0} {1}")
    @MethodSource("testInputs")
    void testHandlingOfDownedCassandraInstances(int instanceDownCount, TestConsistencyLevel cl)
    {
        // progressively stop instances as needed for the test
        stopCassandraInstancesForTest(instanceDownCount);

        QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        DataFrameWriter<Row> dfWriter = bulkWriterDataFrameWriter(df, tableName).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name());
        if (isFailureExpected(instanceDownCount, cl))
        {
            sparkTestUtils.assertExpectedBulkWriteFailure(cl.writeCL.name(), dfWriter);
        }
        else
        {
            dfWriter.save();
            // Validate using CQL
            sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(tableName, cl.readCL.name()));
        }
    }

    static boolean isFailureExpected(int instanceDownCount, TestConsistencyLevel cl)
    {
        if (instanceDownCount == 2)
        {
            // for a 3 instance cluster, if 2 instances are down, we can only write at consistency level ONE
            return cl.writeCL != ONE;
        }
        if (instanceDownCount == 1)
        {
            // when one instance is down, we expect failure at CL = ALL
            return cl.writeCL == ALL;
        }
        // No failure is expected when all instances are up
        return false;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        testConsistencyLevels().forEach(consistencyLevel -> {
            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, consistencyLevel.readCL, consistencyLevel.writeCL);
            createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().nodesPerDc(3);
    }

    void stopCassandraInstancesForTest(int instanceDownCount)
    {
        while (instanceDownCount > downInstances.size())
        {
            for (IInstance instance : cluster)
            {
                if (downInstances.add(instance))
                {
                    cluster.stopUnchecked(instance);
                    break;
                }
            }
        }
    }

    /**
     * @return cartesian product of the list of consistency levels and instance down
     */
    static Stream<Arguments> testInputs()
    {
        return IntStream.rangeClosed(0, 2)
                        .mapToObj(instanceDownCount -> new AbstractMap.SimpleEntry<>(instanceDownCount, testConsistencyLevels()))
                        .flatMap(pair -> pair.getValue().stream().map(cl -> Arguments.of(pair.getKey(), cl)));
    }

    static List<TestConsistencyLevel> testConsistencyLevels()
    {
        return Arrays.asList(TestConsistencyLevel.of(ONE, ALL),
                             TestConsistencyLevel.of(LOCAL_QUORUM, LOCAL_QUORUM),
                             TestConsistencyLevel.of(ONE, ONE));
    }
}
