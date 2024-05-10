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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.analytics.BulkWriteDownInstanceTest.isFailureExpected;
import static org.apache.cassandra.analytics.BulkWriteDownInstanceTest.testConsistencyLevels;
import static org.apache.cassandra.analytics.ResiliencyTestBase.uniqueTestTableFullName;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk writes in different scenarios when Sidecar instances are down. In this test
 * we have one Sidecar managing a single Cassandra instance
 */
class BulkWriteDownSidecarTest extends SharedClusterSparkIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkWriteDownSidecarTest.class);
    Set<Server> downSidecars = new HashSet<>();
    List<Server> sidecarServerList = new ArrayList<>();

    @ParameterizedTest(name = "{index} => instanceDownCount={0} {1}")
    @MethodSource("testInputs")
    void testHandlingOfDownedSidecarInstances(int instanceDownCount, TestConsistencyLevel cl) throws Exception
    {
        // progressively stop Sidecar instances as needed for the test
        stopSidecarInstancesForTest(instanceDownCount);

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

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        for (IInstance instance : cluster)
        {
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        instance.config().num());
            Server server = startSidecarWithInstances(Collections.singleton(instance));
            sidecarServerList.add(server);
        }

        assertThat(sidecarServerList.size()).as("Each Cassandra Instance will be managed by a single Sidecar instance")
                                            .isEqualTo(cluster.size());
        // assign the server to the first instance
        server = sidecarServerList.get(0);
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            ServiceConfiguration conf;
            if (sidecarServerList.isEmpty())
            {
                // As opposed to the base class, this binds the host to a specific interface (localhost)
                conf = ServiceConfigurationImpl.builder()
                                               .host("localhost")
                                               .port(0) // let the test find an available port
                                               .build();
            }
            else
            {
                // Use the same port number for all Sidecar instances that we bring up. We use the port
                // bound for the first instance, but we bind it to a different interface (localhost2, localhost3)
                conf = ServiceConfigurationImpl.builder()
                                               .host("localhost" + (sidecarServerList.size() + 1))
                                               .port(sidecarServerList.get(0).actualPort())
                                               .build();
            }
            builder.serviceConfiguration(conf);

            return builder;
        };
    }

    @Override
    protected void stopSidecar() throws InterruptedException
    {
        for (Server server : sidecarServerList)
        {
            if (downSidecars.add(server))
            {
                CountDownLatch closeLatch = new CountDownLatch(1);
                server.close().onSuccess(res -> closeLatch.countDown());
                if (!closeLatch.await(60, TimeUnit.SECONDS))
                {
                    logger.error("Close event timed out.");
                }
            }
        }
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

    void stopSidecarInstancesForTest(int instanceDownCount) throws Exception
    {
        assertThat(sidecarServerList).isNotEmpty();
        while (instanceDownCount > downSidecars.size())
        {
            for (Server server : sidecarServerList)
            {
                if (downSidecars.add(server))
                {
                    server.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
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
}
