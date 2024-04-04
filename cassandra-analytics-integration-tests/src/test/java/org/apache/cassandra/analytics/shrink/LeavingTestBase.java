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

package org.apache.cassandra.analytics.shrink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

abstract class LeavingTestBase extends ResiliencyTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LeavingTestBase.class);
    List<? extends IInstance> leavingNodes;
    Dataset<Row> df;
    private Map<? extends IInstance, Set<String>> expectedInstanceData;

    protected void runLeavingTestScenario(TestConsistencyLevel cl)
    {
        QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
        bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                            .save();
        // validate data right after bulk writes
        validateData(table, cl.readCL, ROW_COUNT);
        validateNodeSpecificData(table, expectedInstanceData, false);
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        SparkSession spark = getOrCreateSparkSession();
        // Generate some artificial data for the test
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        // generate the expected data for the leaving nodes
        expectedInstanceData = generateExpectedInstanceData(cluster, leavingNodes, ROW_COUNT);
    }

    @Override
    protected void afterClusterProvisioned()
    {
        ClusterBuilderConfiguration configuration = testClusterConfiguration();
        IInstance seed = cluster.getFirstRunningInstance();
        leavingNodes = decommissionNodes(cluster, leavingNodesPerDc(), configuration.dcCount);

        // Wait until nodes have reached expected state
        TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart(), 4, TimeUnit.MINUTES);
        leavingNodes.forEach(instance -> cluster.awaitRingState(seed, instance, "Leaving"));
    }

    protected void completeTransitionsAndValidateWrites(CountDownLatch transitionalStateEnd, Stream<Arguments> testInputs)
    {
        for (int i = 0; i < leavingNodesPerDc(); i++)
        {
            transitionalStateEnd.countDown();
        }

        testInputs.forEach(arguments -> {
            TestConsistencyLevel cl = (TestConsistencyLevel) arguments.get()[0];

            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
            validateData(tableName, cl.readCL, ROW_COUNT);
            validateNodeSpecificData(tableName, expectedInstanceData, false);
        });
    }

    /**
     * @return a latch to wait before the cluster provisioning is complete
     */
    protected abstract CountDownLatch transitioningStateStart();

    /**
     * @return the number of nodes per datacenter that are expected to leave the cluster
     */
    protected abstract int leavingNodesPerDc();

    protected static Stream<Arguments> singleDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(ONE, ALL)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM))
        );
    }

    protected static Stream<Arguments> multiDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(ALL, ONE)),
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, LOCAL_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, EACH_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM)),
        Arguments.of(TestConsistencyLevel.of(ONE, ALL))
        );
    }

    protected List<IInstance> decommissionNodes(ICluster<? extends IInstance> cluster,
                                                int leavingNodesPerDC,
                                                int numDcs)
    {
        List<IInstance> leavingNodes = new ArrayList<>();
        for (int i = 0; i < leavingNodesPerDC * numDcs; i++)
        {
            IInstance node = cluster.get(cluster.size() - i);
            new Thread(() -> {
                NodeToolResult decommission = node.nodetoolResult("decommission");
                if (decommission.getRc() != 0 || decommission.getError() != null)
                {
                    LOGGER.error("Failed to decommission instance={}",
                                 node.config().num(), decommission.getError());
                }
                decommission.asserts().success();
            }).start();
            leavingNodes.add(node);
        }

        return leavingNodes;
    }

    protected boolean areLeavingNodesPartOfCluster(IInstance seed, List<? extends IInstance> leavingNodes)
    {
        Set<String> leavingAddresses = leavingNodes.stream()
                                                   .map(node -> node.broadcastAddress().getAddress().getHostAddress())
                                                   .collect(Collectors.toSet());
        ClusterUtils.ring(seed).forEach(i -> leavingAddresses.remove(i.getAddress()));
        return leavingAddresses.isEmpty();
    }
}
