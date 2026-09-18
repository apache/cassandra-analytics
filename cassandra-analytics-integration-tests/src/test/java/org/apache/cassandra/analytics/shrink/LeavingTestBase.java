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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestConsistencyLevel;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

abstract class LeavingTestBase extends ResiliencyTestBase
{
    private final List<FutureTask<NodeToolResult>> decommissions = new ArrayList<>();
    List<IInstance> leavingNodes;
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
        if (requiresConcurrentTopologyChanges())
        {
            startTopologyChange();
        }
    }

    @Override
    protected void afterSchemaInitialized()
    {
        if (!requiresConcurrentTopologyChanges())
        {
            startTopologyChange();
        }
    }

    private void startTopologyChange()
    {
        prepareTopologyChange();
        IInstance seed = cluster.getFirstRunningInstance();
        leavingNodes = new ArrayList<>();
        int count = leavingNodeCount();
        for (int i = 0; i < count; i++)
        {
            IInstance node = cluster.get(cluster.size() - i);
            decommissionNode(node);
            leavingNodes.add(node);
        }

        // Wait until nodes have reached expected state
        TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart(), 4, TimeUnit.MINUTES);
        leavingNodes.forEach(instance -> cluster.awaitRingState(seed, instance, "Leaving"));
    }

    protected void completeTransitionsAndValidateWrites(CountDownLatch transitionalStateEnd, Stream<Arguments> testInputs)
    {
        completeTransitionsAndValidateWrites(transitionalStateEnd, testInputs, false);
    }

    protected void completeTransitionsAndValidateWrites(CountDownLatch transitionalStateEnd,
                                                        Stream<Arguments> testInputs,
                                                        boolean failureExpected)
    {
        while (transitionalStateEnd.getCount() > 0)
        {
            transitionalStateEnd.countDown();
        }

        decommissions.forEach(task -> {
            NodeToolResult result = awaitTopologyChange(task, false);
            if (failureExpected)
            {
                result.asserts().failure().errorContains("Simulated leave failure");
            }
            else
            {
                result.asserts().success();
            }
        });

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

    protected int leavingNodesPerDc()
    {
        return 1;
    }

    protected int leavingNodeCount()
    {
        return requiresConcurrentTopologyChanges() ? leavingNodesPerDc() * testClusterConfiguration().dcCount : 1;
    }

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

    private void decommissionNode(IInstance node)
    {
        FutureTask<NodeToolResult> decommission = new FutureTask<>(() -> node.nodetoolResult("decommission"));
        decommissions.add(decommission);
        new Thread(decommission, "decommission-node").start();
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
