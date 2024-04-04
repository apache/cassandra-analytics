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

package org.apache.cassandra.analytics.movement;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

abstract class NodeMovementTestBase extends ResiliencyTestBase
{
    public static final int SINGLE_DC_MOVING_NODE_IDX = 5;
    public static final int MULTI_DC_MOVING_NODE_IDX = 3;

    IInstance movingNode;
    Dataset<Row> df;
    Map<? extends IInstance, Set<String>> expectedInstanceData;

    protected void runMovingNodeTest(TestConsistencyLevel cl)
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
        // generate the expected data for the moving instance
        expectedInstanceData = generateExpectedInstanceData(cluster, Collections.singletonList(movingNode), ROW_COUNT);
    }

    @Override
    protected void afterClusterProvisioned()
    {
        ClusterBuilderConfiguration configuration = testClusterConfiguration();
        int movingNodeIndex = configuration.dcCount > 1 ? MULTI_DC_MOVING_NODE_IDX : SINGLE_DC_MOVING_NODE_IDX;
        movingNode = cluster.get(movingNodeIndex);

        IInstance seed = cluster.get(1);
        new Thread(() -> {
            long moveTarget = calculateMoveTargetToken(cluster, configuration.dcCount);
            movingNode.nodetoolResult("move", "--", Long.toString(moveTarget)).asserts().success();
        }).start();

        // Wait until nodes have reached expected state
        TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart(), 2, TimeUnit.MINUTES);
        cluster.awaitRingState(seed, movingNode, "Moving");
    }

    /**
     * @return a latch to wait before the cluster provisioning is complete
     */
    protected abstract CountDownLatch transitioningStateStart();

    protected void completeTransitionAndValidateWrites(CountDownLatch transitionalStateEnd,
                                                       Stream<Arguments> testInputs,
                                                       boolean expectFailure)
    {
        transitionalStateEnd.countDown();

        assertThat(movingNode).isNotNull();

        // It is only in successful MOVE operation that we validate that the node has reached NORMAL state
        if (!expectFailure)
        {
            cluster.awaitRingState(cluster.get(1), movingNode, "Normal");
        }

        testInputs.forEach(arguments -> {
            TestConsistencyLevel cl = (TestConsistencyLevel) arguments.get()[0];

            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
            validateData(tableName, cl.readCL, ROW_COUNT);
            validateNodeSpecificData(tableName, expectedInstanceData, false);
        });

        // For tests that involve MOVE failures, we make a best-effort attempt by checking if the node is either
        // still MOVING or has flipped back to NORMAL state with the initial token that it previously held
        if (expectFailure)
        {
            String initialToken = movingNode.config().getString("initial_token");
            Optional<ClusterUtils.RingInstanceDetails> movingInstance =
            ClusterUtils.ring(cluster.get(1))
                        .stream()
                        .filter(i -> i.getAddress().equals(movingNode.broadcastAddress().getAddress().getHostAddress()))
                        .findFirst();
            assertThat(movingInstance).isPresent();
            String state = movingInstance.get().getState();

            assertThat(state.equals("Moving") ||
                       (state.equals("Normal") && movingInstance.get().getToken().equals(initialToken))).isTrue();
        }
    }

    static long calculateMoveTargetToken(ICluster<? extends IInstance> cluster, int dcCount)
    {
        IInstance seed = cluster.get(1);
        // The target token to move the node to is calculated by adding an offset to the seed node token which
        // is half of the range between 2 tokens.
        // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
        // and 3 to calculate the offset
        int nextIndex = (dcCount > 1) ? 3 : 2;
        long t2 = Long.parseLong(seed.config().getString("initial_token"));
        long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
        return (t2 + ((t3 - t2) / 2));
    }
}
