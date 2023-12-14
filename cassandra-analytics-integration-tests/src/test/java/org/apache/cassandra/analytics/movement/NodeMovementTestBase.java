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
import java.util.function.BiConsumer;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestTokenSupplier;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeMovementTestBase extends ResiliencyTestBase
{
    public static final int SINGLE_DC_MOVING_NODE_IDX = 5;
    public static final int MULTI_DC_MOVING_NODE_IDX = 3;

    // CHECKSTYLE IGNORE: Constructor with many parameters
    void runMovingNodeTest(ConfigurableCassandraTestContext cassandraTestContext,
                           BiConsumer<ClassLoader, Integer> instanceInitializer,
                           CountDownLatch transitioningStateStart,
                           CountDownLatch transitioningStateEnd,
                           boolean isFailure,
                           ConsistencyLevel readCL,
                           ConsistencyLevel writeCL, String testName) throws Exception

    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);
        UpgradeableCluster cluster;
        QualifiedName schema;
        int movingNodeIndex;
        if (annotation.numDcs() > 1)
        {
            movingNodeIndex = MULTI_DC_MOVING_NODE_IDX;
            cluster = getMultiDCCluster(instanceInitializer, cassandraTestContext);
        }
        else
        {
            movingNodeIndex = SINGLE_DC_MOVING_NODE_IDX;
            cluster = cassandraTestContext.configureAndStartCluster(builder -> {
                builder.withInstanceInitializer(instanceInitializer);
                builder.withTokenSupplier(tokenSupplier);
            });
        }

        schema = createAndWaitForKeyspaceAndTable();

        long moveTarget = getMoveTargetToken(cluster);
        IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);
        String initialToken = movingNode.config().getString("initial_token");
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData;
        bulkWriteData(writeCL, schema);
        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            new Thread(() -> movingNode.nodetoolResult("move", "--", Long.toString(moveTarget))
                                       .asserts()
                                       .success()).start();

            // Wait until nodes have reached expected state
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart, 2, TimeUnit.MINUTES);
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");
            bulkWriteData(writeCL, schema);
            expectedInstanceData = generateExpectedInstanceData(cluster, Collections.singletonList(movingNode));
        }
        finally
        {
            transitioningStateEnd.countDown();
        }

        // It is only in successful MOVE operation that we validate that the node has reached NORMAL state
        if (!isFailure)
        {
            ClusterUtils.awaitRingState(cluster.get(1), movingNode, "Normal");
        }
        validateData(schema.table(), readCL);
        validateNodeSpecificData(schema, expectedInstanceData, false);

        // For tests that involve MOVE failures, we make a best-effort attempt by checking if the node is either
        // still MOVING or has flipped back to NORMAL state with the initial token that it previously held
        if (isFailure)
        {
            Optional<ClusterUtils.RingInstanceDetails> movingInstance =
            ClusterUtils.ring(cluster.get(1))
                        .stream()
                        .filter(i -> i.getAddress().equals(movingNode.broadcastAddress().getAddress().getHostAddress()))
                        .findFirst();
            assertThat(movingInstance.isPresent()).isTrue();
            String state = movingInstance.get().getState();

            assertThat(state.equals("Moving") ||
                       (state.equals("Normal") && movingInstance.get().getToken().equals(initialToken))).isTrue();
        }
    }

    protected long getMoveTargetToken(UpgradeableCluster cluster)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        IUpgradeableInstance seed = cluster.get(1);
        // The target token to move the node to is calculated by adding an offset to the seed node token which
        // is half of the range between 2 tokens.
        // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
        // and 3 to calculate the offset
        int nextIndex = (annotation.numDcs() > 1) ? 3 : 2;
        long t2 = Long.parseLong(seed.config().getString("initial_token"));
        long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
        return (t2 + ((t3 - t2) / 2));
    }
}
