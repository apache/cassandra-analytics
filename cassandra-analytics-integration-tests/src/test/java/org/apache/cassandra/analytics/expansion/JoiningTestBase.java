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

package org.apache.cassandra.analytics.expansion;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

public class JoiningTestBase extends ResiliencyTestBase
{

    void runJoiningTestScenario(CountDownLatch transitioningStateStart,
                                CountDownLatch transitioningStateEnd,
                                UpgradeableCluster cluster,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure,
                                String testName) throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        QualifiedName table;
        List<IUpgradeableInstance> newInstances;
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData;
        try
        {
            newInstances = addNewInstances(cluster, annotation.newNodesPerDc(), annotation.numDcs());

            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart, 2, TimeUnit.MINUTES);

            newInstances.forEach(instance -> ClusterUtils.awaitRingState(instance, instance, "Joining"));
            table = createAndWaitForKeyspaceAndTable();
            bulkWriteData(writeCL, table);

            expectedInstanceData = generateExpectedInstanceData(cluster, newInstances);
        }
        finally
        {
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transitioningStateEnd.countDown();
            }
        }

        assertThat(table).isNotNull();

        validateData(table.table(), readCL);
        validateNodeSpecificData(table, expectedInstanceData);

        // For tests that involve JOIN failures, we make a best-effort attempt to check if the node join has failed
        // by checking if the node has either left the ring or is still in JOINING state, but not NORMAL
        if (isFailure)
        {
            for (IUpgradeableInstance joiningNode : newInstances)
            {
                Optional<ClusterUtils.RingInstanceDetails> joiningNodeDetails =
                getMatchingInstanceFromRing(cluster.get(1), joiningNode);
                joiningNodeDetails.ifPresent(ringInstanceDetails -> assertThat(ringInstanceDetails.getState())
                                                                    .isNotEqualTo("Normal"));
            }
        }
    }

    private List<IUpgradeableInstance> addNewInstances(UpgradeableCluster cluster, int newNodesPerDc, int numDcs)
    {
        List<IUpgradeableInstance> newInstances = new ArrayList<>();
        // Go over new nodes and add them once for each DC
        for (int i = 0; i < newNodesPerDc; i++)
        {
            int dcNodeIdx = 1; // Use node 2's DC
            for (int dc = 1; dc <= numDcs; dc++)
            {
                IUpgradeableInstance dcNode = cluster.get(dcNodeIdx++);
                IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                            dcNode.config().localDatacenter(),
                                                                            dcNode.config().localRack(),
                                                                            inst -> {
                                                                                inst.set("auto_bootstrap", true);
                                                                                inst.with(Feature.GOSSIP,
                                                                                          Feature.JMX,
                                                                                          Feature.NATIVE_PROTOCOL);
                                                                            });
                new Thread(() -> newInstance.startup(cluster)).start();
                newInstances.add(newInstance);
            }
        }
        return newInstances;
    }

    void runJoiningTestScenario(ConfigurableCassandraTestContext cassandraTestContext,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transitioningStateStart,
                                CountDownLatch transitioningStateEnd,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure,
                                String testName)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(instanceInitializer);
            builder.withTokenSupplier(tokenSupplier);
        });

        runJoiningTestScenario(transitioningStateStart,
                               transitioningStateEnd,
                               cluster,
                               readCL,
                               writeCL,
                               isFailure,
                               testName);
    }

    private Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IUpgradeableInstance seed,
                                                                                   IUpgradeableInstance instance)
    {
        String ipAddress = instance.broadcastAddress().getAddress().getHostAddress();
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(i -> i.getAddress().equals(ipAddress))
                           .findFirst();
    }
}
