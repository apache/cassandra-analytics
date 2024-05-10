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

package org.apache.cassandra.analytics.replacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

abstract class HostReplacementTestBase extends ResiliencyTestBase
{
    Dataset<Row> df;
    Map<? extends IInstance, Set<String>> expectedInstanceData;
    List<IInstance> newNodes;
    List<String> removedNodeAddresses;

    @Override
    protected void afterClusterProvisioned()
    {
        assertThat(additionalNodesToStop()).isLessThan(cluster.size() - 1);

        IInstance seed = cluster.get(1);
        List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);

        // Remove the last node
        List<IInstance> nodesToRemove = Collections.singletonList(cluster.get(cluster.size()));
        removedNodeAddresses = nodesToRemove.stream()
                                            .map(n -> n.config()
                                                       .broadcastAddress()
                                                       .getAddress()
                                                       .getHostAddress())
                                            .collect(Collectors.toList());
        List<String> removedNodeTokens = ring.stream()
                                             .filter(i -> removedNodeAddresses.contains(i.getAddress()))
                                             .map(ClusterUtils.RingInstanceDetails::getToken)
                                             .collect(Collectors.toList());
        stopNodes(seed, nodesToRemove);

        List<IInstance> additionalRemovalNodes = new ArrayList<>();
        for (int i = 1; i <= additionalNodesToStop(); i++)
        {
            additionalRemovalNodes.add(cluster.get(cluster.size() - i));
        }
        newNodes = startReplacementNodes(nodeStart(), cluster, nodesToRemove);
        stopNodes(seed, additionalRemovalNodes);

        // Wait until replacement nodes are in JOINING state
        // NOTE: While many of these tests wait 2 minutes and pass, this particular transition
        // takes longer, so upping the timeout to 5 minutes

        // Verify state of replacement nodes
        for (IInstance newInstance : newNodes)
        {
            cluster.awaitRingState(newInstance, newInstance, "Joining");
            cluster.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

            String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
            Optional<ClusterUtils.RingInstanceDetails> replacementInstance = getMatchingInstanceFromRing(newInstance, newAddress);
            assertThat(replacementInstance).isPresent();
            // Verify that replacement node tokens match the removed nodes
            assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
        }
    }

    protected void completeTransitionsAndValidateWrites(CountDownLatch transitionalStateEnd,
                                                        Stream<Arguments> testInputs,
                                                        boolean expectFailure)
    {
        long count = transitionalStateEnd.getCount();
        for (int i = 0; i < count; i++)
        {
            transitionalStateEnd.countDown();
        }

        assertThat(newNodes).isNotNull();
        assertThat(removedNodeAddresses).isNotNull();

        // It is only in successful REPLACE operation that we validate that the node has reached NORMAL state
        if (!expectFailure)
        {
            cluster.awaitRingState(newNodes.get(0), newNodes.get(0), "Normal");

            // Validate if data was written to the new transitioning nodes only when bootstrap succeeded
            testInputs.forEach(arguments -> {
                TestConsistencyLevel cl = (TestConsistencyLevel) arguments.get()[0];

                QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
                validateNodeSpecificData(tableName, expectedInstanceData);
                validateData(tableName, cl.readCL, ROW_COUNT);
            });
        }
        else
        {
            // For replacement failure cases, we make a best-effort attempt to validate that
            // 1) replacement node is not NORMAL, and 2) removed node is DOWN
            // This is to work around the non-deterministic nature of gossip settling
            Optional<ClusterUtils.RingInstanceDetails> replacementNode =
            getMatchingInstanceFromRing(newNodes.get(0), newNodes.get(0).broadcastAddress().getAddress().getHostAddress());
            // Validate that the replacement node did not succeed in joining (if still visible in ring)
            replacementNode.ifPresent(ringInstanceDetails -> assertThat(ringInstanceDetails.getState()).isNotEqualTo("Normal"));

            Optional<ClusterUtils.RingInstanceDetails> removedNode =
            getMatchingInstanceFromRing(cluster.get(1), removedNodeAddresses.get(0));
            // Validate that the removed node is "Down" (if still visible in ring)
            removedNode.ifPresent(ringInstanceDetails -> assertThat(ringInstanceDetails.getStatus()).isEqualTo("Down"));

            testInputs.forEach(arguments -> {
                TestConsistencyLevel cl = (TestConsistencyLevel) arguments.get()[0];

                if (cl.readCL != ALL)
                {
                    QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
                    validateData(tableName, cl.readCL, ROW_COUNT);
                }
            });
        }
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        SparkSession spark = getOrCreateSparkSession();
        // Generate some artificial data for the test
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        // generate the expected data for the new nodes
        expectedInstanceData = getInstanceData(newNodes, true, ROW_COUNT);
    }

    /**
     * @return the number of additional nodes to stop for the test
     */
    protected int additionalNodesToStop()
    {
        return 0;
    }

    /**
     * @return a latch that will wait until the node starts
     */
    protected abstract CountDownLatch nodeStart();

    static Stream<Arguments> singleDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(ONE, ALL)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM))
        );
    }

    public static <I extends IInstance> I addInstanceLocal(IClusterExtension<I> cluster,
                                                           String dc,
                                                           String rack,
                                                           Consumer<IInstanceConfig> fn,
                                                           int remPort)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("storage_port", remPort);
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));
        fn.accept(config);
        return cluster.bootstrap(config);
    }

    private List<IInstance> startReplacementNodes(CountDownLatch nodeStart, IClusterExtension<?> cluster,
                                                  List<IInstance> nodesToRemove)
    {
        List<IInstance> newNodes = new ArrayList<>();
        // Launch replacements nodes with the config of the removed nodes
        for (IInstance removed : nodesToRemove)
        {
            // Add new instance for each removed instance as a replacement of its owned token
            IInstanceConfig removedConfig = removed.config();
            String remAddress = removedConfig.broadcastAddress().getAddress().getHostAddress();
            int remPort = removedConfig.getInt("storage_port");
            IInstance replacement =
            addInstanceLocal(cluster,
                             removedConfig.localDatacenter(),
                             removedConfig.localRack(),
                             c -> {
                                 c.set("auto_bootstrap", true);
                                 // explicitly DOES NOT set instances that failed startup as "shutdown"
                                 // so subsequent attempts to shut down the instance are honored
                                 c.set("dtest.api.startup.failure_as_shutdown", false);
                                 c.with(Feature.GOSSIP,
                                        Feature.JMX,
                                        Feature.NATIVE_PROTOCOL);
                             },
                             remPort);

            new Thread(() -> ClusterUtils.start(replacement, (properties) -> {
                replacement.config().set("storage_port", remPort);
                properties.with("cassandra.skip_schema_check", "true");
                properties.with("cassandra.schema_delay_ms", String.valueOf(TimeUnit.SECONDS.toMillis(10L)));
                properties.with("cassandra.broadcast_interval_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(30L)));
                properties.with("cassandra.ring_delay_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(10L)));
                // This property tells cassandra that this new instance is replacing the node with
                // address remAddress and port remPort
                properties.with("cassandra.replace_address_first_boot", remAddress + ":" + remPort);
            })).start();

            Uninterruptibles.awaitUninterruptibly(nodeStart, 2, TimeUnit.MINUTES);
            newNodes.add(replacement);
        }
        return newNodes;
    }

    private void stopNodes(IInstance seed, List<IInstance> nodesToRemove)
    {
        for (IInstance node : nodesToRemove)
        {
            cluster.stopUnchecked(node);
            // awaitRingState will assert that the node state is down. It retries multiple times until a timeout
            // is reached and fails if the expected state is not seen.
            cluster.awaitRingState(seed, node, "Down");
        }
    }


    protected Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IInstance seed, String ipAddress)
    {
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(i -> i.getAddress().equals(ipAddress))
                           .findFirst();
    }
}
