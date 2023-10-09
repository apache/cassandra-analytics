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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.SparkJobFailedException;
import org.apache.cassandra.analytics.TestTokenSupplier;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HostReplacementTestBase extends ResiliencyTestBase
{

    // CHECKSTYLE IGNORE: Method with many parameters
    void runReplacementTest(ConfigurableCassandraTestContext cassandraTestContext,
                            BiConsumer<ClassLoader, Integer> instanceInitializer,
                            CountDownLatch transitioningStateStart,
                            CountDownLatch transitioningStateEnd,
                            CountDownLatch nodeStart,
                            boolean isFailure,
                            ConsistencyLevel readCL,
                            ConsistencyLevel writeCL,
                            String testName) throws Exception
    {
        runReplacementTest(cassandraTestContext,
                           instanceInitializer,
                           transitioningStateStart,
                           transitioningStateEnd,
                           nodeStart,
                           0,
                           isFailure,
                           false,
                           readCL,
                           writeCL,
                           testName);
    }

    // CHECKSTYLE IGNORE: Method with many parameters
    void runReplacementTest(ConfigurableCassandraTestContext cassandraTestContext,
                            BiConsumer<ClassLoader, Integer> instanceInitializer,
                            CountDownLatch transitioningStateStart,
                            CountDownLatch transitioningStateEnd,
                            CountDownLatch nodeStart,
                            int additionalNodesToStop,
                            boolean isFailure,
                            boolean shouldWriteFail,
                            ConsistencyLevel readCL,
                            ConsistencyLevel writeCL,
                            String testName) throws Exception
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

        QualifiedName schema = createAndWaitForKeyspaceAndTable();

        assertThat(additionalNodesToStop).isLessThan(cluster.size() - 1);

        List<IUpgradeableInstance> nodesToRemove = Collections.singletonList(cluster.get(cluster.size()));
        List<IUpgradeableInstance> newNodes;
        List<String> removedNodeAddresses = nodesToRemove.stream()
                                                         .map(n ->
                                                              n.config()
                                                               .broadcastAddress()
                                                               .getAddress()
                                                               .getHostAddress())
                                                         .collect(Collectors.toList());
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<String> removedNodeTokens = ring.stream()
                                                 .filter(i -> removedNodeAddresses.contains(i.getAddress()))
                                                 .map(ClusterUtils.RingInstanceDetails::getToken)
                                                 .collect(Collectors.toList());
            stopNodes(seed, nodesToRemove);

            List<IUpgradeableInstance> additionalRemovalNodes = new ArrayList<>();
            for (int i = 1; i <= additionalNodesToStop; i++)
            {
                additionalRemovalNodes.add(cluster.get(cluster.size() - i));
            }
            newNodes = startReplacementNodes(nodeStart, cluster, nodesToRemove);
            stopNodes(seed, additionalRemovalNodes);

            // Wait until replacement nodes are in JOINING state
            // NOTE: While many of these tests wait 2 minutes and pass, this particular transition
            // takes longer, so upping the timeout to 5 minutes
//            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart, 10, TimeUnit.MINUTES);

            // Verify state of replacement nodes
            for (IUpgradeableInstance newInstance : newNodes)
            {
                ClusterUtils.awaitRingState(newInstance, newInstance, "Joining");
                ClusterUtils.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
                Optional<ClusterUtils.RingInstanceDetails> replacementInstance = getMatchingInstanceFromRing(newInstance, newAddress);
                assertThat(replacementInstance).isPresent();
                // Verify that replacement node tokens match the removed nodes
                assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
            }

            // It is only in the event we have insufficient nodes, we expect write job to fail
            if (shouldWriteFail)
            {
                SparkJobFailedException e = assertThrows(SparkJobFailedException.class, () -> bulkWriteData(writeCL, schema));
                assertThat(e.getStdErr()).containsPattern("Failed to load (\\d+) ranges with EACH_QUORUM for " +
                                                          "job ([a-zA-Z0-9-]+) in phase Environment Validation.");
                return;
            }
            else
            {
                bulkWriteData(writeCL, schema);
            }

            expectedInstanceData = getInstanceData(newNodes, true);
        }
        finally
        {
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transitioningStateEnd.countDown();
            }
        }

        // It is only in successful REPLACE operation that we validate that the node has reached NORMAL state
        if (!isFailure)
        {
            ClusterUtils.awaitRingState(newNodes.get(0), newNodes.get(0), "Normal");

            // Validate if data was written to the new transitioning nodes only when bootstrap succeeded
            validateNodeSpecificData(schema, expectedInstanceData);
        }
        else
        {
            // For replacement failure cases, we make a best-effort attempt to validate that
            // 1) replacement node is not NORMAL, and 2) removed node is DOWN
            // This is to work around the non-deterministic nature of gossip settling
            Optional<ClusterUtils.RingInstanceDetails> replacementNode =
            getMatchingInstanceFromRing(newNodes.get(0), newNodes.get(0).broadcastAddress().getAddress().getHostAddress());
            // Validate that the replacement node did not succeed in joining (if still visible in ring)
            if (replacementNode.isPresent())
            {
                assertThat(replacementNode.get().getState()).isNotEqualTo("Normal");
            }

            Optional<ClusterUtils.RingInstanceDetails> removedNode =
            getMatchingInstanceFromRing(cluster.get(1), removedNodeAddresses.get(0));
            // Validate that the removed node is "Down" (if still visible in ring)
            if (removedNode.isPresent())
            {
                assertThat(removedNode.get().getStatus()).isEqualTo("Down");
            }

            if (readCL == ConsistencyLevel.ALL)
            {
                // No more validations to be performed if CL is ALL and replacement fails
                return;
            }
        }

        validateData(schema.table(), readCL);
    }

    public static <I extends IInstance> I addInstanceLocal(AbstractCluster<I> cluster,
                                                           String dc,
                                                           String rack,
                                                           Consumer<IInstanceConfig> fn,
                                                           int remPort)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");
        InstanceConfig config = cluster.newInstanceConfig();
        config.set("storage_port", remPort);
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));
        fn.accept(config);
        return cluster.bootstrap(config);
    }

    private List<IUpgradeableInstance> startReplacementNodes(CountDownLatch nodeStart,
                                                             UpgradeableCluster cluster,
                                                             List<IUpgradeableInstance> nodesToRemove)
    {
        List<IUpgradeableInstance> newNodes = new ArrayList<>();
        // Launch replacements nodes with the config of the removed nodes
        for (IUpgradeableInstance removed : nodesToRemove)
        {
            // Add new instance for each removed instance as a replacement of its owned token
            IInstanceConfig removedConfig = removed.config();
            String remAddress = removedConfig.broadcastAddress().getAddress().getHostAddress();
            int remPort = removedConfig.getInt("storage_port");
            IUpgradeableInstance replacement =
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
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS,
                               TimeUnit.SECONDS.toMillis(10L));
                properties.with("cassandra.broadcast_interval_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(30L)));
                properties.with("cassandra.ring_delay_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(10L)));
                // This property tells cassandra that this new instance is replacing the node with
                // address remAddress and port remPort
                int localPort = replacement.config().getInt("storage_port");
                properties.with("cassandra.replace_address_first_boot", remAddress + ":" + remPort);
            })).start();

            Uninterruptibles.awaitUninterruptibly(nodeStart, 2, TimeUnit.MINUTES);
            newNodes.add(replacement);
        }
        return newNodes;
    }

    private void stopNodes(IUpgradeableInstance seed, List<IUpgradeableInstance> nodesToRemove)
    {
        for (IUpgradeableInstance node : nodesToRemove)
        {
            ClusterUtils.stopUnchecked(node);
            String remAddress = node.config().broadcastAddress().getAddress().getHostAddress();

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<ClusterUtils.RingInstanceDetails> match = ring.stream()
                                                               .filter((d) -> d.getAddress().equals(remAddress))
                                                               .collect(Collectors.toList());
            assertThat(match.stream().anyMatch(r -> r.getStatus().equals("Down"))).isTrue();
        }
    }


    private Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IUpgradeableInstance seed,
                                                                                   String ipAddress)
    {
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(i -> i.getAddress().equals(ipAddress))
                           .findFirst();
    }

    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc,
                                                      String rack,
                                                      Consumer<IInstanceConfig> fn, int remPort)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");
        InstanceConfig config = cluster.newInstanceConfig();
        config.set("storage_port", remPort);
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));
        fn.accept(config);
        return cluster.bootstrap(config);
    }
}
