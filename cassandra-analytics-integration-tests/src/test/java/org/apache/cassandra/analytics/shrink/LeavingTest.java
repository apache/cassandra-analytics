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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestTokenSupplier;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

class LeavingTest extends LeavingTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void singleLeavingNodeOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleLeavingNode.reset();
        runLeavingTestScenario(cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNode::install,
                               BBHelperSingleLeavingNode.transitionalStateStart,
                               BBHelperSingleLeavingNode.transitionalStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void singleLeavingNodeOneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleLeavingNodeFailure.reset();
        runLeavingTestScenario(cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNodeFailure::install,
                               BBHelperSingleLeavingNodeFailure.transitionalStateStart,
                               BBHelperSingleLeavingNodeFailure.transitionalStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void singleLeavingNodeQuorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleLeavingNode.reset();
        runLeavingTestScenario(cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNode::install,
                               BBHelperSingleLeavingNode.transitionalStateStart,
                               BBHelperSingleLeavingNode.transitionalStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void singleLeavingNodeQuorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleLeavingNodeFailure.reset();
        runLeavingTestScenario(cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNodeFailure::install,
                               BBHelperSingleLeavingNodeFailure.transitionalStateStart,
                               BBHelperSingleLeavingNodeFailure.transitionalStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void multipleLeavingNodesOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMultipleLeavingNodes.reset();
        runLeavingTestScenario(cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodes::install,
                               BBHelperMultipleLeavingNodes.transitionalStateStart,
                               BBHelperMultipleLeavingNodes.transitionalStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void multipleLeavingNodesOneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMultipleLeavingNodesFailure.reset();
        runLeavingTestScenario(cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodesFailure::install,
                               BBHelperMultipleLeavingNodesFailure.transitionalStateStart,
                               BBHelperMultipleLeavingNodesFailure.transitionalStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void multipleLeavingNodesQuorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMultipleLeavingNodes.reset();
        runLeavingTestScenario(cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodes::install,
                               BBHelperMultipleLeavingNodes.transitionalStateStart,
                               BBHelperMultipleLeavingNodes.transitionalStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void multipleLeavingNodesQuorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMultipleLeavingNodesFailure.reset();
        runLeavingTestScenario(cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodesFailure::install,
                               BBHelperMultipleLeavingNodesFailure.transitionalStateStart,
                               BBHelperMultipleLeavingNodesFailure.transitionalStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, network = true, buildCluster = false)
    void halveClusterSizeOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterSize.reset();
        runLeavingTestScenario(cassandraTestContext,
                               3,
                               BBHelperHalveClusterSize::install,
                               BBHelperHalveClusterSize.transitionalStateStart,
                               BBHelperHalveClusterSize.transitionalStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, network = true, buildCluster = false)
    void halveClusterSizeQuorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterSize.reset();
        runLeavingTestScenario(cassandraTestContext,
                               3,
                               BBHelperHalveClusterSize::install,
                               BBHelperHalveClusterSize.transitionalStateStart,
                               BBHelperHalveClusterSize.transitionalStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    // CHECKSTYLE IGNORE: Method with many parameters
    void runLeavingTestScenario(ConfigurableCassandraTestContext cassandraTestContext,
                                int leavingNodesPerDC,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transitionalStateStart,
                                CountDownLatch transitionalStateEnd,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure)
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
        runLeavingTestScenario(leavingNodesPerDC,
                               transitionalStateStart,
                               transitionalStateEnd,
                               cluster,
                               readCL,
                               writeCL,
                               isFailure);
    }

    /**
     * ByteBuddy Helper for a single leaving node
     */
    @Shared
    public static class BBHelperSingleLeavingNode
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (5) to validate token ranges
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperSingleLeavingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            orig.call();
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(1);
            transitionalStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy Helper for a single leaving node failure scenario
     */
    @Shared
    public static class BBHelperSingleLeavingNodeFailure
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (5) to validate token ranges
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperSingleLeavingNodeFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulate leave failure");
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(1);
            transitionalStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes
     */
    @Shared
    public static class BBHelperMultipleLeavingNodes
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(2);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperMultipleLeavingNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            orig.call();
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(2);
            transitionalStateEnd = new CountDownLatch(2);
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes failure scenario
     */
    @Shared
    public static class BBHelperMultipleLeavingNodesFailure
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(2);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperMultipleLeavingNodesFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulate leave failure");
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(2);
            transitionalStateEnd = new CountDownLatch(2);
        }
    }

    /**
     * ByteBuddy helper for shrinking cluster by half its size
     */
    @Shared
    public static class BBHelperHalveClusterSize
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(3);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(3);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 6 node cluster
            // We intercept the shutdown of the removed nodes (4-6) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            orig.call();
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(3);
            transitionalStateEnd = new CountDownLatch(3);
        }
    }
}
