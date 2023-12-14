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

import org.junit.jupiter.api.TestInfo;

import com.datastax.driver.core.ConsistencyLevel;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

class LeavingMultiDCHalveClusterTest extends LeavingTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void allReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transitionalStateStart,
                               BBHelperHalveClusterMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void allReadOneWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDCFailure.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateStart,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transitionalStateStart,
                               BBHelperHalveClusterMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadLocalQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDCFailure.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateStart,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               true,
                               testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transitionalStateStart,
                               BBHelperHalveClusterMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadEachQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDCFailure.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateStart,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               true,
                               testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void quorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transitionalStateStart,
                               BBHelperHalveClusterMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void quorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDCFailure.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateStart,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true,
                               testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transitionalStateStart,
                               BBHelperHalveClusterMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false,
                               testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, buildCluster = false)
    void oneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperHalveClusterMultiDCFailure.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateStart,
                               BBHelperHalveClusterMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true,
                               testInfo.getDisplayName());
    }

    /**
     * ByteBuddy helper for halve cluster size with multi-DC
     */
    @Shared
    public static class BBHelperHalveClusterMultiDC
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(6);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 12 node cluster (6 per DC)
            // We intercept the shutdown of the removed nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 4, TimeUnit.MINUTES);
            orig.call();
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(6);
            transitionalStateEnd = new CountDownLatch(6);
        }
    }

    /**
     * ByteBuddy helper for halve cluster size with multi-DC failure scenario
     */
    @Shared
    public static class BBHelperHalveClusterMultiDCFailure
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(6);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 12 node cluster (6 per DC)
            // We intercept the shutdown of the removed nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterMultiDCFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateStart, 4, TimeUnit.MINUTES);
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
        }

        public static void reset()
        {
            transitionalStateStart = new CountDownLatch(6);
            transitionalStateEnd = new CountDownLatch(6);
        }
    }
}
