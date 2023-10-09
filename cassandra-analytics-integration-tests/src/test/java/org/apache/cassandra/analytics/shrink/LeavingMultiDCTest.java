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

import com.google.common.util.concurrent.Uninterruptibles;

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

class LeavingMultiDCTest extends LeavingTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void allReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transitionalStateStart,
                               BBHelperLeavingNodesMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void allReadOneWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateStart,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               true, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transitionalStateStart,
                               BBHelperLeavingNodesMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false,
                               testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadLocalQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateStart,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               true, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transitionalStateStart,
                               BBHelperLeavingNodesMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void localQuorumReadEachQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateStart,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               true, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void quorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transitionalStateStart,
                               BBHelperLeavingNodesMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void quorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateStart,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transitionalStateStart,
                               BBHelperLeavingNodesMultiDC.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false, testInfo.getDisplayName());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, buildCluster = false)
    void oneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateStart,
                               BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true, testInfo.getDisplayName());
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDC
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(2);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDC.class))
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
     * ByteBuddy helper for multiple leaving nodes multi-DC failure scenario
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDCFailure
    {
        static CountDownLatch transitionalStateStart = new CountDownLatch(2);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDCFailure.class))
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
}
