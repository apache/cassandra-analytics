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

import java.util.Collection;
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
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class HostReplacementMultiDCTest extends HostReplacementTestBase
{

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementMultiDCTest(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperNodeReplacementMultiDC.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperNodeReplacementMultiDC::install,
                           BBHelperNodeReplacementMultiDC.transitionalStateStart,
                           BBHelperNodeReplacementMultiDC.transitionalStateEnd,
                           BBHelperNodeReplacementMultiDC.nodeStart,
                           false,
                           ConsistencyLevel.LOCAL_QUORUM,
                           ConsistencyLevel.LOCAL_QUORUM,
                           testInfo.getDisplayName());

    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementMultiDCEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperNodeReplacementMultiDC.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperNodeReplacementMultiDC::install,
                           BBHelperNodeReplacementMultiDC.transitionalStateStart,
                           BBHelperNodeReplacementMultiDC.transitionalStateEnd,
                           BBHelperNodeReplacementMultiDC.nodeStart,
                           false,
                           ConsistencyLevel.LOCAL_QUORUM,
                           ConsistencyLevel.EACH_QUORUM,
                           testInfo.getDisplayName());

    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementMultiDCQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperNodeReplacementMultiDC.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperNodeReplacementMultiDC::install,
                           BBHelperNodeReplacementMultiDC.transitionalStateStart,
                           BBHelperNodeReplacementMultiDC.transitionalStateEnd,
                           BBHelperNodeReplacementMultiDC.nodeStart,
                           false,
                           ConsistencyLevel.QUORUM,
                           ConsistencyLevel.QUORUM,
                           testInfo.getDisplayName());

    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementMultiDCAllWriteOneRead(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperNodeReplacementMultiDC.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperNodeReplacementMultiDC::install,
                           BBHelperNodeReplacementMultiDC.transitionalStateStart,
                           BBHelperNodeReplacementMultiDC.transitionalStateEnd,
                           BBHelperNodeReplacementMultiDC.nodeStart,
                           false,
                           ConsistencyLevel.ONE,
                           ConsistencyLevel.ALL,
                           testInfo.getDisplayName());

    }

    /**
     * Validates successful write operation when host replacement fails. Also validates that the
     * node intended to be replaced is 'Down' and the replacement node is not 'Normal'.
     */
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementFailureMultiDC(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperReplacementFailureMultiDC.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperReplacementFailureMultiDC::install,
                           BBHelperReplacementFailureMultiDC.transitionalStateStart,
                           BBHelperReplacementFailureMultiDC.transitionalStateEnd,
                           BBHelperReplacementFailureMultiDC.nodeStart,
                           true,
                           ConsistencyLevel.LOCAL_QUORUM,
                           ConsistencyLevel.LOCAL_QUORUM,
                           testInfo.getDisplayName());

    }

    /**
     * Validate failed write operation when host replacement fails resulting in insufficient nodes. This is simulated by
     * bringing down a node in addition to the replacement failure resulting in too few replicas to satisfy the
     * RF requirements.
     */

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void nodeReplacementFailureMultiDCInsufficientNodes(ConfigurableCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        BBHelperNodeReplacementMultiDCInsufficientReplicas.reset();

        runReplacementTest(cassandraTestContext,
                           BBHelperNodeReplacementMultiDCInsufficientReplicas::install,
                           BBHelperNodeReplacementMultiDCInsufficientReplicas.transitionalStateStart,
                           BBHelperNodeReplacementMultiDCInsufficientReplicas.transitionalStateEnd,
                           BBHelperNodeReplacementMultiDCInsufficientReplicas.nodeStart,
                           1,
                           true,
                           true,
                           ConsistencyLevel.EACH_QUORUM,
                           ConsistencyLevel.EACH_QUORUM,
                           testInfo.getDisplayName());

    }

    /**
     * ByteBuddy helper for a multi DC node replacement that succeeds
     */
    @Shared
    public static class BBHelperNodeReplacementMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 6 node cluster (3 per DC) with a replacement node
            // We intercept the bootstrap of the replacement (7th) node to validate token ranges
            if (nodeNumber == 7)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperNodeReplacementMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transitionalStateStart = new CountDownLatch(1);
            transitionalStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy helper for multi DC node replacement failure resulting in insufficient nodes
     */
    @Shared
    public static class BBHelperNodeReplacementMultiDCInsufficientReplicas
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 6 node cluster (3 per DC) with a replacement node
            // We intercept the bootstrap of the replacement (7th) node to validate token ranges
            if (nodeNumber == 7)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(HostReplacementMultiDCTest.BBHelperNodeReplacementMultiDCInsufficientReplicas.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }


        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitionalStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
            // return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transitionalStateStart = new CountDownLatch(1);
            transitionalStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy helper for multi DC node replacement failure
     */
    @Shared
    public static class BBHelperReplacementFailureMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 per DC) with a replacement node
            // We intercept the bootstrap of the replacement (11th) node to validate token ranges
            if (nodeNumber == 11)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementFailureMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitionalStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
            // return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transitionalStateStart = new CountDownLatch(1);
            transitionalStateEnd = new CountDownLatch(1);
        }
    }
}
