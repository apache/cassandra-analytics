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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests to verify bulk writes when a single Cassandra node is leaving the cluster and the operation
 * fails
 */
class LeavingSingleFailureTest extends LeavingTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("singleDCTestInputs")
    void singleLeavingFailure(TestConsistencyLevel cl)
    {
        runLeavingTestScenario(cl);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        singleDCTestInputs().forEach(arguments -> {
            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, arguments.get());
            createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperSingleLeavingNodeFailure.transitionalStateEnd, singleDCTestInputs());

        // For tests that involve LEAVE failures, we validate that the leaving nodes are part of the cluster
        // check leave node are part of cluster when leave fails
        assertThat(areLeavingNodesPartOfCluster(cluster.get(1), leavingNodes)).isTrue();
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(5)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperSingleLeavingNodeFailure::install);
    }

    @Override
    protected int leavingNodesPerDc()
    {
        return 1;
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperSingleLeavingNodeFailure.transitionalStateStart;
    }

    /**
     * ByteBuddy Helper for a single leaving node failure scenario
     */
    @Shared
    public static class BBHelperSingleLeavingNodeFailure
    {
        static final CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static final CountDownLatch transitionalStateEnd = new CountDownLatch(1);

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
    }
}
