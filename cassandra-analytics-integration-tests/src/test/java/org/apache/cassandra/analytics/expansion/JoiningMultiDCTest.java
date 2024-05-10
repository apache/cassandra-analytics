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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestConsistencyLevel;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

class JoiningMultiDCTest extends JoiningTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("multiDCTestInputs")
    void testJoiningNodeInMultiDCTest(TestConsistencyLevel cl)
    {
        runJoiningTestScenario(cl);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        multiDCTestInputs().forEach(arguments -> {
            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, arguments.get());
            createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperMultiDC.transitioningStateEnd,
                                             multiDCTestInputs(),
                                             false);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .newNodesPerDc(3)
                              .dcCount(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperMultiDC::install);
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperMultiDC.transitioningStateStart;
    }

    static Stream<Arguments> multiDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(ALL, ONE)),
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, LOCAL_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(EACH_QUORUM, LOCAL_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM)),
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, EACH_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(ONE, ALL))
        );
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    public static class BBHelperMultiDC
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(6);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves doubling the size of a 6 node cluster (3 per DC)
            // We intercept the bootstrap of nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultiDC.class))
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
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitioningStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateEnd, 2, TimeUnit.MINUTES);
            return result;
        }
    }
}
