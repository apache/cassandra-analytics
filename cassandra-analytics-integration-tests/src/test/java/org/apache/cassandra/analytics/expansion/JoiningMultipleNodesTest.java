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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Integration tests to validate bulk writes during multiple Cassandra instances joining the ring
 */
class JoiningMultipleNodesTest extends JoiningSingleNodeTest
{
    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperMultipleJoiningNodes.transitioningStateEnd,
                                             singleDCTestInputs(),
                                             false);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .newNodesPerDc(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperMultipleJoiningNodes::install);
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperMultipleJoiningNodes.transitioningStateStart;
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    public static class BBHelperMultipleJoiningNodes
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(2);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with a 2 joining nodes
            // We intercept the joining of nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultipleJoiningNodes.class))
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
