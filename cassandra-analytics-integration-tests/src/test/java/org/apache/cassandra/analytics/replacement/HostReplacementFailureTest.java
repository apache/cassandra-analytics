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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Integration tests that verify bulk writes during a host replacement operation in the Cassandra cluster where
 * the operation is expected to fail
 */
class HostReplacementFailureTest extends HostReplacementTest
{
    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperReplacementsNodeFailure.transitioningStateEnd, singleDCTestInputs(), true);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .instanceInitializer(BBHelperReplacementsNodeFailure::install);
    }

    @Override
    protected CountDownLatch nodeStart()
    {
        return BBHelperReplacementsNodeFailure.nodeStart;
    }

    /**
     * ByteBuddy helper for a single node replacement
     */
    public static class BBHelperReplacementsNodeFailure
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static final CountDownLatch nodeStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a replacement node
            // We intercept the bootstrap of the replacement (6th) node to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsNodeFailure.class))
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
            transitioningStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitioningStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
        }
    }
}
