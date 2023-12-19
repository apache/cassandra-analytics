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

package org.apache.cassandra.analytics.movement;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
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
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Integration tests to verify bulk writes when a Cassandra's node range is moved in a multi-DC cluster, and the
 * move operation is expected to fail
 */
class NodeMovementMultiDCFailureTest extends NodeMovementMultiDCTest
{

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperMultiDCMovingNodeFailure.transitioningStateStart;
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionAndValidateWrites(BBHelperMultiDCMovingNodeFailure.transitioningStateEnd, multiDCTestInputs(), true);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .instanceInitializer(BBHelperMultiDCMovingNodeFailure::install);
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMultiDCMovingNodeFailure
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MULTI_DC_MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMultiDCMovingNodeFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transitioningStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitioningStateEnd, 2, TimeUnit.MINUTES);

            throw new IOException("Simulated node movement failure"); // Throws exception to nodetool
        }
    }
}
