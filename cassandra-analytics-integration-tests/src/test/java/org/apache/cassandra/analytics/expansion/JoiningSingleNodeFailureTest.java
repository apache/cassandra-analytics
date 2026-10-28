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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.analytics.TopologyChangeBBUtils;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;


/**
 * Integration tests to validate bulk writes during a Cassandra instance join operation, where the
 * join operation is expected to fail
 */
class JoiningSingleNodeFailureTest extends JoiningSingleNodeTest
{
    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperSingleJoiningNodeFailure.transitioningStateEnd,
                                             singleDCTestInputs(), true);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .instanceInitializer(BBHelperSingleJoiningNodeFailure::install);
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperSingleJoiningNodeFailure.transitioningStateStart;
    }

    /**
     * ByteBuddy helper for a single joining node failure case
     */
    public static class BBHelperSingleJoiningNodeFailure
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with 1 joining node
            // We intercept the bootstrap of the leaving node (4) to validate token ranges
            if (nodeNumber == 4)
            {
                TopologyChangeBBUtils.installBootstrap(cl, BBHelperSingleJoiningNodeFailure.class);
            }
        }

        public static boolean bootstrap(@SuperCall Callable<Boolean> orig) throws Exception
        {
            orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitioningStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
        }
    }
}
