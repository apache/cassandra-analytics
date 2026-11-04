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
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;


class JoiningMultiDCSingleReplicatedFailureTest extends JoiningMultiDCSingleReplicatedTest
{
    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperMultiDCFailure.transitioningStateEnd,
                                             multiDCTestInputs(),
                                             true);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(5)
                              .newNodesPerDc(1)
                              .dcCount(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperMultiDCFailure::install);
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperMultiDCFailure.transitioningStateStart;
    }

    /**
     * ByteBuddy helper for a joining node failure
     */
    public static class BBHelperMultiDCFailure
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Intercept the joining node in datacenter1.
            if (nodeNumber == 11)
            {
                TopologyChangeBBUtils.installBootstrap(cl, BBHelperMultiDCFailure.class);
            }
        }

        public static boolean bootstrap(@SuperCall Callable<Boolean> orig) throws Exception
        {
            orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitioningStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateEnd, 2, TimeUnit.MINUTES);
            throw new IllegalStateException("Unable to contact any seeds: ");
        }
    }
}
