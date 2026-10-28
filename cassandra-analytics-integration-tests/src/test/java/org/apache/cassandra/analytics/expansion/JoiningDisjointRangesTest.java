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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Bulk writes during concurrent TCM joins with Sidecar's CASSSIDECAR-277 token placement.
 */
class JoiningDisjointRangesTest extends JoiningMultiDCTest
{
    @Override
    protected void beforeClusterProvisioning()
    {
        super.beforeClusterProvisioning();
        assumeTrue(usesTcm(), "Disjoint range locks require TCM");
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(5)
                              .newNodesPerDc(1)
                              .dcCount(2)
                              .tokenSupplier(disjointMultiDcTokens())
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelper::install);
    }

    @Override
    protected int joiningDatacenters()
    {
        return 2;
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelper.transitioningStateStart;
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        assertThat(newInstances).extracting(instance -> instance.config().localDatacenter())
                                .containsExactly("datacenter1", "datacenter2");
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelper.transitioningStateEnd, multiDCTestInputs(), false);
    }

    public static class BBHelper
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(2);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader loader, Integer nodeNumber)
        {
            if (nodeNumber == 11 || nodeNumber == 12)
            {
                TopologyChangeBBUtils.installBootstrap(loader, BBHelper.class);
            }
        }

        public static boolean bootstrap(@SuperCall Callable<Boolean> original) throws Exception
        {
            boolean result = original.call();
            transitioningStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateEnd, 2, TimeUnit.MINUTES);
            return result;
        }
    }
}
