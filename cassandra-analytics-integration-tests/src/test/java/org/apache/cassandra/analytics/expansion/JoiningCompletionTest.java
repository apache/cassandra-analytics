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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.testing.IClusterExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JoiningCompletionTest
{
    @Test
    void rejectsAJoinThatHasNotReachedNormal()
    {
        IClusterExtension<IInstance> cluster = mock(IClusterExtension.class);
        IInstance seed = mock(IInstance.class);
        IInstance joining = mock(IInstance.class);
        when(cluster.get(1)).thenReturn(seed);
        doThrow(new AssertionError("Node is still Joining")).when(cluster).awaitRingState(seed, joining, "Normal");

        Scenario scenario = new Scenario(cluster, joining);
        assertThatThrownBy(() -> scenario.completeTransitionsAndValidateWrites(new CountDownLatch(0), Stream.empty(), false))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Node is still Joining");
    }

    private static class Scenario extends JoiningTestBase
    {
        Scenario(IClusterExtension<IInstance> cluster, IInstance joining)
        {
            this.cluster = cluster;
            newInstances = Collections.singletonList(joining);
        }

        @Override
        protected void initializeSchemaForTest()
        {
        }

        @Override
        protected CountDownLatch transitioningStateStart()
        {
            return new CountDownLatch(0);
        }
    }
}
