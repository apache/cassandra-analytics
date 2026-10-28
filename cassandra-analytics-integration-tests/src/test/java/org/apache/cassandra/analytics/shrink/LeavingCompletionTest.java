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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.testing.IClusterExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LeavingCompletionTest
{
    @Test
    void propagatesDecommissionFailureToTheTestThread()
    {
        Scenario scenario = new Scenario(result(1, "Unexpected streaming failure"));
        assertThatThrownBy(() -> scenario.completeTransitionsAndValidateWrites(new CountDownLatch(0), Stream.empty()))
        .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsSuccessWhenDecommissionFailureWasExpected()
    {
        Scenario scenario = new Scenario(result(0, null));
        assertThatThrownBy(() -> scenario.completeTransitionsAndValidateWrites(new CountDownLatch(0), Stream.empty(), true))
        .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsAnUnrelatedDecommissionFailure()
    {
        Scenario scenario = new Scenario(result(1, "Unexpected streaming failure"));
        assertThatThrownBy(() -> scenario.completeTransitionsAndValidateWrites(new CountDownLatch(0), Stream.empty(), true))
        .isInstanceOf(AssertionError.class);
    }

    @Test
    void acceptsTheInjectedDecommissionFailure()
    {
        Scenario scenario = new Scenario(result(1, "Simulated leave failure"));
        assertThatCode(() -> scenario.completeTransitionsAndValidateWrites(new CountDownLatch(0), Stream.empty(), true))
        .doesNotThrowAnyException();
    }

    private static NodeToolResult result(int rc, String error)
    {
        return new NodeToolResult(new String[]{"decommission"}, rc, Collections.emptyList(),
                                  error == null ? null : new RuntimeException(error));
    }

    private static class Scenario extends LeavingTestBase
    {
        Scenario(NodeToolResult result)
        {
            IClusterExtension<IInstance> cluster = mock(IClusterExtension.class);
            IInstance leaving = mock(IInstance.class);
            IInstanceConfig config = mock(IInstanceConfig.class);
            when(config.num()).thenReturn(1);
            when(leaving.config()).thenReturn(config);
            when(cluster.size()).thenReturn(1);
            when(cluster.get(1)).thenReturn(leaving);
            when(cluster.getFirstRunningInstance()).thenReturn(leaving);
            when(leaving.nodetoolResult("decommission")).thenReturn(result);
            this.cluster = cluster;
            afterSchemaInitialized();
        }

        @Override
        protected void prepareTopologyChange()
        {
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
