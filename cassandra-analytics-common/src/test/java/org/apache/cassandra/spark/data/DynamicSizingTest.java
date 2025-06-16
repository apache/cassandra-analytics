/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.data;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link DynamicSizing} class
 */
class DynamicSizingTest
{
    public static final long TEN_GIB = 10L * 1024L * 1024L * 1024L;
    private static final ReplicationFactor RF = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                      Map.of("datacenter1", 3));

    @ParameterizedTest
    @MethodSource("scenarios")
    void testSizingScenario(SizingScenario scenario)
    {
        TableSizeProvider tableSizeProvider = (keyspace, table, datacenter) -> scenario.tableSizeInBytes;
        Sizing sizing = new DynamicSizing(tableSizeProvider,
                                          ConsistencyLevel.LOCAL_QUORUM,
                                          RF,
                                          "big-data",
                                          "customers",
                                          "datacenter1",
                                          scenario.maxPartitionSize,
                                          scenario.numCores);
        assertThat(sizing.getEffectiveNumberOfCores()).as("Number of cores does not match").isEqualTo(scenario.expectedNumberOfCores);
    }

    static Stream<Arguments> scenarios()
    {
        return Stream.of(
        Arguments.arguments(new SizingScenario(1000, 5, TEN_GIB, 4)),
        Arguments.arguments(new SizingScenario(1000, 1, TEN_GIB, 20)),
        Arguments.arguments(new SizingScenario(1000, 1, TEN_GIB, 20)),
        Arguments.arguments(new SizingScenario(1000, 5, TEN_GIB, 4)),
        Arguments.arguments(new SizingScenario(1000, 5, TEN_GIB, 4))
        );
    }

    static class SizingScenario
    {
        private final int numCores;
        private final int maxPartitionSize;
        private final long tableSizeInBytes;
        private final int expectedNumberOfCores;

        SizingScenario(int numCores, int maxPartitionSize, long tableSizeInBytes, int expectedNumberOfCores)
        {
            this.numCores = numCores;
            this.maxPartitionSize = maxPartitionSize;
            this.tableSizeInBytes = tableSizeInBytes;
            this.expectedNumberOfCores = expectedNumberOfCores;
        }

        @Override
        public String toString()
        {
            return "Scenario{" +
                   "numCores=" + numCores +
                   ", maxPartitionSize=" + maxPartitionSize +
                   ", tableSizeInBytes=" + tableSizeInBytes +
                   ", expectedNumberOfCores=" + expectedNumberOfCores +
                   '}';
        }
    }
}
