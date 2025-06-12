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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link DynamicSizing} class
 */
class DynamicSizingTest
{
    public static final long TEN_GIB = 10L * 1024L * 1024L * 1024L;
    private ReplicationFactor rf;

    @BeforeEach
    public void setup()
    {
        rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, Map.of("datacenter1", 3));
    }

    @Test
    public void testTableSize1ByteRF2MaxPartitionSize1GiB()
    {
        testScenario(1000, 1, 1, 1);
    }

    @Test
    public void testTableSize10GiBRF2MaxPartitionSize1GiB()
    {
        testScenario(1000, 1, TEN_GIB, 20);
    }

    @Test
    public void testTableSize10GiBRF2MaxPartitionSize1GiBBounded()
    {
        // upper bounded by 5 cores
        testScenario(5, 1, TEN_GIB, 5);
    }

    @Test
    public void testTableSize10GiBRF2MaxPartitionSize5GB()
    {
        testScenario(1000, 5, TEN_GIB, 4);
    }

    @Test
    public void testTableSize10GiBRF2MaxPartitionSize5GBBounded()
    {
        testScenario(2, 5, TEN_GIB, 2);
    }

    private void testScenario(int numCores, int maxPartitionSize, long expectedTableSizeInBytes, int expectedNumberOfCores)
    {
        TableSizeProvider tableSizeProvider = (keyspace, table, datacenter) -> expectedTableSizeInBytes;
        Sizing sizing = new DynamicSizing(tableSizeProvider,
                                          ConsistencyLevel.LOCAL_QUORUM,
                                          rf,
                                          "big-data",
                                          "customers",
                                          "datacenter1",
                                          maxPartitionSize,
                                          numCores);

        assertThat(sizing.getEffectiveNumberOfCores()).as("Number of cores does not match").isEqualTo(expectedNumberOfCores);
    }
}
