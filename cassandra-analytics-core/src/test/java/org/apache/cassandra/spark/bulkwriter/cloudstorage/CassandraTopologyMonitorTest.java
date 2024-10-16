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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraTopologyMonitorTest
{
    @Test
    void testNoTopologyChange()
    {
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockClusterInfo.getTokenRangeMapping(false)).thenReturn(buildTopology(10));
        AtomicBoolean noChange = new AtomicBoolean(true);
        CassandraTopologyMonitor monitor = new CassandraTopologyMonitor(mockClusterInfo, event -> noChange.set(false));
        monitor.checkTopologyOnDemand();
        assertTrue(noChange.get());
    }

    @Test
    void testTopologyChanged()
    {
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockClusterInfo.getTokenRangeMapping(false))
        .thenReturn(buildTopology(10))
        .thenReturn(buildTopology(11)); // token moved
        AtomicBoolean noChange = new AtomicBoolean(true);
        CassandraTopologyMonitor monitor = new CassandraTopologyMonitor(mockClusterInfo, event -> noChange.set(false));
        monitor.checkTopologyOnDemand();
        assertFalse(noChange.get());
    }

    private TokenRangeMapping<RingInstance> buildTopology(int instancesCount)
    {
        return TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), instancesCount);
    }
}
