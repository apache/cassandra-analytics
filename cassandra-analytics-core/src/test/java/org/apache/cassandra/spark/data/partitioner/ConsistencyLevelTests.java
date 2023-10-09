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

package org.apache.cassandra.spark.data.partitioner;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsistencyLevelTests
{
    @Test
    public void testSimpleStrategy()
    {
        assertEquals(1, ConsistencyLevel.ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 3)), null));
        assertEquals(1, ConsistencyLevel.ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 1)), null));
        assertEquals(2, ConsistencyLevel.TWO.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 3)), null));
        assertEquals(3, ConsistencyLevel.THREE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 3)), null));
        assertEquals(1, ConsistencyLevel.LOCAL_ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 3)), null));
        assertEquals(2, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 3)), null));
        assertEquals(3, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                      ImmutableMap.of("replication_factor", 5)), null));
    }

    @Test
    public void testNetworkTopologyStrategy()
    {
        assertEquals(1, ConsistencyLevel.ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), null));
        assertEquals(1, ConsistencyLevel.ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 1)), null));
        assertEquals(2, ConsistencyLevel.TWO.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), null));
        assertEquals(3, ConsistencyLevel.THREE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), null));
        assertEquals(1, ConsistencyLevel.LOCAL_ONE.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), null));
        assertEquals(2, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), "DC1"));
        assertEquals(3, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 5)), "DC1"));
        assertEquals(2, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3)), null));
        assertEquals(3, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 5)), null));

        assertEquals(2, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC1"));
        assertEquals(3, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC2"));
        assertEquals(3, ConsistencyLevel.LOCAL_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC3"));

        assertEquals(5, ConsistencyLevel.EACH_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3, "DC2", 5)), null));
        assertEquals(8, ConsistencyLevel.EACH_QUORUM.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), null));

        assertEquals(5, ConsistencyLevel.ALL.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 5)), null));
        assertEquals(10, ConsistencyLevel.ALL.blockFor(
                new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                      ImmutableMap.of("DC1", 5, "DC2", 5)), null));
    }
}
