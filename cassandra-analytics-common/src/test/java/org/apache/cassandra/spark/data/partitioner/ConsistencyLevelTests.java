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

import static org.assertj.core.api.Assertions.assertThat;

public class ConsistencyLevelTests
{
    @Test
    public void testSimpleStrategy()
    {
        assertThat(ConsistencyLevel.ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 3)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 1)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.TWO.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 3)), null)).isEqualTo(2);
        assertThat(ConsistencyLevel.THREE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 3)), null)).isEqualTo(3);
        assertThat(ConsistencyLevel.LOCAL_ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 3)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 3)), null)).isEqualTo(2);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                              ImmutableMap.of("replication_factor", 5)), null)).isEqualTo(3);
    }

    @Test
    public void testNetworkTopologyStrategy()
    {
        assertThat(ConsistencyLevel.ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 1)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.TWO.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), null)).isEqualTo(2);
        assertThat(ConsistencyLevel.THREE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), null)).isEqualTo(3);
        assertThat(ConsistencyLevel.LOCAL_ONE.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), null)).isEqualTo(1);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), "DC1")).isEqualTo(2);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 5)), "DC1")).isEqualTo(3);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3)), null)).isEqualTo(2);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 5)), null)).isEqualTo(3);

        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC1")).isEqualTo(2);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC2")).isEqualTo(3);
        assertThat(ConsistencyLevel.LOCAL_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), "DC3")).isEqualTo(3);

        assertThat(ConsistencyLevel.EACH_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3, "DC2", 5)), null)).isEqualTo(5);
        assertThat(ConsistencyLevel.EACH_QUORUM.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 3, "DC2", 5, "DC3", 4)), null)).isEqualTo(8);

        assertThat(ConsistencyLevel.ALL.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 5)), null)).isEqualTo(5);
        assertThat(ConsistencyLevel.ALL.blockFor(
        new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                              ImmutableMap.of("DC1", 5, "DC2", 5)), null)).isEqualTo(10);
    }
}
