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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testEachQuorumBlockForWithNetworkTopologyStrategy()
    {
        // Create a NetworkTopologyStrategy with multiple data centers
        Map<String, Integer> options = Map.of(
        "DC1", 3,
        "DC2", 5,
        "DC3", 2
        );
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
        options
        );

        Map<String, Integer> result = ConsistencyLevel.EACH_QUORUM.eachQuorumBlockFor(replicationFactor);

        // Expected: (3/2)+1=2, (5/2)+1=3, (2/2)+1=2
        assertThat(result).hasSize(3)
                          .containsEntry("DC1", 2)
                          .containsEntry("DC2", 3)
                          .containsEntry("DC3", 2);
    }

    @Test
    void testEachQuorumBlockForWithSingleDataCenter()
    {
        Map<String, Integer> options = Map.of("DC1", 6);
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
        options
        );

        Map<String, Integer> result = ConsistencyLevel.EACH_QUORUM.eachQuorumBlockFor(replicationFactor);

        // Expected: (6/2)+1=4
        assertThat(result).containsEntry("DC1", 4);
        assertThat(result).hasSize(1);
    }

    @Test
    void testEachQuorumBlockForWithOddReplicationFactors()
    {
        Map<String, Integer> options = Map.of(
        "DC1", 1,
        "DC2", 3,
        "DC3", 5
        );
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
        options
        );

        Map<String, Integer> result = ConsistencyLevel.EACH_QUORUM.eachQuorumBlockFor(replicationFactor);

        // Expected: (1/2)+1=1, (3/2)+1=2, (5/2)+1=3
        assertThat(result).containsEntry("DC1", 1);
        assertThat(result).containsEntry("DC2", 2);
        assertThat(result).containsEntry("DC3", 3);
        assertThat(result).hasSize(3);
    }

    @Test
    void testEachQuorumBlockForThrowsExceptionForSimpleStrategy()
    {
        ReplicationFactor replicationFactor = ReplicationFactor.simpleStrategy(3);

        assertThatThrownBy(() -> ConsistencyLevel.EACH_QUORUM.eachQuorumBlockFor(replicationFactor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid Replication Strategy: SimpleStrategy for EACH_QUORUM consistency read.");
    }

    @Test
    void testEachQuorumBlockForAllOtherConsistencyLevelsThrowException()
    {
        Map<String, Integer> options = Map.of("DC1", 3);
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
        options
        );

        // Test a few other consistency levels to ensure they all throw exceptions
        assertThatThrownBy(() -> ConsistencyLevel.ONE.eachQuorumBlockFor(replicationFactor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consistency level needed is EACH_QUORUM, provided is: ONE");

        assertThatThrownBy(() -> ConsistencyLevel.LOCAL_QUORUM.eachQuorumBlockFor(replicationFactor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consistency level needed is EACH_QUORUM, provided is: LOCAL_QUORUM");

        assertThatThrownBy(() -> ConsistencyLevel.ALL.eachQuorumBlockFor(replicationFactor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consistency level needed is EACH_QUORUM, provided is: ALL");
    }
}
