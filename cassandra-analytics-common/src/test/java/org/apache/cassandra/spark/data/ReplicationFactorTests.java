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

import java.util.ArrayList;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReplicationFactorTests
{
    @Test
    public void testReplicationFactorNtsClassNameOnly()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3",
        "datacenter2", "5"));
        assertThat(replicationFactor.getReplicationStrategy())
        .isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(replicationFactor.getOptions().get("datacenter1")).isEqualTo(Integer.valueOf(3));
        assertThat(replicationFactor.getOptions().get("datacenter2")).isEqualTo(Integer.valueOf(5));
    }

    @Test
    public void testReplicationFactorNtsFullyQualifiedClassName()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
        "datacenter1", "9",
        "datacenter2", "2"));
        assertThat(replicationFactor.getReplicationStrategy())
        .isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(replicationFactor.getOptions().get("datacenter1")).isEqualTo(Integer.valueOf(9));
        assertThat(replicationFactor.getOptions().get("datacenter2")).isEqualTo(Integer.valueOf(2));
    }

    @Test
    public void testReplicationFactorSimpleClassNameOnly()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "SimpleStrategy",
        "replication_factor", "3"));
        assertThat(replicationFactor.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.SimpleStrategy);
        assertThat(replicationFactor.getOptions().get("replication_factor")).isEqualTo(Integer.valueOf(3));
    }

    @Test
    public void testReplicationFactorSimpleFullyQualifiedClassName()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.SimpleStrategy",
        "replication_factor", "5"));
        assertThat(replicationFactor.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.SimpleStrategy);
        assertThat(replicationFactor.getOptions().get("replication_factor")).isEqualTo(Integer.valueOf(5));
    }

    @Test()
    public void testUnexpectedRFClass()
    {
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.NotSimpleStrategy",
        "replication_factor", "5")))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test()
    public void testUnknownRFClass()
    {
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "NoSuchStrategy",
        "replication_factor", "5")))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEquality()
    {
        ReplicationFactor replicationFactor1 = new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.SimpleStrategy",
        "replication_factor", "5"));
        ReplicationFactor replicationFactor2 = new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.SimpleStrategy",
        "replication_factor", "5"));
        assertThat(replicationFactor1).isNotSameAs(replicationFactor2);
        assertThat(replicationFactor1).isNotEqualTo(null);
        assertThat(replicationFactor2).isNotEqualTo(null);
        assertThat(replicationFactor1).isEqualTo(replicationFactor1);
        assertThat(replicationFactor2).isEqualTo(replicationFactor2);
        assertThat(replicationFactor1).isNotEqualTo(new ArrayList<>());
        assertThat(replicationFactor1).isEqualTo(replicationFactor2);
        assertThat(replicationFactor1.hashCode()).isEqualTo(replicationFactor2.hashCode());
    }
}
