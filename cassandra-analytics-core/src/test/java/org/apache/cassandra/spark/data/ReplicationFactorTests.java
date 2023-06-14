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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReplicationFactorTests
{
    @Test
    public void testReplicationFactorNtsClassNameOnly()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
                "class", "NetworkTopologyStrategy",
                "datacenter1", "3",
                "datacenter2", "5"));
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                     replicationFactor.getReplicationStrategy());
        assertEquals(Integer.valueOf(3), replicationFactor.getOptions().get("datacenter1"));
        assertEquals(Integer.valueOf(5), replicationFactor.getOptions().get("datacenter2"));
    }

    @Test
    public void testReplicationFactorNtsFullyQualifiedClassName()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
                "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                "datacenter1", "9",
                "datacenter2", "2"));
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                     replicationFactor.getReplicationStrategy());
        assertEquals(Integer.valueOf(9), replicationFactor.getOptions().get("datacenter1"));
        assertEquals(Integer.valueOf(2), replicationFactor.getOptions().get("datacenter2"));
    }

    @Test
    public void testReplicationFactorSimpleClassNameOnly()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
                "class", "SimpleStrategy",
                "replication_factor", "3"));
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, replicationFactor.getReplicationStrategy());
        assertEquals(Integer.valueOf(3), replicationFactor.getOptions().get("replication_factor"));
    }

    @Test
    public void testReplicationFactorSimpleFullyQualifiedClassName()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
                "class", "org.apache.cassandra.locator.SimpleStrategy",
                "replication_factor", "5"));
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, replicationFactor.getReplicationStrategy());
        assertEquals(Integer.valueOf(5), replicationFactor.getOptions().get("replication_factor"));
    }

    @Test()
    public void testUnexpectedRFClass()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> new ReplicationFactor(ImmutableMap.of(
                     "class", "org.apache.cassandra.locator.NotSimpleStrategy",
                     "replication_factor", "5"))
        );
    }

    @Test()
    public void testUnknownRFClass()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> new ReplicationFactor(ImmutableMap.of(
                     "class", "NoSuchStrategy",
                     "replication_factor", "5"))
        );
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
        assertNotSame(replicationFactor1, replicationFactor2);
        assertNotEquals(null, replicationFactor1);
        assertNotEquals(replicationFactor2, null);
        assertEquals(replicationFactor1, replicationFactor1);
        assertEquals(replicationFactor2, replicationFactor2);
        assertNotEquals(new ArrayList<>(), replicationFactor1);
        assertEquals(replicationFactor1, replicationFactor2);
        assertEquals(replicationFactor1.hashCode(), replicationFactor2.hashCode());
    }
}
