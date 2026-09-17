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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

    // Transient / witness replicas: the <replicas>/<transient> form, reused by witness replicas under
    // mutation tracking (CEP-45/CEP-46)

    @Test
    public void testNoTransientReplicasByDefault()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3"));
        assertThat(replicationFactor.hasTransientReplicas()).isFalse();
        assertThat(replicationFactor.getTransientOptions()).isEmpty();
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(3);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(3);
        assertThat(replicationFactor.getTransientReplicationFactor()).isEqualTo(0);
        assertThat(replicationFactor.getFullReplicas("datacenter1")).isEqualTo(3);
        assertThat(replicationFactor.getTransientReplicas("datacenter1")).isEqualTo(0);
    }

    @Test
    public void testTransientReplicasSingleDatacenter()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1"));
        assertThat(replicationFactor.hasTransientReplicas()).isTrue();
        // total keeps its original meaning: all replicas, witnesses included
        assertThat(replicationFactor.getOptions().get("datacenter1")).isEqualTo(Integer.valueOf(3));
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(3);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(2);
        assertThat(replicationFactor.getTransientReplicationFactor()).isEqualTo(1);
        assertThat(replicationFactor.getFullReplicas("datacenter1")).isEqualTo(2);
        assertThat(replicationFactor.getTransientReplicas("datacenter1")).isEqualTo(1);
    }

    @Test
    public void testTransientReplicasMultipleDatacenters()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3/1"));
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(4);
        assertThat(replicationFactor.getTransientReplicationFactor()).isEqualTo(2);
    }

    @Test
    public void testMixedTransientAndFullDatacenters()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(5);
        assertThat(replicationFactor.getTransientReplicas("datacenter1")).isEqualTo(1);
        assertThat(replicationFactor.getTransientReplicas("datacenter2")).isEqualTo(0);
        assertThat(replicationFactor.getFullReplicas("datacenter2")).isEqualTo(3);
        // datacenter2 has no transient replicas, so it must be absent rather than mapped to zero
        assertThat(replicationFactor.getTransientOptions()).containsOnlyKeys("datacenter1");
    }

    @Test
    public void testTransientReplicasSimpleStrategy()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "SimpleStrategy",
        "replication_factor", "3/1"));
        assertThat(replicationFactor.getReplicationStrategy())
        .isEqualTo(ReplicationFactor.ReplicationStrategy.SimpleStrategy);
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(3);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(2);
    }

    @Test
    public void testTransientReplicasWithWhitespace()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", " 3 / 1 "));
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(3);
        assertThat(replicationFactor.getFullReplicationFactor()).isEqualTo(2);
    }

    @Test
    public void testTransientEqualToTotalIsRejected()
    {
        // Cassandra requires at least one full replica, so 3/3 is invalid and must not be accepted silently
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/3")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("datacenter1");
    }

    @Test
    public void testMalformedReplicationValuesAreRejected()
    {
        for (String malformed : new String[]{"3/", "/1", "3/1/1", "3/x", "x/1", "3/-1", ""})
        {
            assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
            "class", "NetworkTopologyStrategy",
            "datacenter1", malformed)))
            .as("malformed replication value '%s' should be rejected", malformed)
            .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testGetFullReplicasUnknownDatacenter()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1"));
        assertThatThrownBy(() -> replicationFactor.getFullReplicas("nosuchdc"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testTransientOptionsForUnknownDatacenterRejected()
    {
        assertThatThrownBy(() -> new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
        ImmutableMap.of("datacenter1", 3),
        ImmutableMap.of("datacenter2", 1)))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEqualityConsidersTransientReplicas()
    {
        ReplicationFactor full = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3"));
        ReplicationFactor withTransient = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1"));
        assertThat(full).isNotEqualTo(withTransient);
        assertThat(full.hashCode()).isNotEqualTo(withTransient.hashCode());
    }

    @Test
    public void testNegativeReplicationFactorIsRejected()
    {
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "-3")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("datacenter1");
    }

    @Test
    public void testZeroReplicationFactorIsAllowed()
    {
        // RF 0 is legitimate for NetworkTopologyStrategy: the keyspace is simply not replicated to that datacenter
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3",
        "datacenter2", "0"));
        assertThat(replicationFactor.getOptions().get("datacenter2")).isEqualTo(Integer.valueOf(0));
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(3);
    }

    @Test
    public void testKryoSerializationRoundTripWithTransientReplicas() throws Exception
    {
        ReplicationFactor original = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));

        Kryo kryo = new Kryo();
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, original);
        }
        ReplicationFactor deserialized;
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = kryo.readObject(in, ReplicationFactor.class);
        }

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(deserialized.getFullReplicationFactor()).isEqualTo(5);
        assertThat(deserialized.getTransientReplicas("datacenter1")).isEqualTo(1);
        assertThat(deserialized.getTransientReplicas("datacenter2")).isEqualTo(0);
    }

    @Test
    public void testJdkSerializationRoundTripWithTransientReplicas() throws Exception    {
        ReplicationFactor original = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(original);
        }
        ReplicationFactor deserialized;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = (ReplicationFactor) in.readObject();
        }

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getFullReplicationFactor()).isEqualTo(5);
        assertThat(deserialized.getTransientReplicas("datacenter1")).isEqualTo(1);
    }

    @Test
    public void testUnparseableValueIsRejected()
    {
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "xyz")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("datacenter1");
    }

    @Test
    public void testNoDatacenterEntriesIsRejected()
    {
        assertThatThrownBy(() -> new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Could not find replication info");
    }

    @Test
    public void testLocalStrategyWithNoEntriesIsAllowed()
    {
        // LocalStrategy legitimately has no datacenter entries, e.g. the system_schema keyspace
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "org.apache.cassandra.locator.LocalStrategy"));
        assertThat(replicationFactor.getReplicationStrategy())
        .isEqualTo(ReplicationFactor.ReplicationStrategy.LocalStrategy);
        assertThat(replicationFactor.getOptions()).isEmpty();
        assertThat(replicationFactor.getTotalReplicationFactor()).isEqualTo(0);
    }

    // The per-datacenter counts are held together rather than in two parallel maps, so the total and the
    // transient count cannot drift apart

    @Test
    public void testReplicationExposesCombinedCounts()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));

        assertThat(replicationFactor.getReplication()).containsOnlyKeys("datacenter1", "datacenter2");
        ReplicationFactor.ReplicaCounts dc1 = replicationFactor.getReplication().get("datacenter1");
        assertThat(dc1.allReplicas()).isEqualTo(3);
        assertThat(dc1.fullReplicas()).isEqualTo(2);
        assertThat(dc1.transientReplicas()).isEqualTo(1);

        ReplicationFactor.ReplicaCounts dc2 = replicationFactor.getReplication().get("datacenter2");
        assertThat(dc2.allReplicas()).isEqualTo(3);
        assertThat(dc2.fullReplicas()).isEqualTo(3);
        assertThat(dc2.transientReplicas()).isEqualTo(0);
    }

    @Test
    public void testDerivedOptionMapsAreUnmodifiable()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1"));
        assertThatThrownBy(() -> replicationFactor.getOptions().put("datacenter2", 3))
        .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> replicationFactor.getTransientOptions().put("datacenter2", 1))
        .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> replicationFactor.getReplication().remove("datacenter1"))
        .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testGetTransientReplicasUnknownDatacenter()
    {
        // Consistent with getFullReplicas: an unknown datacenter is a programming error, not zero transients
        ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1"));
        assertThatThrownBy(() -> replicationFactor.getTransientReplicas("nosuchdc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nosuchdc");
    }

    @Test
    public void testPrecomputedValuesSurviveJdkSerialization() throws Exception
    {
        // The aggregate counts and derived maps are precomputed and transient, so they are absent from the
        // serialized form and must be rebuilt on the way back in
        ReplicationFactor original = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(original);
        }
        ReplicationFactor deserialized;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = (ReplicationFactor) in.readObject();
        }

        assertThat(deserialized.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(deserialized.getFullReplicationFactor()).isEqualTo(5);
        assertThat(deserialized.getTransientReplicationFactor()).isEqualTo(1);
        assertThat(deserialized.hasTransientReplicas()).isTrue();
        assertThat(deserialized.getOptions()).containsOnlyKeys("datacenter1", "datacenter2");
        assertThat(deserialized.getTransientOptions()).containsOnlyKeys("datacenter1");
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testPrecomputedValuesSurviveKryoSerialization()
    {
        ReplicationFactor original = new ReplicationFactor(ImmutableMap.of(
        "class", "NetworkTopologyStrategy",
        "datacenter1", "3/1",
        "datacenter2", "3"));

        Kryo kryo = new Kryo();
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, original);
        }
        ReplicationFactor deserialized;
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = kryo.readObject(in, ReplicationFactor.class);
        }

        assertThat(deserialized.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(deserialized.getFullReplicationFactor()).isEqualTo(5);
        assertThat(deserialized.getOptions()).containsOnlyKeys("datacenter1", "datacenter2");
        assertThat(deserialized.getTransientOptions()).containsOnlyKeys("datacenter1");
    }
}
