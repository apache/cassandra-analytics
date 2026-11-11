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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

@SuppressWarnings("UnstableApiUsage")
public class CassandraRingTests
{
    private static Range<BigInteger> mergeRanges(Collection<Range<BigInteger>> ranges)
    {
        Range<BigInteger> mergedRange = null;
        for (Range<BigInteger> range : ranges)
        {
            if (mergedRange == null)
            {
                mergedRange = range;
            }
            else
            {
                mergedRange = mergedRange.span(range);
            }
        }

        return mergedRange;
    }

    private void validateRanges(Collection<Range<BigInteger>> ranges,
                                Collection<BigInteger> enclosedTokens,
                                Collection<BigInteger> excludedTokens)
    {
        RangeSet<BigInteger> rangeSet = TreeRangeSet.create();

        ranges.forEach(rangeSet::add);
        enclosedTokens.forEach(token -> assertThat(rangeSet.contains(token)).as(token + " should have been a valid token").isTrue());
        excludedTokens.forEach(token -> assertThat(rangeSet.contains(token)).isFalse());
    }

    @Test
    public void testSimpleStrategyRF3()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DEV"),
                                                          new CassandraInstance("100", "local0-i2", "DEV"),
                                                          new CassandraInstance("200", "local0-i3", "DEV"));
        CassandraRing ring = new CassandraRing(Partitioner.Murmur3Partitioner,
                                               "test",
                                               new ReplicationFactor(ImmutableMap.of(
                                               "class", "org.apache.cassandra.locator.SimpleStrategy",
                                               "replication_factor", "3")),
                                               instances);

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(200L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();
        for (CassandraInstance instance : instances)
        {
            assertThat(mergeRanges(tokenRanges.get(instance)))
            .isEqualTo(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                        Partitioner.Murmur3Partitioner.maxToken()));
        }
    }

    @Test
    public void testSimpleStrategyRF1()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DEV"),
                                                          new CassandraInstance("100", "local0-i2", "DEV"),
                                                          new CassandraInstance("200", "local0-i3", "DEV"));
        CassandraRing ring = new CassandraRing(Partitioner.Murmur3Partitioner,
                                               "test",
                                               new ReplicationFactor(ImmutableMap.of(
                                               "class", "org.apache.cassandra.locator.SimpleStrategy",
                                               "replication_factor", "1")),
                                               instances);

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(200L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();

        // token(0) => [201 - 0] => [201 - MAX], [MIN - 0]
        validateRanges(tokenRanges.get(instances.get(0)),
                       Arrays.asList(BigInteger.ZERO,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(1L)));

        // token(100) => [1 - 100]
        validateRanges(tokenRanges.get(instances.get(1)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(50L)),
                       Arrays.asList(BigInteger.valueOf(101L),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(0L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));

        // token(200) => [101 - 200]
        validateRanges(tokenRanges.get(instances.get(2)),
                       Arrays.asList(BigInteger.valueOf(101L),
                                     BigInteger.valueOf(150L),
                                     BigInteger.valueOf(200L)),
                       Arrays.asList(BigInteger.valueOf(100L),
                                     BigInteger.valueOf(201L),
                                     BigInteger.valueOf(1L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));
    }

    @Test
    public void testSimpleStrategyRF2()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DEV"),
                                                          new CassandraInstance("100", "local0-i2", "DEV"),
                                                          new CassandraInstance("200", "local0-i3", "DEV"));
        CassandraRing ring = new CassandraRing(Partitioner.Murmur3Partitioner,
                                               "test",
                                               new ReplicationFactor(ImmutableMap.of(
                                               "class", "org.apache.cassandra.locator.SimpleStrategy",
                                               "replication_factor", "2")),
                                               instances);

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(200L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();

        // token(0) => [101 - 0] => [101 - MAX] [MIN - 0]
        validateRanges(tokenRanges.get(instances.get(0)),
                       Arrays.asList(BigInteger.ZERO,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(101L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(1L)));

        // token(100) => [201 - 100] => [201 - MAX] [MIN - 100]
        validateRanges(tokenRanges.get(instances.get(1)),
                       Arrays.asList(BigInteger.valueOf(0L),
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(200L)));

        // token(200) => [1 - 200]
        validateRanges(tokenRanges.get(instances.get(2)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(200L)),
                       Arrays.asList(BigInteger.valueOf(0L),
                                     BigInteger.valueOf(201L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));
    }

    @Test
    public void testNetworkStrategyRF33()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                                                          new CassandraInstance("100", "local0-i2", "DC1"),
                                                          new CassandraInstance("200", "local0-i3", "DC1"),
                                                          new CassandraInstance("1", "local1-i1", "DC2"),
                                                          new CassandraInstance("101", "local1-i2", "DC2"),
                                                          new CassandraInstance("201", "local1-i3", "DC2"));

        CassandraRing ring = new CassandraRing(
        Partitioner.Murmur3Partitioner,
        "test",
        new ReplicationFactor(ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                              "DC1", "3",
                                              "DC2", "3")),
        Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                      new CassandraInstance("100", "local0-i2", "DC1"),
                      new CassandraInstance("200", "local0-i3", "DC1"),
                      new CassandraInstance("1", "local1-i1", "DC2"),
                      new CassandraInstance("101", "local1-i2", "DC2"),
                      new CassandraInstance("201", "local1-i3", "DC2")));

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(1L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(101L),
                                                                    BigInteger.valueOf(200L),
                                                                    BigInteger.valueOf(201L)).toArray());

        assertThat(ring.tokens("DC1").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                         BigInteger.valueOf(100L),
                                                                         BigInteger.valueOf(200L)).toArray());

        assertThat(ring.tokens("DC2").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(1L),
                                                                         BigInteger.valueOf(101L),
                                                                         BigInteger.valueOf(201L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();
        for (CassandraInstance instance : instances)
        {
            assertThat(mergeRanges(tokenRanges.get(instance)))
            .isEqualTo(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                        Partitioner.Murmur3Partitioner.maxToken()));
        }
    }

    @Test
    public void testNetworkStrategyRF11()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                                                          new CassandraInstance("100", "local0-i2", "DC1"),
                                                          new CassandraInstance("200", "local0-i3", "DC1"),
                                                          new CassandraInstance("1", "local1-i1", "DC2"),
                                                          new CassandraInstance("101", "local1-i2", "DC2"),
                                                          new CassandraInstance("201", "local1-i3", "DC2"));

        CassandraRing ring = new CassandraRing(
        Partitioner.Murmur3Partitioner,
        "test",
        new ReplicationFactor(ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                              "DC1", "1",
                                              "DC2", "1")),
        Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                      new CassandraInstance("100", "local0-i2", "DC1"),
                      new CassandraInstance("200", "local0-i3", "DC1"),
                      new CassandraInstance("1", "local1-i1", "DC2"),
                      new CassandraInstance("101", "local1-i2", "DC2"),
                      new CassandraInstance("201", "local1-i3", "DC2")));

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(1L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(101L),
                                                                    BigInteger.valueOf(200L),
                                                                    BigInteger.valueOf(201L)).toArray());

        assertThat(ring.tokens("DC1").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                         BigInteger.valueOf(100L),
                                                                         BigInteger.valueOf(200L)).toArray());

        assertThat(ring.tokens("DC2").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(1L),
                                                                         BigInteger.valueOf(101L),
                                                                         BigInteger.valueOf(201L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();

        // token(0) => [201 - 0] => [201 - MAX], [MIN - 0]
        validateRanges(tokenRanges.get(instances.get(0)),
                       Arrays.asList(BigInteger.ZERO,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(1L)));

        // token(100) => [1 - 100]
        validateRanges(tokenRanges.get(instances.get(1)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(50L)),
                       Arrays.asList(BigInteger.valueOf(101L),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(0L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));

        // token(200) => [101 - 200]
        validateRanges(tokenRanges.get(instances.get(2)),
                       Arrays.asList(BigInteger.valueOf(101L),
                                     BigInteger.valueOf(150L),
                                     BigInteger.valueOf(200L)),
                       Arrays.asList(BigInteger.valueOf(100L),
                                     BigInteger.valueOf(201L),
                                     BigInteger.valueOf(1L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));

        // token(1) => [202 - 1] => [202 - MAX], [MIN - 1]
        validateRanges(tokenRanges.get(instances.get(3)),
                       Arrays.asList(BigInteger.ONE,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(202L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(201L),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(2L)));

        // token(101) => [2 - 101]
        validateRanges(tokenRanges.get(instances.get(4)),
                       Arrays.asList(BigInteger.valueOf(2L),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(50L)),
                       Arrays.asList(BigInteger.valueOf(102L),
                                     BigInteger.valueOf(201L),
                                     BigInteger.valueOf(1L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));

        // token(201) => [102 - 201]
        validateRanges(tokenRanges.get(instances.get(5)),
                       Arrays.asList(BigInteger.valueOf(102L),
                                     BigInteger.valueOf(151L),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(BigInteger.valueOf(101L),
                                     BigInteger.valueOf(202L),
                                     BigInteger.valueOf(2L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));
    }

    @Test
    public void testNetworkStrategyRF22()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                                                          new CassandraInstance("100", "local0-i2", "DC1"),
                                                          new CassandraInstance("200", "local0-i3", "DC1"),
                                                          new CassandraInstance("1", "local1-i1", "DC2"),
                                                          new CassandraInstance("101", "local1-i2", "DC2"),
                                                          new CassandraInstance("201", "local1-i3", "DC2"));

        CassandraRing ring = new CassandraRing(
        Partitioner.Murmur3Partitioner,
        "test",
        new ReplicationFactor(ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                              "DC1", "2",
                                              "DC2", "2")),
        Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                      new CassandraInstance("100", "local0-i2", "DC1"),
                      new CassandraInstance("200", "local0-i3", "DC1"),
                      new CassandraInstance("1", "local1-i1", "DC2"),
                      new CassandraInstance("101", "local1-i2", "DC2"),
                      new CassandraInstance("201", "local1-i3", "DC2")));

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                    BigInteger.valueOf(1L),
                                                                    BigInteger.valueOf(100L),
                                                                    BigInteger.valueOf(101L),
                                                                    BigInteger.valueOf(200L),
                                                                    BigInteger.valueOf(201L)).toArray());

        assertThat(ring.tokens("DC1").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(0L),
                                                                         BigInteger.valueOf(100L),
                                                                         BigInteger.valueOf(200L)).toArray());

        assertThat(ring.tokens("DC2").toArray()).isEqualTo(Arrays.asList(BigInteger.valueOf(1L),
                                                                         BigInteger.valueOf(101L),
                                                                         BigInteger.valueOf(201L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();

        // token(0) => [101 - 0] => [101 - MAX] [MIN - 0]
        validateRanges(tokenRanges.get(instances.get(0)),
                       Arrays.asList(BigInteger.ZERO,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(101L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(1L)));

        // token(100) => [201 - 100] => [201 - MAX] [MIN - 100]
        validateRanges(tokenRanges.get(instances.get(1)),
                       Arrays.asList(BigInteger.valueOf(0L),
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(200L)));

        // token(200) => [1 - 200]
        validateRanges(tokenRanges.get(instances.get(2)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     BigInteger.valueOf(100L),
                                     BigInteger.valueOf(200L)),
                       Arrays.asList(BigInteger.valueOf(0L),
                                     BigInteger.valueOf(201L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));

        // token(1) => [102 - 1] => [102 - MAX] [MIN - 1]
        validateRanges(tokenRanges.get(instances.get(3)),
                       Arrays.asList(BigInteger.ONE,
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(200L),
                                     BigInteger.valueOf(102L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(2L)));

        // token(101) => [202 - 101] => [202 - MAX] [MIN - 101]
        validateRanges(tokenRanges.get(instances.get(4)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     Partitioner.Murmur3Partitioner.maxToken(),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(202L)),
                       Arrays.asList(Partitioner.Murmur3Partitioner.minToken(),
                                     BigInteger.valueOf(102L),
                                     BigInteger.valueOf(201L)));

        // token(201) => [2 - 201]
        validateRanges(tokenRanges.get(instances.get(5)),
                       Arrays.asList(BigInteger.valueOf(2L),
                                     BigInteger.valueOf(101L),
                                     BigInteger.valueOf(201L)),
                       Arrays.asList(BigInteger.valueOf(1L),
                                     BigInteger.valueOf(202L),
                                     Partitioner.Murmur3Partitioner.minToken(),
                                     Partitioner.Murmur3Partitioner.maxToken()));
    }

    @Test
    public void testJdkSerializationPreservesTransientReplicas() throws Exception
    {
        // CassandraRing hand-rolls readObject/writeObject and rebuilds ReplicationFactor from strategy plus
        // options, so a transient (witness) count could silently vanish on the way to a Spark executor
        CassandraRing ring = new CassandraRing(
        Partitioner.Murmur3Partitioner,
        "test",
        new ReplicationFactor(ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                              "DC1", "3/1",
                                              "DC2", "3")),
        Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                      new CassandraInstance("100", "local0-i2", "DC1"),
                      new CassandraInstance("200", "local0-i3", "DC1"),
                      new CassandraInstance("1", "local1-i1", "DC2"),
                      new CassandraInstance("101", "local1-i2", "DC2"),
                      new CassandraInstance("201", "local1-i3", "DC2")));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(ring);
        }
        CassandraRing deserialized;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = (CassandraRing) in.readObject();
        }

        ReplicationFactor rf = deserialized.replicationFactor();
        assertThat(rf.getTransientReplicas("DC1")).isEqualTo(1);
        assertThat(rf.getTransientReplicas("DC2")).isEqualTo(0);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(rf.getFullReplicationFactor()).isEqualTo(5);
        assertThat(rf).isEqualTo(ring.replicationFactor());
        // Deliberately not asserting deserialized.equals(ring): CassandraRing#equals compares the derived
        // replicas and tokenRangeMap fields, and does not hold across a JDK round trip even without transient
        // replicas. Pre-existing behaviour, unrelated to replica types.
    }

    @Test
    public void testJdkDeserializationRejectsUnknownFormatVersion() throws Exception
    {
        // The hand-rolled format gained a transient-replica section, but the class signature did not change, so the
        // computed serialVersionUID would still match an older stream. A leading format-version byte makes a stale
        // stream fail loudly instead of being misread.
        CassandraRing ring = new CassandraRing(
        Partitioner.Murmur3Partitioner,
        "test",
        new ReplicationFactor(ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                              "DC1", "3")),
        Arrays.asList(new CassandraInstance("0", "local0-i1", "DC1"),
                      new CassandraInstance("100", "local0-i2", "DC1"),
                      new CassandraInstance("200", "local0-i3", "DC1")));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(ring);
        }

        // Locate the format-version byte deterministically: the format is
        // [version][partitioner] then writeUTF(keyspace), so it sits two bytes before the "test" UTF header
        byte[] raw = bytes.toByteArray();
        byte[] keyspaceUtf = new byte[]{0, 4, 't', 'e', 's', 't'};
        int keyspaceIndex = indexOf(raw, keyspaceUtf);
        assertThat(keyspaceIndex).as("keyspace UTF header should be locatable").isGreaterThan(1);
        int versionIndex = keyspaceIndex - 2;
        // Deliberately not asserting the current version number, so this test does not need updating
        // every time the format changes
        assertThat(raw[versionIndex]).as("format version byte should be positive").isGreaterThan((byte) 0);

        // Simulate a stream written by a version whose format we do not understand
        raw[versionIndex] = (byte) 99;

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(raw)))
        {
            assertThatThrownBy(in::readObject)
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Unsupported CassandraRing serialization format version");
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle)
    {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++)
        {
            for (int j = 0; j < needle.length; j++)
            {
                if (haystack[i + j] != needle[j])
                {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    // Cassandra-reported token ranges, used for witness-enabled keyspaces where the locally
    // derived ranges cannot be trusted because the derivation ignores racks

    private static CassandraRing reportedRangeRing()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "n1", "dc1"),
                                                         new CassandraInstance("100", "n2", "dc1"),
                                                         new CassandraInstance("200", "n3", "dc1"));
        // Deliberately not what the derivation would produce: boundary at 50, and only two of three
        // replicas per range. A derived ring can never produce this shape.
        Map<Range<BigInteger>, List<CassandraInstance>> reported = new LinkedHashMap<>();
        reported.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.valueOf(50L)),
                     Arrays.asList(instances.get(0), instances.get(1)));
        reported.put(Range.openClosed(BigInteger.valueOf(50L), Partitioner.Murmur3Partitioner.maxToken()),
                     Arrays.asList(instances.get(1), instances.get(2)));
        return new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                 new ReplicationFactor(ImmutableMap.of(
                                 "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                 "dc1", "3/1")),
                                 instances, reported);
    }

    private static Set<String> replicaNamesAt(CassandraRing ring, long token)
    {
        Set<String> names = new TreeSet<>();
        ring.getReplicas(BigInteger.valueOf(token)).forEach(instance -> names.add(instance.nodeName()));
        return names;
    }

    @Test
    public void testReportedRangesAreHonouredInsteadOfDerived()
    {
        CassandraRing reportedRing = reportedRangeRing();
        assertThat(reportedRing.hasReportedRanges()).isTrue();
        // the supplied boundary at 50 is used, and only the supplied replicas are returned
        assertThat(replicaNamesAt(reportedRing, 10L)).containsExactly("n1", "n2");
        assertThat(replicaNamesAt(reportedRing, 1000L)).containsExactly("n2", "n3");
    }

    @Test
    public void testDerivedRingIsUnchangedByReportedRangeSupport()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "n1", "dc1"),
                                                         new CassandraInstance("100", "n2", "dc1"),
                                                         new CassandraInstance("200", "n3", "dc1"));
        CassandraRing derived = new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                  new ReplicationFactor(ImmutableMap.of(
                                                  "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                  "dc1", "3")),
                                                  instances);
        assertThat(derived.hasReportedRanges()).isFalse();
        // derivation puts boundaries at node tokens, so 50 is not a boundary
        Set<BigInteger> upperBounds = new TreeSet<>();
        derived.rangeMap().asMapOfRanges().forEach((range, replicas) -> {
            if (!replicas.isEmpty())
            {
                upperBounds.add(range.upperEndpoint());
            }
        });
        assertThat(upperBounds).doesNotContain(BigInteger.valueOf(50L));
    }

    @Test
    public void testReportedRangesSurviveJdkSerialization() throws Exception
    {
        CassandraRing original = reportedRangeRing();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(original);
        }
        CassandraRing deserialized;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = (CassandraRing) in.readObject();
        }
        assertThat(deserialized.hasReportedRanges()).isTrue();
        assertThat(replicaNamesAt(deserialized, 10L)).containsExactly("n1", "n2");
        assertThat(replicaNamesAt(deserialized, 1000L)).containsExactly("n2", "n3");
    }

    @Test
    public void testReportedRangesSurviveKryoSerialization()
    {
        CassandraRing original = reportedRangeRing();
        Kryo kryo = new Kryo();
        kryo.register(CassandraRing.class, new CassandraRing.Serializer());
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        kryo.register(CassandraInstance.class, new CassandraInstance.Serializer());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, original);
        }
        CassandraRing deserialized;
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = kryo.readObject(in, CassandraRing.class);
        }
        assertThat(deserialized.hasReportedRanges()).isTrue();
        assertThat(replicaNamesAt(deserialized, 10L)).containsExactly("n1", "n2");
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testDerivedRingStaysDerivedThroughKryo()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "n1", "dc1"),
                                                         new CassandraInstance("100", "n2", "dc1"),
                                                         new CassandraInstance("200", "n3", "dc1"));
        CassandraRing derived = new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                  new ReplicationFactor(ImmutableMap.of(
                                                  "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                  "dc1", "3")),
                                                  instances);
        Kryo kryo = new Kryo();
        kryo.register(CassandraRing.class, new CassandraRing.Serializer());
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        kryo.register(CassandraInstance.class, new CassandraInstance.Serializer());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, derived);
        }
        CassandraRing deserialized;
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = kryo.readObject(in, CassandraRing.class);
        }
        assertThat(deserialized.hasReportedRanges()).isFalse();
        assertThat(deserialized).isEqualTo(derived);
    }

    @Test
    public void testReportedRangeWithUnknownReplicaIsRejected()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "n1", "dc1"),
                                                         new CassandraInstance("100", "n2", "dc1"));
        Map<Range<BigInteger>, List<CassandraInstance>> reported = new LinkedHashMap<>();
        reported.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)),
                     Arrays.asList(new CassandraInstance("999", "nope", "dc1")));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                   new ReplicationFactor(ImmutableMap.of(
                                                   "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                   "dc1", "2")),
                                                   instances, reported))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not present in the ring instances");
    }

    private static CassandraRing reportedRing(Map<Range<BigInteger>, List<CassandraInstance>> reported,
                                              List<CassandraInstance> instances)
    {
        return new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                 new ReplicationFactor(ImmutableMap.of(
                                 "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                 "dc1", "3")),
                                 instances, reported);
    }

    @Test
    public void testReportedRingDoesNotDependOnReplicaOrder()
    {
        // Sidecar lists replicas in ring-walk order, while a deserialized ring lists them in instance order, so the
        // ring must not depend on the order replicas were supplied in
        List<CassandraInstance> instances = new ArrayList<>();
        for (int node = 0; node < 32; node++)
        {
            instances.add(new CassandraInstance(String.valueOf(node * 100), "n" + node, "dc1"));
        }
        Map<Range<BigInteger>, List<CassandraInstance>> forward = new LinkedHashMap<>();
        Map<Range<BigInteger>, List<CassandraInstance>> backward = new LinkedHashMap<>();
        for (int node = 0; node < instances.size(); node++)
        {
            BigInteger lower = node == 0 ? Partitioner.Murmur3Partitioner.minToken() : BigInteger.valueOf((node - 1) * 100L + 1);
            BigInteger upper = node == instances.size() - 1 ? Partitioner.Murmur3Partitioner.maxToken() : BigInteger.valueOf(node * 100L);
            List<CassandraInstance> replicas = Arrays.asList(instances.get(node),
                                                             instances.get((node + 1) % instances.size()),
                                                             instances.get((node + 2) % instances.size()));
            List<CassandraInstance> reversed = new ArrayList<>(replicas);
            Collections.reverse(reversed);
            forward.put(Range.openClosed(lower, upper), replicas);
            backward.put(Range.openClosed(lower, upper), reversed);
        }

        CassandraRing ring = reportedRing(forward, instances);
        CassandraRing reversedRing = reportedRing(backward, instances);
        assertThat(reversedRing).isEqualTo(ring);
        // whatever order they were supplied in, each range lists its replicas in token order
        reversedRing.rangeMap().asMapOfRanges().values().forEach(replicas -> {
            List<CassandraInstance> sorted = new ArrayList<>(replicas);
            sorted.sort((left, right) -> new BigInteger(left.token()).compareTo(new BigInteger(right.token())));
            assertThat(replicas).containsExactlyElementsOf(sorted);
        });

        Kryo kryo = new Kryo();
        kryo.register(CassandraRing.class, new CassandraRing.Serializer());
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        kryo.register(CassandraInstance.class, new CassandraInstance.Serializer());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, ring);
        }
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            assertThat(kryo.readObject(in, CassandraRing.class)).isEqualTo(ring);
        }
    }

    @Test
    public void testReportedRangesAreNormalisedToOpenClosed()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "n1", "dc1"),
                                                         new CassandraInstance("100", "n2", "dc1"));
        Map<Range<BigInteger>, List<CassandraInstance>> reported = new LinkedHashMap<>();
        reported.put(Range.closed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO),
                     Arrays.asList(instances.get(0), instances.get(1)));
        reported.put(Range.openClosed(BigInteger.ZERO, Partitioner.Murmur3Partitioner.maxToken()),
                     Arrays.asList(instances.get(0), instances.get(1)));

        CassandraRing ring = reportedRing(reported, instances);

        assertThat(ring.tokenRanges().get(instances.get(0)))
        .containsExactly(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO),
                         Range.openClosed(BigInteger.ZERO, Partitioner.Murmur3Partitioner.maxToken()));
    }

    // Witness replicas: Cassandra-reported ranges that also identify which replica of each range is a witness

    private static final List<CassandraInstance> WITNESS_RING_INSTANCES = Arrays.asList(
    new CassandraInstance("0", "n1", "dc1"),
    new CassandraInstance("100", "n2", "dc1"),
    new CassandraInstance("200", "n3", "dc1"));

    private static Map<Range<BigInteger>, List<CassandraInstance>> witnessRingReplicas()
    {
        CassandraInstance n1 = WITNESS_RING_INSTANCES.get(0);
        CassandraInstance n2 = WITNESS_RING_INSTANCES.get(1);
        CassandraInstance n3 = WITNESS_RING_INSTANCES.get(2);
        Map<Range<BigInteger>, List<CassandraInstance>> replicas = new LinkedHashMap<>();
        replicas.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO), Arrays.asList(n1, n2, n3));
        replicas.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)), Arrays.asList(n2, n3, n1));
        replicas.put(Range.openClosed(BigInteger.valueOf(100L), BigInteger.valueOf(200L)), Arrays.asList(n3, n1, n2));
        replicas.put(Range.openClosed(BigInteger.valueOf(200L), Partitioner.Murmur3Partitioner.maxToken()), Arrays.asList(n1, n2, n3));
        return replicas;
    }

    /**
     * Three nodes with {@code '3/1'}: every node replicates every range, and is the witness for some of them, which is
     * why witness status cannot be a per-node property.
     */
    private static CassandraRing witnessRing()
    {
        CassandraInstance n1 = WITNESS_RING_INSTANCES.get(0);
        CassandraInstance n2 = WITNESS_RING_INSTANCES.get(1);
        CassandraInstance n3 = WITNESS_RING_INSTANCES.get(2);
        Map<Range<BigInteger>, List<CassandraInstance>> witnesses = new LinkedHashMap<>();
        witnesses.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO), Collections.singletonList(n3));
        witnesses.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)), Collections.singletonList(n1));
        witnesses.put(Range.openClosed(BigInteger.valueOf(100L), BigInteger.valueOf(200L)), Collections.singletonList(n2));
        witnesses.put(Range.openClosed(BigInteger.valueOf(200L), Partitioner.Murmur3Partitioner.maxToken()), Collections.singletonList(n3));
        return new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                 new ReplicationFactor(ImmutableMap.of(
                                 "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                 "dc1", "3/1")),
                                 WITNESS_RING_INSTANCES, witnessRingReplicas(), witnesses);
    }

    private static Set<String> fullReplicaNamesAt(CassandraRing ring, long token)
    {
        Set<String> names = new TreeSet<>();
        Range<BigInteger> point = Range.closed(BigInteger.valueOf(token), BigInteger.valueOf(token));
        ring.getFullReplicaSubRanges(point).asMapOfRanges().values()
            .forEach(replicas -> replicas.forEach(instance -> names.add(instance.nodeName())));
        return names;
    }

    private static void assertWitnessesExcluded(CassandraRing ring)
    {
        assertThat(ring.hasWitnesses()).isTrue();
        assertThat(fullReplicaNamesAt(ring, -10L)).containsExactly("n1", "n2");
        assertThat(fullReplicaNamesAt(ring, 50L)).containsExactly("n2", "n3");
        assertThat(fullReplicaNamesAt(ring, 150L)).containsExactly("n1", "n3");
        assertThat(fullReplicaNamesAt(ring, 1000L)).containsExactly("n1", "n2");
    }

    @Test
    public void testWitnessesAreExcludedFromFullReplicas()
    {
        assertWitnessesExcluded(witnessRing());
    }

    @Test
    public void testWitnessesRemainInPlacement()
    {
        // getSubRanges describes placement, which still includes the witness
        CassandraRing ring = witnessRing();
        assertThat(replicaNamesAt(ring, 50L)).containsExactly("n1", "n2", "n3");
        assertThat(ring.tokenRanges().get(WITNESS_RING_INSTANCES.get(0))).hasSize(4);
    }

    @Test
    public void testFullReplicasSpanningSeveralRangesExcludeEachRangesOwnWitness()
    {
        // A Spark partition usually covers more than one Cassandra range, each with a different witness
        Range<BigInteger> partition = Range.openClosed(BigInteger.valueOf(-10L), BigInteger.valueOf(150L));
        Map<Range<BigInteger>, List<CassandraInstance>> subRanges = witnessRing().getFullReplicaSubRanges(partition)
                                                                                 .asMapOfRanges();
        Map<BigInteger, Set<String>> namesByUpperBound = new TreeMap<>();
        subRanges.forEach((range, replicas) -> namesByUpperBound.put(range.upperEndpoint(),
                                                                     replicas.stream()
                                                                             .map(CassandraInstance::nodeName)
                                                                             .collect(Collectors.toCollection(TreeSet::new))));
        assertThat(namesByUpperBound).containsOnly(entry(BigInteger.ZERO, new TreeSet<>(Arrays.asList("n1", "n2"))),
                                                   entry(BigInteger.valueOf(100L), new TreeSet<>(Arrays.asList("n2", "n3"))),
                                                   entry(BigInteger.valueOf(150L), new TreeSet<>(Arrays.asList("n1", "n3"))));
    }

    @Test
    public void testRingWithoutWitnessesReturnsEveryReplicaAsFull()
    {
        CassandraRing ring = reportedRangeRing();
        assertThat(ring.hasWitnesses()).isFalse();
        assertThat(fullReplicaNamesAt(ring, 10L)).containsExactly("n1", "n2");
        assertThat(fullReplicaNamesAt(ring, 1000L)).containsExactly("n2", "n3");
    }

    @Test
    public void testDerivedRingHasNoWitnesses()
    {
        CassandraRing derived = new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                  new ReplicationFactor(ImmutableMap.of(
                                                  "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                  "dc1", "3")),
                                                  WITNESS_RING_INSTANCES);
        assertThat(derived.hasWitnesses()).isFalse();
        assertThat(derived.getFullReplicaSubRanges(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                                                    Partitioner.Murmur3Partitioner.maxToken())))
        .isEqualTo(derived.getSubRanges(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                                         Partitioner.Murmur3Partitioner.maxToken())));
    }

    @Test
    public void testWitnessesSurviveJdkSerialization() throws Exception
    {
        CassandraRing original = witnessRing();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(original);
        }
        CassandraRing deserialized;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = (CassandraRing) in.readObject();
        }
        assertWitnessesExcluded(deserialized);
    }

    @Test
    public void testWitnessesSurviveKryoSerialization()
    {
        CassandraRing original = witnessRing();
        Kryo kryo = new Kryo();
        kryo.register(CassandraRing.class, new CassandraRing.Serializer());
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        kryo.register(CassandraInstance.class, new CassandraInstance.Serializer());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (Output out = new Output(bytes))
        {
            kryo.writeObject(out, original);
        }
        CassandraRing deserialized;
        try (Input in = new Input(new ByteArrayInputStream(bytes.toByteArray())))
        {
            deserialized = kryo.readObject(in, CassandraRing.class);
        }
        assertWitnessesExcluded(deserialized);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testWitnessThatIsNotAReplicaIsRejected()
    {
        Map<Range<BigInteger>, List<CassandraInstance>> replicas = new LinkedHashMap<>();
        replicas.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)),
                     Arrays.asList(WITNESS_RING_INSTANCES.get(0), WITNESS_RING_INSTANCES.get(1)));
        Map<Range<BigInteger>, List<CassandraInstance>> witnesses = new LinkedHashMap<>();
        witnesses.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L)),
                      Collections.singletonList(WITNESS_RING_INSTANCES.get(2)));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                   new ReplicationFactor(ImmutableMap.of(
                                                   "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                   "dc1", "3/1")),
                                                   WITNESS_RING_INSTANCES, replicas, witnesses))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not one of its replicas");
    }

    @Test
    public void testWitnessForUnknownRangeIsRejected()
    {
        Map<Range<BigInteger>, List<CassandraInstance>> witnesses = new LinkedHashMap<>();
        witnesses.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(50L)),
                      Collections.singletonList(WITNESS_RING_INSTANCES.get(0)));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                   new ReplicationFactor(ImmutableMap.of(
                                                   "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                   "dc1", "3/1")),
                                                   WITNESS_RING_INSTANCES, witnessRingReplicas(), witnesses))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("which has no replicas");
    }

    @Test
    public void testWitnessesRequireReportedRanges()
    {
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                                   new ReplicationFactor(ImmutableMap.of(
                                                   "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                   "dc1", "3/1")),
                                                   WITNESS_RING_INSTANCES, null, Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cassandra-reported ranges");
    }
}
