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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        enclosedTokens.forEach(token -> assertTrue(rangeSet.contains(token), token + " should have been a valid token"));
        excludedTokens.forEach(token -> assertFalse(rangeSet.contains(token)));
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                 BigInteger.valueOf(100L),
                                                                 BigInteger.valueOf(200L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();
        for (CassandraInstance instance : instances)
        {
            assertEquals(mergeRanges(tokenRanges.get(instance)),
                         Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                 BigInteger.valueOf(1L),
                                                                 BigInteger.valueOf(100L),
                                                                 BigInteger.valueOf(101L),
                                                                 BigInteger.valueOf(200L),
                                                                 BigInteger.valueOf(201L)).toArray());

        assertArrayEquals(ring.tokens("DC1").toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                      BigInteger.valueOf(100L),
                                                                      BigInteger.valueOf(200L)).toArray());

        assertArrayEquals(ring.tokens("DC2").toArray(), Arrays.asList(BigInteger.valueOf(1L),
                                                                      BigInteger.valueOf(101L),
                                                                      BigInteger.valueOf(201L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();
        for (CassandraInstance instance : instances)
        {
            assertEquals(mergeRanges(tokenRanges.get(instance)),
                         Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                 BigInteger.valueOf(1L),
                                                                 BigInteger.valueOf(100L),
                                                                 BigInteger.valueOf(101L),
                                                                 BigInteger.valueOf(200L),
                                                                 BigInteger.valueOf(201L)).toArray());

        assertArrayEquals(ring.tokens("DC1").toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                      BigInteger.valueOf(100L),
                                                                      BigInteger.valueOf(200L)).toArray());

        assertArrayEquals(ring.tokens("DC2").toArray(), Arrays.asList(BigInteger.valueOf(1L),
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

        assertArrayEquals(ring.tokens().toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                 BigInteger.valueOf(1L),
                                                                 BigInteger.valueOf(100L),
                                                                 BigInteger.valueOf(101L),
                                                                 BigInteger.valueOf(200L),
                                                                 BigInteger.valueOf(201L)).toArray());

        assertArrayEquals(ring.tokens("DC1").toArray(), Arrays.asList(BigInteger.valueOf(0L),
                                                                      BigInteger.valueOf(100L),
                                                                      BigInteger.valueOf(200L)).toArray());

        assertArrayEquals(ring.tokens("DC2").toArray(), Arrays.asList(BigInteger.valueOf(1L),
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
}
