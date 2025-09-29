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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;

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

    private Map.Entry<Range<BigInteger>, List<String>> replicaEntry(String startToken, String endToken, List<String> replicas)
    {
        Range<BigInteger> range = Range.openClosed(new BigInteger(startToken), new BigInteger(endToken));
        return Map.entry(range, replicas);
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
    public void testNetworkStrategyRF3Tokens1WithRangeToReplicas()
    {
        List<CassandraInstance> instances = Arrays.asList(
                new CassandraInstance("0", "local0-i1", "DC1"),
                new CassandraInstance("100", "local0-i2", "DC1"),
                new CassandraInstance(Partitioner.Murmur3Partitioner.maxToken().toString(), "local0-i3", "DC1"));

        List<String> replicas = List.of("local0-i1", "local0-i2", "local0-i3");
        Map<Range<BigInteger>, List<String>> rangeToReplicas = Map.ofEntries(
                replicaEntry(Partitioner.Murmur3Partitioner.minToken().toString(), "0",  replicas),
                replicaEntry("0", "100",  replicas),
                replicaEntry("100", Partitioner.Murmur3Partitioner.maxToken().toString(),  replicas)
        );

        CassandraRing ring = new CassandraRing(
                Partitioner.Murmur3Partitioner,
                "test",
                new ReplicationFactor(
                        ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "DC1", "3")),
                instances,
                rangeToReplicas);

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(
                BigInteger.valueOf(0L),
                BigInteger.valueOf(100L),
                Partitioner.Murmur3Partitioner.maxToken()).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();
        for (CassandraInstance instance : instances)
        {
            assertThat(mergeRanges(tokenRanges.get(instance)))
                    .isEqualTo(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                            Partitioner.Murmur3Partitioner.maxToken()));
        }
    }

    @Test
    public void testNetworkStrategyRF3Tokens4WithRangeToReplicas()
    {
        List<CassandraInstance> instances = Arrays.asList(
                new CassandraInstance("-8000", "local0-i1", "DC1"),
                new CassandraInstance("-2000", "local0-i1", "DC1"),
                new CassandraInstance("2000", "local0-i1", "DC1"),
                new CassandraInstance("8000", "local0-i1", "DC1"),
                new CassandraInstance("-6000", "local0-i2", "DC1"),
                new CassandraInstance("-1000", "local0-i2", "DC1"),
                new CassandraInstance("4000", "local0-i2", "DC1"),
                new CassandraInstance("9000", "local0-i2", "DC1"),
                new CassandraInstance("-4000", "local0-i3", "DC1"),
                new CassandraInstance("-5", "local0-i3", "DC1"),
                new CassandraInstance("3050", "local0-i3", "DC1"),
                new CassandraInstance("10000", "local0-i3", "DC1"));

        List<String> replicas = List.of("local0-i1", "local0-i2", "local0-i3");
        Map<Range<BigInteger>, List<String>> rangeToReplicas = Map.ofEntries(
                replicaEntry(Partitioner.Murmur3Partitioner.minToken().toString(), "-8000",  replicas),
                replicaEntry("-8000", "-6000",  replicas),
                replicaEntry("-6000", "-4000",  replicas),
                replicaEntry("-4000", "-2000",  replicas),
                replicaEntry("-2000", "-1000",  replicas),
                replicaEntry("-1000", "-5",  replicas),
                replicaEntry("-5", "2000",  replicas),
                replicaEntry("2000", "3050",  replicas),
                replicaEntry("3050", "4000",  replicas),
                replicaEntry("4000", "8000",  replicas),
                replicaEntry("8000", "9000",  replicas),
                replicaEntry("9000", "10000",  replicas),
                replicaEntry("10000", Partitioner.Murmur3Partitioner.maxToken().toString(),  replicas)
        );

        CassandraRing ring = new CassandraRing(
                Partitioner.Murmur3Partitioner,
                "test",
                new ReplicationFactor(
                        ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "DC1", "3")),
                instances,
                rangeToReplicas);

        assertThat(ring.tokens().toArray()).isEqualTo(Arrays.asList(
                BigInteger.valueOf(-8000L), BigInteger.valueOf(-6000L), BigInteger.valueOf(-4000L),
                BigInteger.valueOf(-2000L), BigInteger.valueOf(-1000L), BigInteger.valueOf(-5L),
                BigInteger.valueOf(2000L), BigInteger.valueOf(3050L), BigInteger.valueOf(4000L),
                BigInteger.valueOf(8000L), BigInteger.valueOf(9000L), BigInteger.valueOf(10000L)).toArray());

        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ring.tokenRanges();

        // token(0) (-8000) => (MIN -> -8000], (8000 -> 9000], (9000 -> 10000], (10000 -> MAX]
        //                  => (MIN -> -8000], (8000 -> MAX]
        validateRanges(tokenRanges.get(instances.get(0)),
                Arrays.asList(BigInteger.valueOf(-8000), Partitioner.Murmur3Partitioner.maxToken()),
                Arrays.asList(Partitioner.Murmur3Partitioner.minToken(), BigInteger.valueOf(8000)));

        // token(2) (-2000) => (-8000 -> -6000], (-6000 -> -4000], (-4000 -> -2000]
        //                  => (-8000 -> -2000]
        validateRanges(tokenRanges.get(instances.get(1)),
                Arrays.asList(BigInteger.valueOf(-2000)),
                Arrays.asList(BigInteger.valueOf(-8000)));

        // token(3) (2000) => (-2000 -> -1000], (-1000 -> -5], (-5 -> 2000]
        //                 => (-2000 -> 2000]
        validateRanges(tokenRanges.get(instances.get(2)),
                Arrays.asList(BigInteger.valueOf(2000)),
                Arrays.asList(BigInteger.valueOf(-2000)));

        // token(4) (8000) => (2000 -> 3050], (3050 -> 4000], (4000 -> 8000]
        //                 => (2000 -> 8000]
        validateRanges(tokenRanges.get(instances.get(3)),
                Arrays.asList(BigInteger.valueOf(8000)),
                Arrays.asList(BigInteger.valueOf(2000)));

        // token(5) (-6000) => (MIN -> -8000], (-8000 -> -6000], (9000 -> 10000], (10000 -> MAX]
        //                 => (MIN -> -6000], (9000 -> MAX)
        validateRanges(tokenRanges.get(instances.get(4)),
                Arrays.asList(BigInteger.valueOf(-6000), Partitioner.Murmur3Partitioner.maxToken()),
                Arrays.asList(Partitioner.Murmur3Partitioner.minToken(), BigInteger.valueOf(9000)));

        // token(6) (-1000) => (-6000 -> -4000], (-4000 -> -2000], (-2000 -> -1000]
        //                 => (-6000 -> -1000]
        validateRanges(tokenRanges.get(instances.get(5)),
                Arrays.asList(BigInteger.valueOf(-1000)),
                Arrays.asList(BigInteger.valueOf(-6000)));

        // token(7) (4000) => (-1000 -> -5], (-5 -> 2000], (2000 -> 3050], (3050 -> 4000]
        //                 => (-1000 -> 4000]
        validateRanges(tokenRanges.get(instances.get(6)),
                Arrays.asList(BigInteger.valueOf(4000)),
                Arrays.asList(BigInteger.valueOf(-1000)));

        // token(8) (9000) => (4000 -> 8000], (8000 -> 9000]
        //                 => (4000 -> 9000]
        validateRanges(tokenRanges.get(instances.get(7)),
                Arrays.asList(BigInteger.valueOf(9000)),
                Arrays.asList(BigInteger.valueOf(4000)));

        // token(9) (-4000) => (MIN -> -8000], (-8000 -> -6000], (-6000 -> -4000], (10000 -> MAX]
        //                 => (MIN -> -4000], (10000 -> -MAX]
        validateRanges(tokenRanges.get(instances.get(8)),
                Arrays.asList(BigInteger.valueOf(-4000), Partitioner.Murmur3Partitioner.maxToken()),
                Arrays.asList(Partitioner.Murmur3Partitioner.minToken(), BigInteger.valueOf(10000)));

        // token(10) (-5) => (-4000 -> -2000], (-2000 -> -1000], (-1000 -> -5]
        //                 => (-4000 -> -5]
        validateRanges(tokenRanges.get(instances.get(9)),
                Arrays.asList(BigInteger.valueOf(-5)),
                Arrays.asList(BigInteger.valueOf(-4000)));

        // token(11) (3050) => (-5 -> 2000], (2000 -> 3050]
        //                 => (-5 -> 3050]
        validateRanges(tokenRanges.get(instances.get(10)),
                Arrays.asList(BigInteger.valueOf(3050)),
                Arrays.asList(BigInteger.valueOf(-5)));

        // token(12) (10000) => (3050 -> 4000], (4000 -> 8000], (8000 -> 9000], (9000 -> 10000]
        //                 => (3050 -> 10000]
        validateRanges(tokenRanges.get(instances.get(11)),
                Arrays.asList(BigInteger.valueOf(10000)),
                Arrays.asList(BigInteger.valueOf(3050)));
    }
}
