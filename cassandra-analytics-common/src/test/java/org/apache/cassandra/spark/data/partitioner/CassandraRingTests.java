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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ---------- 5-arg authoritative-replica constructor ----------

    private static ReplicationFactor ntsRf33()
    {
        return new ReplicationFactor(ImmutableMap.of(
            "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
            "DC1", "3",
            "DC2", "3"));
    }

    private static List<CassandraInstance> twoDcSixInstances()
    {
        return new ArrayList<>(Arrays.asList(
            new CassandraInstance("0",   "dc1-a", "DC1"),
            new CassandraInstance("100", "dc1-b", "DC1"),
            new CassandraInstance("200", "dc1-c", "DC1"),
            new CassandraInstance("1",   "dc2-a", "DC2"),
            new CassandraInstance("101", "dc2-b", "DC2"),
            new CassandraInstance("201", "dc2-c", "DC2")));
    }

    /**
     * Build a single-range full-ring authoritative mapping where every supplied instance is a
     * replica. Useful for tests where the per-range placement is uninteresting but full-ring
     * coverage must be satisfied.
     */
    private static RangeMap<BigInteger, List<CassandraInstance>> fullRingAllReplicas(List<CassandraInstance> instances)
    {
        RangeMap<BigInteger, List<CassandraInstance>> rm = TreeRangeMap.create();
        rm.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                Partitioner.Murmur3Partitioner.maxToken()),
               new ArrayList<>(instances));
        return rm;
    }

    @Test
    public void testAuthoritativeCtorReplicasAdoptedVerbatim()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        BigInteger minToken = Partitioner.Murmur3Partitioner.minToken();
        BigInteger maxToken = Partitioner.Murmur3Partitioner.maxToken();
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        Range<BigInteger> lowHalf = Range.openClosed(minToken, BigInteger.ZERO);
        Range<BigInteger> highHalf = Range.openClosed(BigInteger.ZERO, maxToken);
        List<CassandraInstance> lowReplicas = Arrays.asList(instances.get(0), instances.get(3));
        List<CassandraInstance> highReplicas = Arrays.asList(instances.get(1), instances.get(2),
                                                             instances.get(4), instances.get(5));
        auth.put(lowHalf, lowReplicas);
        auth.put(highHalf, highReplicas);

        CassandraRing ring = new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth);

        // Exact map equality - the supplied mapping IS what rangeMap() returns.
        assertThat(ring.rangeMap().asMapOfRanges()).isEqualTo(auth.asMapOfRanges());

        // Instances sorted by token.
        assertThat(new ArrayList<>(ring.instances()).stream().map(CassandraInstance::token))
            .containsExactly("0", "1", "100", "101", "200", "201");

        // tokens() and tokens(dc) still derive from instances correctly.
        assertThat(ring.tokens(Partitioner.Murmur3Partitioner.name())).isNotNull();
        assertThat(ring.tokens("DC1")).containsExactlyInAnyOrder(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));

        // tokenRangeMap (multimap inverse) must list every instance against the range it was assigned to.
        Multimap<CassandraInstance, Range<BigInteger>> inverse = ring.tokenRanges();
        for (CassandraInstance i : lowReplicas)
        {
            assertThat(inverse.get(i)).contains(lowHalf);
        }
        for (CassandraInstance i : highReplicas)
        {
            assertThat(inverse.get(i)).contains(highHalf);
        }
    }

    @Test
    public void testAuthoritativeCtorRejectsNullMap()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(),
                                                   instances, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAuthoritativeCtorRejectsEmptyReplicaList()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        auth.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                  Partitioner.Murmur3Partitioner.maxToken()),
                 Collections.emptyList());
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-null and non-empty");
    }

    @Test
    public void testAuthoritativeCtorRejectsUnknownReplica()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        CassandraInstance stranger = new CassandraInstance("9999", "ghost", "DC1");
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        auth.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(),
                                  Partitioner.Murmur3Partitioner.maxToken()),
                 Arrays.asList(instances.get(0), stranger));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not in the supplied instances collection");
    }

    @Test
    public void testAuthoritativeCtorRejectsRangeGap()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        BigInteger minToken = Partitioner.Murmur3Partitioner.minToken();
        BigInteger maxToken = Partitioner.Murmur3Partitioner.maxToken();
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        // Leave a gap between -10 and +10
        auth.put(Range.openClosed(minToken, BigInteger.valueOf(-10L)),
                 new ArrayList<>(instances));
        auth.put(Range.openClosed(BigInteger.valueOf(10L), maxToken),
                 new ArrayList<>(instances));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("gap");
    }

    @Test
    public void testAuthoritativeCtorRejectsPartialCoverageMissingTail()
    {
        // Guava's TreeRangeMap.put auto-overlap-resolves; non-overlapping partial coverage
        // (missing the head or tail) is the relevant failure mode here.
        List<CassandraInstance> instances = twoDcSixInstances();
        BigInteger minToken = Partitioner.Murmur3Partitioner.minToken();
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        auth.put(Range.openClosed(minToken, BigInteger.valueOf(0L)),
                 new ArrayList<>(instances));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxToken");
    }

    @Test
    public void testAuthoritativeCtorRejectsMissingHead()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        BigInteger maxToken = Partitioner.Murmur3Partitioner.maxToken();
        RangeMap<BigInteger, List<CassandraInstance>> auth = TreeRangeMap.create();
        auth.put(Range.openClosed(BigInteger.valueOf(0L), maxToken),
                 new ArrayList<>(instances));
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("minToken");
    }

    @Test
    public void testAuthoritativeCtorRejectsEmptyMap()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        RangeMap<BigInteger, List<CassandraInstance>> empty = TreeRangeMap.create();
        assertThatThrownBy(() -> new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, empty))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAuthoritativeAndNaiveRingsAreNotEqualForDifferentPlacement()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        CassandraRing naive = new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances);
        // Authoritative: all 6 instances replicate the full ring (very different from RF=3-per-DC naive placement)
        CassandraRing authoritative = new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(),
                                                        instances, fullRingAllReplicas(instances));
        assertThat(authoritative).isNotEqualTo(naive);
        assertThat(authoritative.hashCode()).isNotEqualTo(naive.hashCode());
    }

    @Test
    public void testAuthoritativeCtorRoundTripsEqual()
    {
        List<CassandraInstance> instances = twoDcSixInstances();
        RangeMap<BigInteger, List<CassandraInstance>> auth = fullRingAllReplicas(instances);
        CassandraRing a = new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth);
        CassandraRing b = new CassandraRing(Partitioner.Murmur3Partitioner, "ks", ntsRf33(), instances, auth);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
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
}
