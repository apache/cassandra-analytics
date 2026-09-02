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
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.RangeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenPartitionerValidationTest
{
    private static final Partitioner PARTITIONER = Partitioner.Murmur3Partitioner;

    @Test
    public void testValidationDetectsRangeGap()
    {
        List<Range<BigInteger>> subRanges = RangeUtils.split(wholeRing(), 4);

        assertThatThrownBy(() -> new TokenPartitioner(withGapAt(subRanges, 2), ring()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("There should be no missing ranges")
        .hasMessageContaining(gapPunchedInto(subRanges.get(2)).toString());
    }

    @Test
    public void testValidationDetectsRangeGapAtRingLowerEdge()
    {
        // Guards the bound type at minToken from both sides: minToken itself is owned by no sub-range and must not
        // be reported, yet a gap starting immediately above it must still be caught. The gap is punched into the
        // first sub-range rather than dropping it, so that the partition count stays put and validateMapSizes
        // cannot fail first with an unrelated message.
        List<Range<BigInteger>> subRanges = RangeUtils.split(wholeRing(), 4);

        assertThatThrownBy(() -> new TokenPartitioner(withGapAt(subRanges, 0), ring()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("There should be no missing ranges")
        .hasMessageContaining(gapPunchedInto(subRanges.get(0)).toString());
    }

    @Test
    public void testValidationAcceptsCompleteRangeCoverage()
    {
        // minToken is deliberately left uncovered: the sub-ranges are open-closed, so it belongs to none of them.
        // Validation must not report it as a gap, otherwise every job fails on a healthy ring.
        TokenPartitioner partitioner = new TokenPartitioner(RangeUtils.split(wholeRing(), 4), ring());
        assertThat(partitioner.numPartitions()).isEqualTo(4);
    }

    private static Range<BigInteger> wholeRing()
    {
        return Range.openClosed(PARTITIONER.minToken(), PARTITIONER.maxToken());
    }

    /**
     * Punches a real, non-empty gap into the sub-range at {@code gapIndex} by moving its lower endpoint up, so that
     * the returned ranges leave exactly {@link #gapPunchedInto} uncovered.
     */
    private static List<Range<BigInteger>> withGapAt(List<Range<BigInteger>> gapFreeRanges, int gapIndex)
    {
        List<Range<BigInteger>> ranges = new ArrayList<>(gapFreeRanges);
        Range<BigInteger> covered = ranges.get(gapIndex);
        ranges.set(gapIndex, Range.openClosed(gapPunchedInto(covered).upperEndpoint(), covered.upperEndpoint()));
        return ranges;
    }

    /**
     * @return the sub-range that {@link #withGapAt} leaves uncovered when it punches a gap into {@code range}
     */
    private static Range<BigInteger> gapPunchedInto(Range<BigInteger> range)
    {
        return Range.openClosed(range.lowerEndpoint(), range.lowerEndpoint().add(BigInteger.TEN));
    }

    private static CassandraRing ring()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DEV"),
                                                          new CassandraInstance("100", "local0-i2", "DEV"),
                                                          new CassandraInstance("200", "local0-i3", "DEV"));
        return new CassandraRing(PARTITIONER,
                                 "test",
                                 new ReplicationFactor(ImmutableMap.of("class", "NetworkTopologyStrategy", "DEV", "3")),
                                 instances);
    }
}
