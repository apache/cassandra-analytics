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

package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.BoundType;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeUtilsTest
{
    private static final Pattern RANGE_PATTERN = Pattern.compile("^[\\\\(|\\[](-?\\d+),(-?\\d+)[\\\\)|\\]]$");

    @Test
    void testCalculateTokenRangesTenNodesRF10()
    {
        assertTokenRanges(10, 10,
                          new String[]{"(-9223372036854775808,9223372036854775807]"},
                          new String[]{"(-7378697629483820647,9223372036854775807]", "(-9223372036854775808,-7378697629483820647]"},
                          new String[]{"(-5534023222112865486,9223372036854775807]", "(-9223372036854775808,-5534023222112865486]"},
                          new String[]{"(-3689348814741910325,9223372036854775807]", "(-9223372036854775808,-3689348814741910325]"},
                          new String[]{"(-1844674407370955164,9223372036854775807]", "(-9223372036854775808,-1844674407370955164]"},
                          new String[]{"(-3,9223372036854775807]", "(-9223372036854775808,-3]"},
                          new String[]{"(1844674407370955158,9223372036854775807]", "(-9223372036854775808,1844674407370955158]"},
                          new String[]{"(3689348814741910319,9223372036854775807]", "(-9223372036854775808,3689348814741910319]"},
                          new String[]{"(5534023222112865480,9223372036854775807]", "(-9223372036854775808,5534023222112865480]"},
                          new String[]{"(7378697629483820641,9223372036854775807]", "(-9223372036854775808,7378697629483820641]"});
    }

    @Test
    void testCalculateTokenRangesTenNodesRF7()
    {
        assertTokenRanges(10, 7,
                          new String[]{"(-3689348814741910325,9223372036854775807]"},
                          new String[]{"(-1844674407370955164,9223372036854775807]", "(-9223372036854775808,-7378697629483820647]"},
                          new String[]{"(-3,9223372036854775807]", "(-9223372036854775808,-5534023222112865486]"},
                          new String[]{"(1844674407370955158,9223372036854775807]", "(-9223372036854775808,-3689348814741910325]"},
                          new String[]{"(3689348814741910319,9223372036854775807]", "(-9223372036854775808,-1844674407370955164]"},
                          new String[]{"(5534023222112865480,9223372036854775807]", "(-9223372036854775808,-3]"},
                          new String[]{"(7378697629483820641,9223372036854775807]", "(-9223372036854775808,1844674407370955158]"},
                          new String[]{"(-9223372036854775808,3689348814741910319]"},
                          new String[]{"(-7378697629483820647,5534023222112865480]"},
                          new String[]{"(-5534023222112865486,7378697629483820641]"});
    }

    @Test
    void testCalculateTokenRangesTenNodesRF5()
    {
        assertTokenRanges(10, 5,
                          new String[]{"(-3,9223372036854775807]"},
                          new String[]{"(1844674407370955158,9223372036854775807]", "(-9223372036854775808,-7378697629483820647]"},
                          new String[]{"(3689348814741910319,9223372036854775807]", "(-9223372036854775808,-5534023222112865486]"},
                          new String[]{"(5534023222112865480,9223372036854775807]", "(-9223372036854775808,-3689348814741910325]"},
                          new String[]{"(7378697629483820641,9223372036854775807]", "(-9223372036854775808,-1844674407370955164]"},
                          new String[]{"(-9223372036854775808,-3]"},
                          new String[]{"(-7378697629483820647,1844674407370955158]"},
                          new String[]{"(-5534023222112865486,3689348814741910319]"},
                          new String[]{"(-3689348814741910325,5534023222112865480]"},
                          new String[]{"(-1844674407370955164,7378697629483820641]"});
    }

    @Test
    void testCalculateTokenRangesTenNodesRF3()
    {
        assertTokenRanges(10, 3,
                          new String[]{"(3689348814741910319,9223372036854775807]"},
                          new String[]{"(5534023222112865480,9223372036854775807]", "(-9223372036854775808,-7378697629483820647]"},
                          new String[]{"(7378697629483820641,9223372036854775807]", "(-9223372036854775808,-5534023222112865486]"},
                          new String[]{"(-9223372036854775808,-3689348814741910325]"},
                          new String[]{"(-7378697629483820647,-1844674407370955164]"},
                          new String[]{"(-5534023222112865486,-3]"},
                          new String[]{"(-3689348814741910325,1844674407370955158]"},
                          new String[]{"(-1844674407370955164,3689348814741910319]"},
                          new String[]{"(-3,5534023222112865480]"},
                          new String[]{"(1844674407370955158,7378697629483820641]"});
    }

    @Test
    void testCalculateTokenRangesTenNodesRF1()
    {
        assertTokenRanges(10, 1,
                          new String[]{"(7378697629483820641,9223372036854775807]"},
                          new String[]{"(-9223372036854775808,-7378697629483820647]"},
                          new String[]{"(-7378697629483820647,-5534023222112865486]"},
                          new String[]{"(-5534023222112865486,-3689348814741910325]"},
                          new String[]{"(-3689348814741910325,-1844674407370955164]"},
                          new String[]{"(-1844674407370955164,-3]"},
                          new String[]{"(-3,1844674407370955158]"},
                          new String[]{"(1844674407370955158,3689348814741910319]"},
                          new String[]{"(3689348814741910319,5534023222112865480]"},
                          new String[]{"(5534023222112865480,7378697629483820641]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF4()
    {
        assertTokenRanges(4, 4,
                          new String[]{"(-9223372036854775808,9223372036854775807]"},
                          new String[]{"(-4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                          new String[]{"(0,9223372036854775807]", "(-9223372036854775808,0]"},
                          new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF3()
    {
        assertTokenRanges(4, 3,
                          new String[]{"(-4611686018427387904,9223372036854775807]"},
                          new String[]{"(0,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                          new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,0]"},
                          new String[]{"(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF2()
    {
        assertTokenRanges(4, 2,
                          new String[]{"(0,9223372036854775807]"},
                          new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                          new String[]{"(-9223372036854775808,0]"},
                          new String[]{"(-4611686018427387904,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF1()
    {
        assertTokenRanges(4, 1,
                          new String[]{"(4611686018427387904,9223372036854775807]"},
                          new String[]{"(-9223372036854775808,-4611686018427387904]"},
                          new String[]{"(-4611686018427387904,0]"},
                          new String[]{"(0,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesRFGreaterThanNodesFails()
    {
        assertThatThrownBy(() -> assertTokenRanges(2, 3,
                                                   new String[]{"Does Not"},
                                                   new String[]{"Matter"}))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCalculateTokenRangesZeroNodesSucceeds()
    {
        assertTokenRanges(0, 3);
    }

    @Test
    void testSplitTinyRange()
    {
        Range<BigInteger> range = Range.openClosed(BigInteger.ZERO, BigInteger.ONE);
        List<Range<BigInteger>> expected = Collections.singletonList(range);
        for (int nrSplits = 1; nrSplits < 5; nrSplits++)
        {
            // regardless of number of splits, the output should be a list of single range
            assertThat(RangeUtils.split(range, nrSplits)).isEqualTo(expected);
        }
    }

    @Test
    void testSplitOnlyProduceOpenClosedRanges()
    {
        Range<BigInteger> range = Range.openClosed(BigInteger.ZERO, BigInteger.TEN);
        for (int nrSplit = 1; nrSplit < 15; nrSplit++) // the input range can be split for at most 10 time
        {
            for (Range<BigInteger> subrange : RangeUtils.split(range, nrSplit))
            {
                assertThat(subrange.lowerBoundType()).isEqualTo(BoundType.OPEN);
                assertThat(subrange.upperBoundType()).isEqualTo(BoundType.CLOSED);
            }
        }
    }

    @Test
    void testSplitYieldMoreEvenRanges()
    {
        Range<BigInteger> range = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(11L));
        int nrSplits = 4;
        List<Range<BigInteger>> expectedResult = Arrays.asList(
        Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(3)),
        Range.openClosed(BigInteger.valueOf(3), BigInteger.valueOf(6)),
        Range.openClosed(BigInteger.valueOf(6), BigInteger.valueOf(9)),
        Range.openClosed(BigInteger.valueOf(9), BigInteger.valueOf(11))
        );
        assertThat(RangeUtils.split(range, nrSplits)).isEqualTo(expectedResult);
    }

    @Test
    void testSplitNotSatisfyNrSplits()
    {
        Range<BigInteger> range = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(2));
        int nrSplits = 5;
        List<Range<BigInteger>> expectedResult = Arrays.asList(
        Range.openClosed(BigInteger.ZERO, BigInteger.ONE),
        Range.openClosed(BigInteger.ONE, BigInteger.valueOf(2))
        );
        assertThat(RangeUtils.split(range, nrSplits)).isEqualTo(expectedResult);
    }

    @Test
    void testFindUncoveredRangesWithCompleteCoverage()
    {
        Range<BigInteger> fullRange = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(30));
        List<Range<BigInteger>> covering = Arrays.asList(
        Range.openClosed(BigInteger.ZERO, BigInteger.TEN),
        Range.openClosed(BigInteger.TEN, BigInteger.valueOf(20)),
        Range.openClosed(BigInteger.valueOf(20), BigInteger.valueOf(30))
        );
        assertThat(RangeUtils.findUncoveredRanges(fullRange, covering)).isEmpty();
    }

    @Test
    void testFindUncoveredRangesDetectsGap()
    {
        // leaves (10, 20] uncovered -- a real, non-empty gap
        Range<BigInteger> fullRange = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(30));
        List<Range<BigInteger>> covering = Arrays.asList(
        Range.openClosed(BigInteger.ZERO, BigInteger.TEN),
        Range.openClosed(BigInteger.valueOf(20), BigInteger.valueOf(30))
        );
        assertThat(RangeUtils.findUncoveredRanges(fullRange, covering))
        .containsExactly(Range.openClosed(BigInteger.TEN, BigInteger.valueOf(20)));
    }

    @Test
    void testFindUncoveredRangesDetectsMultipleGapsInAscendingOrder()
    {
        Range<BigInteger> fullRange = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(40));
        List<Range<BigInteger>> covering = Arrays.asList(
        Range.openClosed(BigInteger.valueOf(20), BigInteger.valueOf(30)),
        Range.openClosed(BigInteger.ZERO, BigInteger.TEN)
        );
        assertThat(RangeUtils.findUncoveredRanges(fullRange, covering))
        .containsExactly(Range.openClosed(BigInteger.TEN, BigInteger.valueOf(20)),
                         Range.openClosed(BigInteger.valueOf(30), BigInteger.valueOf(40)));
    }

    @Test
    void testFindUncoveredRangesToleratesOverlappingAndUnsortedInput()
    {
        Range<BigInteger> fullRange = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(30));
        List<Range<BigInteger>> covering = Arrays.asList(
        Range.openClosed(BigInteger.valueOf(15), BigInteger.valueOf(30)),
        Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(20))
        );
        assertThat(RangeUtils.findUncoveredRanges(fullRange, covering)).isEmpty();
    }

    @Test
    void testFindUncoveredRangesToleratesEmptyCoveringRanges()
    {
        // (5, 5] is degenerate: it covers no token, so removing it is a no-op and the ranges either side still cover
        Range<BigInteger> fullRange = Range.openClosed(BigInteger.ZERO, BigInteger.TEN);
        List<Range<BigInteger>> covering = Arrays.asList(
        Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(5)),
        Range.openClosed(BigInteger.valueOf(5), BigInteger.valueOf(5)),
        Range.openClosed(BigInteger.valueOf(5), BigInteger.TEN)
        );
        assertThat(RangeUtils.findUncoveredRanges(fullRange, covering)).isEmpty();
    }

    @Test
    void testFindUncoveredRangesRejectsEmptyFullRange()
    {
        // Nothing can cover an empty range, so reporting it as fully covered would be a false negative
        Range<BigInteger> emptyRange = Range.openClosed(BigInteger.TEN, BigInteger.TEN);
        assertThatThrownBy(() -> RangeUtils.findUncoveredRanges(emptyRange, Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fullRange must not be empty");
    }

    @Test
    void testFindUncoveredRingRangesDetectsGap()
    {
        for (Partitioner partitioner : Partitioner.values())
        {
            List<Range<BigInteger>> ring = RangeUtils.split(wholeRing(partitioner), 4);
            assertThat(RangeUtils.findUncoveredRingRanges(partitioner, withGapAt(ring, 2)))
            .as("A gap in the %s ring should be reported", partitioner)
            .containsExactly(gapPunchedInto(ring.get(2)));
        }
    }

    @Test
    void testFindUncoveredRingRangesDetectsGapAtRingLowerEdge()
    {
        // The bound type at minToken is the whole subtlety of findUncoveredRingRanges: it must not report minToken
        // itself as a gap, yet it must still report a gap that starts immediately above minToken
        for (Partitioner partitioner : Partitioner.values())
        {
            List<Range<BigInteger>> ring = RangeUtils.split(wholeRing(partitioner), 4);
            assertThat(RangeUtils.findUncoveredRingRanges(partitioner, withGapAt(ring, 0)))
            .as("A gap at the lower edge of the %s ring should be reported", partitioner)
            .containsExactly(gapPunchedInto(ring.get(0)));
        }
    }

    @Test
    void testFindUncoveredRingRangesDetectsMissingFirstSubRange()
    {
        // Dropping the first sub-range leaves everything above minToken up to its upper endpoint uncovered
        for (Partitioner partitioner : Partitioner.values())
        {
            List<Range<BigInteger>> ring = RangeUtils.split(wholeRing(partitioner), 4);
            assertThat(RangeUtils.findUncoveredRingRanges(partitioner, ring.subList(1, ring.size())))
            .as("A missing first sub-range of the %s ring should be reported", partitioner)
            .containsExactly(Range.openClosed(partitioner.minToken(), ring.get(0).upperEndpoint()));
        }
    }

    @Test
    void testFindUncoveredRingRangesDetectsMissingLastSubRange()
    {
        // The counterpart of the above at the other end of the ring, where maxToken is inclusive
        for (Partitioner partitioner : Partitioner.values())
        {
            List<Range<BigInteger>> ring = RangeUtils.split(wholeRing(partitioner), 4);
            assertThat(RangeUtils.findUncoveredRingRanges(partitioner, ring.subList(0, ring.size() - 1)))
            .as("A missing last sub-range of the %s ring should be reported", partitioner)
            .containsExactly(ring.get(ring.size() - 1));
        }
    }

    @Test
    void testFindUncoveredRingRangesOverWholeRingIsGapFree()
    {
        // A gap-free split of the whole ring leaves minToken uncovered, because the sub-ranges are open-closed.
        // findUncoveredRingRanges owns that bound-type decision, so no caller can get it wrong: it must not
        // report [minToken, minToken] as a gap.
        for (Partitioner partitioner : Partitioner.values())
        {
            assertThat(RangeUtils.findUncoveredRingRanges(partitioner, RangeUtils.split(wholeRing(partitioner), 16)))
            .as("Splitting the whole %s ring should leave no gap", partitioner)
            .isEmpty();
        }
    }

    private static Range<BigInteger> wholeRing(Partitioner partitioner)
    {
        return Range.openClosed(partitioner.minToken(), partitioner.maxToken());
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

    private static void assertTokenRanges(int nodes, int replicationFactor, String[]... ranges)
    {
        assertThat(nodes).isEqualTo(ranges.length);
        BigInteger[] tokens = getTokens(Partitioner.Murmur3Partitioner, nodes);
        List<CassandraInstance> instances = getInstances(tokens);
        Multimap<CassandraInstance, Range<BigInteger>> allRanges =
        RangeUtils.calculateTokenRanges(instances, replicationFactor, Partitioner.Murmur3Partitioner);
        for (int node = 0; node < nodes; node++)
        {
            assertExpectedRanges(allRanges.get(instances.get(node)), ranges[node]);
        }
    }

    private static void assertExpectedRanges(Collection<Range<BigInteger>> actual, String... expectedRanges)
    {
        assertThat(expectedRanges.length).isEqualTo(actual.size());
        for (String expected : expectedRanges)
        {
            assertThat(actual).as(String.format("Expected range %s not found in %s", expected, actual)).contains(range(expected));
        }
    }

    private static BigInteger[] getTokens(Partitioner partitioner, int nodes)
    {
        BigInteger[] tokens = new BigInteger[nodes];

        for (int node = 0; node < nodes; node++)
        {
            tokens[node] = partitioner == Partitioner.Murmur3Partitioner
                           ? getMurmur3Token(nodes, node)
                           : getRandomToken(nodes, node);
        }
        return tokens;
    }

    private static BigInteger getRandomToken(int nodes, int index)
    {
        // ((2^127 / nodes) * i)
        return ((BigInteger.valueOf(2).pow(127)).divide(BigInteger.valueOf(nodes))).multiply(BigInteger.valueOf(index));
    }

    private static BigInteger getMurmur3Token(int nodes, int index)
    {
        // (((2^64 / n) * i) - 2^63)
        return (((BigInteger.valueOf(2).pow(64)).divide(BigInteger.valueOf(nodes)))
                .multiply(BigInteger.valueOf(index))).subtract(BigInteger.valueOf(2).pow(63));
    }

    private static List<CassandraInstance> getInstances(BigInteger[] tokens)
    {
        List<CassandraInstance> instances = new ArrayList<>();
        for (int token = 0; token < tokens.length; token++)
        {
            instances.add(new CassandraInstance(tokens[token].toString(), "node-" + token, "dc"));
        }
        return instances;
    }

    private static Range<BigInteger> range(String range)
    {
        Matcher m = RANGE_PATTERN.matcher(range);
        if (m.matches())
        {
            int length = range.length();

            BigInteger lowerBound = new BigInteger(m.group(1));
            BigInteger upperBound = new BigInteger(m.group(2));

            if (range.charAt(0) == '(')
            {
                if (range.charAt(length - 1) == ')')
                {
                    return Range.open(lowerBound, upperBound);
                }
                return Range.openClosed(lowerBound, upperBound);
            }
            else
            {
                if (range.charAt(length - 1) == ')')
                {
                    return Range.closedOpen(lowerBound, upperBound);
                }
                return Range.closed(lowerBound, upperBound);
            }
        }
        throw new IllegalArgumentException("Range " + range + " is not valid");
    }
}
