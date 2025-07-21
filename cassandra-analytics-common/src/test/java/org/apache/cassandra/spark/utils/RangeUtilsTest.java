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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeUtilsTest
{
    private static final Pattern RANGE_PATTERN = Pattern.compile("^[\\\\(|\\[](-?\\d+),(-?\\d+)[\\\\)|\\]]$");

    @Test
    void testCalculateTokenRangesFourNodesRF3NumTokens3()
    {
        assertTokenRanges(4, 3, 3,
                new String[]{"(7686143364045646503,9223372036854775807]"},
                new String[]{"(-9223372036854775808,-7686143364045646507]"},
                new String[]{"(-7686143364045646507,-6148914691236517206]"},
                new String[]{"(-6148914691236517206,-4611686018427387905]"},
                new String[]{"(-4611686018427387905,-3074457345618258604]"},
                new String[]{"(-3074457345618258604,-1537228672809129303]"},
                new String[]{"(-1537228672809129303,-2]"},
                new String[]{"(-2,1537228672809129299]"},
                new String[]{"(1537228672809129299,3074457345618258600]"},
                new String[]{"(3074457345618258600,4611686018427387901]"},
                new String[]{"(4611686018427387901,6148914691236517202]"},
                new String[]{"(6148914691236517202,7686143364045646503]"});
    }

    @Test
    void testCalculateTokenRangesTenNodesRF10()
    {
        assertTokenRanges(10, 10, 1,
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
        assertTokenRanges(10, 7, 1,
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
        assertTokenRanges(10, 5, 1,
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
        assertTokenRanges(10, 3, 1,
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
        assertTokenRanges(10, 1, 1,
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
        assertTokenRanges(4, 4, 1,
                new String[]{"(-9223372036854775808,9223372036854775807]"},
                new String[]{"(-4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(0,9223372036854775807]", "(-9223372036854775808,0]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF3()
    {
        assertTokenRanges(4, 3, 1,
                new String[]{"(-4611686018427387904,9223372036854775807]"},
                new String[]{"(0,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,0]"},
                new String[]{"(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF2()
    {
        assertTokenRanges(4, 2, 1,
                new String[]{"(0,9223372036854775807]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(-9223372036854775808,0]"},
                new String[]{"(-4611686018427387904,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesFourNodesRF1()
    {
        assertTokenRanges(4, 1, 1,
                new String[]{"(4611686018427387904,9223372036854775807]"},
                new String[]{"(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(-4611686018427387904,0]"},
                new String[]{"(0,4611686018427387904]"});
    }

    @Test
    void testCalculateTokenRangesRFGreaterThanNodesFails()
    {
        assertThrows(IllegalArgumentException.class,
                () -> assertTokenRanges(2, 3, 1,
                        new String[]{"Does Not"},
                        new String[]{"Matter"})
        );
    }

    @Test
    void testCalculateTokenRangesZeroNodesSucceeds()
    {
        assertTokenRanges(0, 3, 1);
    }

    @Test
    void testSplitTinyRange()
    {
        Range<BigInteger> range = Range.openClosed(BigInteger.ZERO, BigInteger.ONE);
        List<Range<BigInteger>> expected = Collections.singletonList(range);
        for (int nrSplits = 1; nrSplits < 5; nrSplits++)
        {
            // regardless of number of splits, the output should be a list of single range
            assertEquals(expected, RangeUtils.split(range, nrSplits));
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
                assertEquals(BoundType.OPEN, subrange.lowerBoundType());
                assertEquals(BoundType.CLOSED, subrange.upperBoundType());
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
        assertEquals(expectedResult, RangeUtils.split(range, nrSplits));
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
        assertEquals(expectedResult, RangeUtils.split(range, nrSplits));
    }

    private static void assertTokenRanges(int nodes, int replicationFactor, int numTokens, String[]... ranges)
    {
        // assertEquals(nodes * numTokens, ranges.length);
        BigInteger[] tokens = getTokens(Partitioner.Murmur3Partitioner, nodes, numTokens);
        List<CassandraInstance> instances = getInstances(tokens, numTokens);
        Multimap<CassandraInstance, Range<BigInteger>> allRanges =
                RangeUtils.calculateTokenRanges(instances, replicationFactor, Partitioner.Murmur3Partitioner);
        for (int instance = 0; instance < nodes * numTokens; instance++)
        {
            assertExpectedRanges(allRanges.get(instances.get(instance)), ranges[instance]);
        }
    }

    private static void assertExpectedRanges(Collection<Range<BigInteger>> actual, String... expectedRanges)
    {
        assertEquals(expectedRanges.length, actual.size());
        for (String expected : expectedRanges)
        {
            assertTrue(actual.contains(range(expected)),
                    String.format("Expected range %s not found in %s", expected, actual));
        }
    }

    private static BigInteger[] getTokens(Partitioner partitioner, int nodes, int numTokens)
    {
        BigInteger[] tokens = new BigInteger[nodes * numTokens];

        for (int node = 0; node < nodes; node++)
        {
            for (int token = 0; token < numTokens; token++) {
                tokens[(node * numTokens)+token] = partitioner == Partitioner.Murmur3Partitioner
                        ? getMurmur3Token(nodes, node, numTokens, token)
                        : getRandomToken(nodes, node); // TODO(aj): Support for vnodes
            }
        }
        return tokens;
    }

    private static BigInteger getRandomToken(int nodes, int index)
    {
        // ((2^127 / nodes) * i)
        return ((BigInteger.valueOf(2).pow(127)).divide(BigInteger.valueOf(nodes))).multiply(BigInteger.valueOf(index));
    }

    private static BigInteger getMurmur3Token(int numNodes, int nodeIndex, int numTokens, int tokenIndex)
    {
        // (((2^64 / numNodes + numTokens) * (token * numNodes + node)) - 2^63)
        return (((BigInteger.valueOf(2).pow(64)).divide(BigInteger.valueOf((long) numTokens * numNodes)))
                .multiply(BigInteger.valueOf((long) tokenIndex * numNodes + nodeIndex))).subtract(BigInteger.valueOf(2).pow(63));
    }

    private static List<CassandraInstance> getInstances(BigInteger[] tokens, int numTokens)
    {
        List<CassandraInstance> instances = new ArrayList<>();
        for (int node = 0; node < (tokens.length / numTokens); node++)
        {
            for (int token = 0; token < numTokens; token++)
            {
                instances.add(new CassandraInstance(tokens[(node * numTokens) + token].toString(), "node-" + node, "dc"));
            }
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
