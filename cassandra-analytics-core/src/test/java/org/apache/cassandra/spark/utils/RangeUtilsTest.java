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
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.token.RangeUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangeUtilsTest
{
    private static final Pattern RANGE_PATTERN = Pattern.compile("^[\\\\(|\\[](-?\\d+),(-?\\d+)[\\\\)|\\]]$");

    @Test
    public void testCalculateTokenRangesTenNodesRF10()
    {
        assertTokenRanges(10, 10,
                new String[]{"(-9223372036854775808,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
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
    public void testCalculateTokenRangesTenNodesRF7()
    {
        assertTokenRanges(10, 7,
                new String[]{"(-3689348814741910325,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
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
    public void testCalculateTokenRangesTenNodesRF5()
    {
        assertTokenRanges(10, 5,
                new String[]{"(-3,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
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
    public void testCalculateTokenRangesTenNodesRF3()
    {
        assertTokenRanges(10, 3,
                new String[]{"(3689348814741910319,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
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
    public void testCalculateTokenRangesTenNodesRF1()
    {
        assertTokenRanges(10, 1,
                new String[]{"(7378697629483820641,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
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
    public void testCalculateTokenRangesFourNodesRF4()
    {
        assertTokenRanges(4, 4,
                new String[]{"(-9223372036854775808,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
                new String[]{"(-4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(0,9223372036854775807]", "(-9223372036854775808,0]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    public void testCalculateTokenRangesFourNodesRF3()
    {
        assertTokenRanges(4, 3,
                new String[]{"(-4611686018427387904,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
                new String[]{"(0,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,0]"},
                new String[]{"(-9223372036854775808,4611686018427387904]"});
    }

    @Test
    public void testCalculateTokenRangesFourNodesRF2()
    {
        assertTokenRanges(4, 2,
                new String[]{"(0,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(-9223372036854775808,0]"},
                new String[]{"(-4611686018427387904,4611686018427387904]"});
    }

    @Test
    public void testCalculateTokenRangesFourNodesRF1()
    {
        assertTokenRanges(4, 1,
                new String[]{"(4611686018427387904,9223372036854775807]", "(-9223372036854775808,-9223372036854775808]"},
                new String[]{"(-9223372036854775808,-4611686018427387904]"},
                new String[]{"(-4611686018427387904,0]"},
                new String[]{"(0,4611686018427387904]"});
    }

    @Test()
    public void testCalculateTokenRangesRFGreaterThanNodesFails()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> assertTokenRanges(2, 3,
                                             new String[]{"Does Not"},
                                             new String[]{"Matter"})
        );
    }

    @Test
    public void testCalculateTokenRangesZeroNodesSucceeds()
    {
        assertTokenRanges(0, 3);
    }

    private static void assertTokenRanges(int nodes, int replicationFactor, String[]... ranges)
    {
        assertEquals(nodes, ranges.length);
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
        assertEquals(expectedRanges.length, actual.size());
        for (String expected : expectedRanges)
        {
            assertTrue(actual.contains(range(expected)),
                       String.format("Expected range %s not found in %s", expected, actual));
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
