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
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.model.TokenOwner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

/**
 * Common Cassandra range operations on Guava ranges. Assumes ranges are not wrapped around.
 * It's the responsibility of caller to unwrap ranges. For example, {@code (100, 1]} should become
 * {@code (100, MAX]} and {@code (MIN, 1]}. MIN and MAX values depend on {@link Partitioner}.
 */
public final class RangeUtils
{
    private RangeUtils()
    {
    }

    public static BigInteger sizeOf(Range<BigInteger> range)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "RangeUtils assume ranges are not wrap-around");
        Preconditions.checkArgument(isOpenClosedRange(range), "Input must be an open-closed range");

        if (range.isEmpty())
        {
            return BigInteger.ZERO;
        }

        return range.upperEndpoint().subtract(range.lowerEndpoint());
    }

    /**
     * Check whether a range is open (exclusive) on its lower end and closed (inclusive) on its upper end.
     * @param range range
     * @return true if the range is open closed.
     */
    public static boolean isOpenClosedRange(Range<?> range)
    {
        return range.lowerBoundType() == BoundType.OPEN && range.upperBoundType() == BoundType.CLOSED;
    }

    /**
     * Finds the sub-ranges of the whole token ring that none of the {@code coveringRanges} covers, i.e. the gaps in
     * the ring coverage. An empty result means the ring is covered in its entirety.
     *
     * The ring is expressed as {@code (minToken, maxToken]}, matching the open-closed notation used for token
     * ranges everywhere else. Expressing it as {@code [minToken, maxToken]} instead would report minToken as a
     * spurious single-token gap, because open-closed ranges never cover their own lower endpoint.
     *
     * @param partitioner the partitioner whose ring bounds are expected to be covered
     * @param coveringRanges the ranges expected to cover the ring; they may overlap and need not be sorted
     * @return the uncovered sub-ranges of the ring, in ascending order
     */
    public static List<Range<BigInteger>> findUncoveredRingRanges(Partitioner partitioner,
                                                                  Collection<Range<BigInteger>> coveringRanges)
    {
        return findUncoveredRanges(Range.openClosed(partitioner.minToken(), partitioner.maxToken()), coveringRanges);
    }

    /**
     * Finds the sub-ranges of {@code fullRange} that none of the {@code coveringRanges} covers, i.e. the gaps in
     * the coverage. An empty result means {@code coveringRanges} covers {@code fullRange} in its entirety.
     *
     * Both the input and the output honor Guava's bound types, so {@code fullRange} should be expressed using the
     * same notation as the covering ranges. Prefer {@link #findUncoveredRingRanges} when the range to cover is a
     * whole token ring, so that the notation is not decided at each call site. Empty ranges are never reported,
     * as a range that contains no token cannot be a gap.
     *
     * @param fullRange the non-empty range expected to be fully covered
     * @param coveringRanges the ranges expected to cover {@code fullRange}; they may overlap and need not be sorted
     * @return the uncovered sub-ranges of {@code fullRange}, in ascending order
     */
    public static List<Range<BigInteger>> findUncoveredRanges(Range<BigInteger> fullRange,
                                                              Collection<Range<BigInteger>> coveringRanges)
    {
        // An empty fullRange has nothing to cover, so every input would trivially look fully covered.
        // Reject it rather than report a false "no gaps".
        Preconditions.checkArgument(!fullRange.isEmpty(), "fullRange must not be empty");

        RangeSet<BigInteger> uncovered = TreeRangeSet.create();
        uncovered.add(fullRange);
        coveringRanges.forEach(uncovered::remove);
        // TreeRangeSet coalesces connected ranges and never retains empty ones,
        // so everything left is a real, non-empty gap
        return new ArrayList<>(uncovered.asRanges());
    }

    /**
     * Splits the given range into equal-sized small ranges. Number of splits can be controlled by
     * nrSplits. If nrSplits are smaller than size of the range, split size would be set to 1, which is
     * the minimum allowed. For example, if the input range is {@code (0, 1]} and nrSplits is 10, the split
     * process yields a single range. Because {@code (0, 1]} cannot be split further.
     *
     * This is a best-effort scheme. The nrSplits is not necessarily as promised, and not all splits may be
     * the exact same size.
     *
     * @param range the range to split
     * @param nrSplits the number of sub-ranges into which the range should be divided
     * @return a list of sub-ranges
     */
    public static List<Range<BigInteger>> split(Range<BigInteger> range, int nrSplits)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "RangeUtils assume ranges are not wrap-around");
        Preconditions.checkArgument(isOpenClosedRange(range), "Input must be an open-closed range");

        if (range.isEmpty())
        {
            return Collections.emptyList();
        }

        if (nrSplits == 1 || sizeOf(range).equals(BigInteger.ONE))
        {
            // no split required; exit early
            return Collections.singletonList(range);
        }

        Preconditions.checkArgument(nrSplits >= 1, "nrSplits must be greater than or equal to 1");

        // The following algorithm tries to get a more evenly-split ranges.
        // For example, if we split the range (0, 11] into 4 sub-ranges, the naive split yields the following:
        // (0, 2], (2, 4], (4, 6], (6, 11]
        // As you can see, the last range is significantly larger than the prior range.
        // The desired split is (0, 3], (3, 6], (6, 9], (9, 11]. The sizes of the ranges are more close to each other.
        // --
        // The procedure of the algorithm is as simple as the below 2 steps:
        // 1. Get the quotient and the remainder from the integer division.
        // 2. For each range, as long as the remainder is not 0, we move 1 from the remainder to it.
        // See org.apache.cassandra.spark.utils.RangeUtilsTest.testSplitYieldMoreEvenRanges
        // Given that the remainder is always smaller than the divisor (nrSplits), the remainder is exhausted
        // before the for-loop end.
        // In some special cases, for example, split (0, 3] into 5 sub-ranges. The quotient is 0 and the remainder is 3.
        // When the remainder is exhausted, the input is also exhausted. It yields, (0, 1], (1, 2], (2, 3].
        // See org.apache.cassandra.spark.utils.RangeUtilsTest.testSplitNotSatisfyNrSplits
        BigInteger[] divideAndRemainder = sizeOf(range).divideAndRemainder(BigInteger.valueOf(nrSplits));
        BigInteger quotient = divideAndRemainder[0]; // quotient could be 0
        int remainder = divideAndRemainder[1].intValue(); // remainder must be smaller than nrSplit, which is an integer
        BigInteger lowerEndpoint = range.lowerEndpoint();
        List<Range<BigInteger>> splits = new ArrayList<>();
        for (int i = 0; i < nrSplits; i++)
        {
            BigInteger upperEndpoint = lowerEndpoint.add(quotient);
            if (remainder > 0)
            {
                upperEndpoint = upperEndpoint.add(BigInteger.ONE);
                remainder--;
            }
            if (i + 1 == nrSplits || upperEndpoint.compareTo(range.upperEndpoint()) >= 0)
            {
                splits.add(Range.openClosed(lowerEndpoint, range.upperEndpoint()));
                break; // the split process terminate early because the original range is exhausted
            }
            splits.add(Range.openClosed(lowerEndpoint, upperEndpoint));
            lowerEndpoint = upperEndpoint;
        }

        return splits;
    }

    public static <Instance extends TokenOwner> Multimap<Instance, Range<BigInteger>>
    calculateTokenRanges(List<Instance> instances,
                         int replicationFactor,
                         Partitioner partitioner)
    {
        Preconditions.checkArgument(replicationFactor != 0, "Calculation token ranges wouldn't work with RF 0");
        Preconditions.checkArgument(instances.isEmpty() || replicationFactor <= instances.size(),
                                    "Calculation token ranges wouldn't work when RF (" + replicationFactor
                                  + ") is greater than number of Cassandra instances " + instances.size());
        Multimap<Instance, Range<BigInteger>> tokenRanges = ArrayListMultimap.create();
        for (int index = 0; index < instances.size(); index++)
        {
            Instance instance = instances.get(index);
            int disjointReplica = ((instances.size() + index) - replicationFactor) % instances.size();
            BigInteger rangeStart = new BigInteger(instances.get(disjointReplica).token());
            BigInteger rangeEnd = new BigInteger(instance.token());

            // If start token is greater than or equal to end token we are looking at a wrap around range, split it
            if (rangeStart.compareTo(rangeEnd) >= 0)
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, partitioner.maxToken()));
                // Skip adding the empty range (minToken, minToken]
                if (!rangeEnd.equals(partitioner.minToken()))
                {
                    tokenRanges.put(instance, Range.openClosed(partitioner.minToken(), rangeEnd));
                }
            }
            else
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, rangeEnd));
            }
        }

        return tokenRanges;
    }

    @NotNull
    public static TokenRange toTokenRange(@NotNull Range<BigInteger> range)
    {
        Preconditions.checkArgument(isOpenClosedRange(range), "Input must be an open-closed range");
        return TokenRange.openClosed(range.lowerEndpoint(), range.upperEndpoint());
    }

    @NotNull
    public static Range<BigInteger> fromTokenRange(@NotNull TokenRange range)
    {
        return Range.openClosed(range.lowerEndpoint(), range.upperEndpoint());
    }

    /**
     * @param bi a {@link BigInteger}
     * @return the size of the array returned when {@link BigInteger#toByteArray()} is called.
     */
    public static int bigIntegerByteArraySize(BigInteger bi)
    {
        return bi.bitLength() / 8 + 1;
    }
}
