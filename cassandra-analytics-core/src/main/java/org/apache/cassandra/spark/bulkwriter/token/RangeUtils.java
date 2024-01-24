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

package org.apache.cassandra.spark.bulkwriter.token;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

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
        Preconditions.checkArgument(range.lowerBoundType() == BoundType.OPEN
                                    && range.upperBoundType() == BoundType.CLOSED,
                                    "Input must be an open-closed range");

        if (range.isEmpty())
        {
            return BigInteger.ZERO;
        }

        return range.upperEndpoint().subtract(range.lowerEndpoint());
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
        Preconditions.checkArgument(range.lowerBoundType() == BoundType.OPEN
                                    && range.upperBoundType() == BoundType.CLOSED,
                                    "Input must be an open-closed range");

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

        // Make sure split size is not 0
        BigInteger splitSize = sizeOf(range).divide(BigInteger.valueOf(nrSplits));
        boolean isTinyRange = splitSize.compareTo(BigInteger.ZERO) == 0; // a tiny range that cannot be split this many times
        if (isTinyRange)
        {
            splitSize = BigInteger.ONE;
        }

        // Start from range lower endpoint and spit ranges of size splitSize, until we cross the range
        BigInteger lowerEndpoint = range.lowerEndpoint();
        List<Range<BigInteger>> splits = new ArrayList<>();
        for (int i = 0; i < nrSplits; i++)
        {
            BigInteger upperEndpoint = lowerEndpoint.add(splitSize);
            if (isTinyRange && upperEndpoint.compareTo(range.upperEndpoint()) >= 0)
            {
                splits.add(Range.openClosed(lowerEndpoint, upperEndpoint));
                break; // the split process terminate early because the original range is exhausted
            }

            // correct the upper endpoint of the last range if needed
            if (i + 1 == nrSplits && (upperEndpoint.compareTo(range.upperEndpoint()) != 0))
            {
                upperEndpoint = range.upperEndpoint();

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

            // If start token is  greater than or equal to end token we are looking at a wrap around range, split it
            if (rangeStart.compareTo(rangeEnd) >= 0)
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, partitioner.maxToken()));
                tokenRanges.put(instance, Range.openClosed(partitioner.minToken(), rangeEnd));
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
        BigInteger lowerEndpoint = range.lowerEndpoint();
        if (range.lowerBoundType() == BoundType.OPEN)
        {
            lowerEndpoint = lowerEndpoint.add(BigInteger.ONE);
        }
        BigInteger upperEndpoint = range.upperEndpoint();
        if (range.upperBoundType() == BoundType.OPEN)
        {
            upperEndpoint = upperEndpoint.subtract(BigInteger.ONE);
        }
        return TokenRange.closed(lowerEndpoint, upperEndpoint);
    }

    @NotNull
    public static Range<BigInteger> fromTokenRange(@NotNull TokenRange range)
    {
        return Range.closed(range.lowerEndpoint(), range.upperEndpoint());
    }
}
