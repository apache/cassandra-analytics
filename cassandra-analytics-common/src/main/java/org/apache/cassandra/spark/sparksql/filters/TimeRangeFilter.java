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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.util.Objects;

import com.google.common.collect.Range;

import org.jetbrains.annotations.NotNull;

/**
 * {@link TimeRangeFilter} to filter out based on timestamp.
 * Uses Google Guava's Range internally for storing time range.
 */
public final class TimeRangeFilter implements Serializable
{
    private final Range<Long> timeRange;

    /**
     * Creates a {@link TimeRangeFilter} with given time {@link Range}
     */
    private TimeRangeFilter(Range<Long> timeRange)
    {
        this.timeRange = timeRange;
    }

    /**
     * Returns the underlying Range.
     *
     * @return the time range
     */
    @NotNull
    public Range<Long> range()
    {
        return timeRange;
    }

    /**
     * Determines if given start and end timestamp match the filter. SSTable is included if it overlaps with
     * filter time range.
     *
     * @param givenStart the SSTable min timestamp
     * @param givenEnd the SSTable max timestamp
     * @return true if the SSTable should be included, false if it should be omitted.
     */
    public boolean filter(long givenStart, long givenEnd)
    {
        // Create range for the given SSTable timestamps, always closed
        Range<Long> sstableTimeRange = Range.closed(givenStart, givenEnd);

        // Check if ranges are connected (overlap or adjacent)
        return timeRange.isConnected(sstableTimeRange) && !timeRange.intersection(sstableTimeRange).isEmpty();
    }

    @Override
    public String toString()
    {
        return String.format("TimeRangeFilter%s", timeRange.toString());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof TimeRangeFilter))
        {
            return false;
        }
        TimeRangeFilter that = (TimeRangeFilter) o;
        return timeRange.equals(that.timeRange);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timeRange);
    }

    /**
     * Creates a {@link TimeRangeFilter} with only start bound.
     *
     * @param startTimestampMicros the start timestamp in microseconds (inclusive)
     * @return {@link TimeRangeFilter} with only start timestamp
     */
    @NotNull
    public static TimeRangeFilter startingAt(long startTimestampMicros)
    {
        Range<Long> range = Range.atLeast(startTimestampMicros);
        return new TimeRangeFilter(range);
    }

    /**
     * Creates a {@link TimeRangeFilter} with only end bound.
     *
     * @param endTimestampMicros the end timestamp in microseconds (inclusive)
     * @return {@link TimeRangeFilter} with only end timestamp
     */
    @NotNull
    public static TimeRangeFilter endingAt(long endTimestampMicros)
    {
        Range<Long> range = Range.atMost(endTimestampMicros);
        return new TimeRangeFilter(range);
    }

    /**
     * Creates a {@link TimeRangeFilter} for a specific time range.
     *
     * @param startTimestampMicros the start timestamp in microseconds (inclusive)
     * @param endTimestampMicros   the end timestamp in microseconds (inclusive)
     * @return {@link TimeRangeFilter} with both start and end timestamps
     */
    @NotNull
    public static TimeRangeFilter create(long startTimestampMicros, long endTimestampMicros)
    {
        return new TimeRangeFilter(Range.closed(startTimestampMicros, endTimestampMicros));
    }
}
