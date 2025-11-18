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
 * {@link SSTableTimeRangeFilter} to filter out based on timestamp in microseconds.
 * Uses Google Guava's Range internally for storing time range.
 */
public class SSTableTimeRangeFilter implements Serializable
{
    public static final SSTableTimeRangeFilter EMPTY = new SSTableTimeRangeFilter(Range.closed(0L, 0L))
    {
        @Override
        public boolean overlaps(long startMicros, long endMicros)
        {
            return true; // accepts all SSTables
        }
    };

    /**
     * {@code timeRange} is range of timestamp values represented in microseconds. Supports only closed range.
     */
    private final Range<Long> timeRange;
    private final int hashcode;

    /**
     * Creates a {@link SSTableTimeRangeFilter} with given time {@link Range} of timestamp values represented in
     * microseconds.
     */
    private SSTableTimeRangeFilter(Range<Long> timeRange)
    {
        this.timeRange = timeRange;
        this.hashcode = Objects.hash(timeRange);
    }

    /**
     * Returns the underlying Range.
     *
     * @return the time range of timestamp values in microseconds.
     */
    @NotNull
    public Range<Long> range()
    {
        return timeRange;
    }

    /**
     * Determines if SSTable with min and max timestamp overlap with the filter. SSTable is included if it
     * overlaps with filter time range.
     *
     * @param startMicros   SSTable min timestamp in microseconds (inclusive)
     * @param endMicros     SSTable max timestamp in microseconds (inclusive)
     * @return true if the SSTable should be included, false if it should be omitted.
     */
    public boolean overlaps(long startMicros, long endMicros)
    {
        // Creates a closed range with startMicros and endMicros
        Range<Long> sstableTimeRange = Range.closed(startMicros, endMicros);

        // Check if ranges are connected (overlap or adjacent)
        return timeRange.isConnected(sstableTimeRange);
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
        if (!(o instanceof SSTableTimeRangeFilter))
        {
            return false;
        }
        SSTableTimeRangeFilter that = (SSTableTimeRangeFilter) o;
        return timeRange.equals(that.timeRange);
    }

    @Override
    public int hashCode()
    {
        return hashcode;
    }

    /**
     * Creates a {@link SSTableTimeRangeFilter} for a specific time range.
     *
     * @param startMicros the start timestamp in microseconds (inclusive)
     * @param endMicros   the end timestamp in microseconds (inclusive)
     * @return {@link SSTableTimeRangeFilter} with both start and end timestamps
     */
    @NotNull
    public static SSTableTimeRangeFilter create(long startMicros, long endMicros)
    {
        return new SSTableTimeRangeFilter(Range.closed(startMicros, endMicros));
    }
}
