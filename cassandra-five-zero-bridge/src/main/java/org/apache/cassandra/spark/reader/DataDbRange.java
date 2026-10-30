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
package org.apache.cassandra.spark.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.spark.utils.Preconditions;
import org.jetbrains.annotations.NotNull;

/**
 * An uncompressed {@code Data.db} byte range in the form {@code [start, end)}.
 */
public final class DataDbRange
{
    private final long start;
    private final long end;

    public DataDbRange(long start, long end)
    {
        Preconditions.checkArgument(start >= 0 && end >= start,
                                    "Invalid Data.db range [" + start + ", " + end + ")");
        this.start = start;
        this.end = end;
    }

    public long start()
    {
        return start;
    }

    public long end()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "[" + start + ", " + end + ")";
    }

    /**
     * Incrementally coalesces already sorted physical Data.db ranges without retaining a temporary range per hit.
     */
    public static final class Accumulator
    {
        private final long maxGapBytes; // Coalesce token ranges with limited gap between them to ease memory pressure.
        private final List<DataDbRange> ranges = new ArrayList<>();
        private long currentStart = -1L;
        private long currentEnd = -1L;

        public Accumulator(long maxGapBytes)
        {
            Preconditions.checkArgument(maxGapBytes >= 0, "maxGapBytes must be non-negative");
            this.maxGapBytes = maxGapBytes;
        }

        public void add(long start, long end)
        {
            Preconditions.checkArgument(start >= 0 && end >= start,
                                        "Invalid Data.db range [" + start + ", " + end + ")");
            if (currentStart < 0)
            {
                currentStart = start;
                currentEnd = end;
                return;
            }

            Preconditions.checkState(start >= currentStart,
                                     "Data.db ranges must be supplied in sorted order");

            long gap = start <= currentEnd ? 0L : start - currentEnd;
            if (gap <= maxGapBytes)
            {
                currentEnd = Math.max(currentEnd, end);
            }
            else
            {
                flush();
                currentStart = start;
                currentEnd = end;
            }
        }

        @NotNull
        public List<DataDbRange> build()
        {
            flush();
            if (ranges.isEmpty())
            {
                return Collections.emptyList();
            }
            return Collections.unmodifiableList(new ArrayList<>(ranges));
        }

        private void flush()
        {
            if (currentStart >= 0)
            {
                ranges.add(new DataDbRange(currentStart, currentEnd));
                currentStart = -1L;
                currentEnd = -1L;
            }
        }
    }
}
