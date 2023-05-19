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

package org.apache.cassandra.spark.cdc;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.jetbrains.annotations.NotNull;

public interface CommitLog extends AutoCloseable
{
    /**
     * @return filename of the CommitLog
     */
    String name();

    /**
     * @return path to the CommitLog
     */
    String path();

    /**
     * @return the max offset that can be read in the CommitLog.
     *         This may be less than or equal to {@link CommitLog#length()}.
     *         The reader should not read passed this point when the CommitLog is incomplete.
     */
    long maxOffset();

    /**
     * @return length of the CommitLog in bytes
     */
    long length();

    /**
     * @return an SSTableSource for asynchronously reading the CommitLog bytes
     */
    SSTableSource<? extends SSTable> source();

    /**
     * @return the CassandraInstance this CommitLog resides on
     */
    CassandraInstance instance();

    default CommitLog.Marker zeroMarker()
    {
        return markerAt(0, 0);
    }

    default CommitLog.Marker markerAt(long section, int offset)
    {
        return new CommitLog.Marker(instance(), section, offset);
    }

    /**
     * Override to provide custom stats implementation
     *
     * @return stats instance for publishing stats
     */
    default Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }

    class Marker implements Comparable<Marker>
    {
        CassandraInstance instance;
        long segmentId;
        int position;

        public Marker(CassandraInstance instance, long segmentId, int position)
        {
            this.instance = instance;
            this.segmentId = segmentId;
            this.position = position;
        }

        /**
         * Marks the start position of the section
         *
         * @return position in CommitLog of the section
         */
        public long segmentId()
        {
            return segmentId;
        }

        public CassandraInstance instance()
        {
            return instance;
        }

        /**
         * The offset into the section where the mutation starts
         *
         * @return mutation offset within the section
         */
        public int position()
        {
            return position;
        }

        @Override
        public int compareTo(@NotNull Marker that)
        {
            Preconditions.checkArgument(this.instance.equals(that.instance),
                                        "CommitLog Markers should be on the same instance");
            int comparison = Long.compare(this.segmentId, that.segmentId());
            if (comparison == 0)
            {
                return Integer.compare(this.position, that.position());
            }
            return comparison;
        }

        public String toString()
        {
            return String.format("{\"segmentId\": %d, \"position\": %d}", segmentId, position);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder()
                   .append(instance)
                   .append(segmentId)
                   .append(position)
                   .toHashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null)
            {
                return false;
            }
            if (this == other)
            {
                return true;
            }
            if (this.getClass() != other.getClass())
            {
                return false;
            }

            Marker that = (Marker) other;
            return new EqualsBuilder()
                   .append(this.instance, that.instance)
                   .append(this.segmentId, that.segmentId)
                   .append(this.position, that.position)
                   .isEquals();
        }
    }
}
