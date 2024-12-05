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

package org.apache.cassandra.cdc.api;

import java.io.Closeable;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.jetbrains.annotations.NotNull;

public interface CommitLog extends Closeable, CassandraFile, Comparable<CommitLog>
{
    Logger LOGGER = LoggerFactory.getLogger(CommitLog.class);

    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-6-12345.log.
    Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog(-(\\d+))?-(\\d+).log");

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final CommitLog log)
    {
        return extractVersionAndSegmentId(log.name());
    }

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final String filename)
    {
        final Matcher matcher = CommitLog.COMMIT_LOG_FILE_PATTERN.matcher(filename);
        if (matcher.matches())
        {
            try
            {
                final int version = matcher.group(2) == null ? 6 : Integer.parseInt(matcher.group(2));
                if (version != 6 && version != 7)
                {
                    throw new IllegalStateException("Unknown commitlog version " + version);
                }
                // logic taken from org.apache.cassandra.db.commitlog.CommitLogDescriptor.getMessagingVersion()
                return Optional.of(Pair.of(version == 6 ? 10 : 12, Long.parseLong(matcher.group(3))));
            }
            catch (NumberFormatException e)
            {
                LOGGER.error("Could not parse commit log segmentId name={}", filename, e);
                return Optional.empty();
            }
        }
        LOGGER.error("Could not parse commit log filename name={}", filename);
        return Optional.empty(); // cannot extract segment id
    }

    /**
     * @return filename of the CommitLog
     */
    String name();

    /**
     * @return path to the CommitLog
     */
    String path();

    /**
     * @return the max offset that can be read in the CommitLog. This may be less than or equal to {@link CommitLog#length()}.
     * The reader should not read passed this point when the commit log is incomplete.
     */
    long maxOffset();

    /**
     * @return length of the CommitLog in bytes
     */
    long length();

    /**
     * @return true if the commit log has been fully written to and no more mutations will be written.
     * "COMPLETED" is written to the CommitLog-7-*_cdc.idx index file when Cassandra is finished.
     */
    boolean completed();

    /**
     * @return a Source for asynchronously reading the CommitLog bytes.
     */
    CassandraFileSource<CommitLog> source();

    /**
     * @return the CassandraInstance this CommitLog resides on.
     */
    CassandraInstance instance();

    default long segmentId()
    {
        return extractVersionAndSegmentId(this).map(Pair::getRight).orElseThrow(() -> new RuntimeException("Could not extract segmentId from CommitLog"));
    }

    default Marker zeroMarker()
    {
        return markerAt(segmentId(), 0);
    }

    default Marker maxMarker()
    {
        return markerAt(segmentId(), Math.toIntExact(maxOffset()));
    }

    default Marker markerAt(long section, int offset)
    {
        return new Marker(instance(), section, offset);
    }

    /**
     * Override to provide custom stats implementation.
     *
     * @return stats instance for publishing stats
     */
    default ICdcStats stats()
    {
        return ICdcStats.STUB;
    }

    @Override
    default int compareTo(@NotNull CommitLog other)
    {
        return Long.compare(segmentId(), other.segmentId());
    }
}
