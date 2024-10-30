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

import java.util.List;

import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;

public interface CommitLogReader
{
    List<PartitionUpdateWrapper> updates();

    CommitLog log();

    long segmentId();

    int position();

    boolean skipped();

    class Result
    {
        private final List<PartitionUpdateWrapper> updates;
        private final Marker marker;
        private final boolean isFullyRead;
        private final boolean skipped;

        public Result(CommitLogReader reader)
        {
            CommitLog log = reader.log();
            this.updates = reader.updates();
            this.marker = log.markerAt(reader.segmentId(), reader.position());
            // commit log has been fully written to, and we have read upto the max offset in this BufferingCommitLogReader
            this.isFullyRead = log.completed() && marker.position >= log.maxOffset();
            this.skipped = reader.skipped();
        }

        public List<PartitionUpdateWrapper> updates()
        {
            return updates;
        }

        public Marker marker()
        {
            return marker;
        }

        /**
         * @return true if finished fully reading this commit log segment, so we can proceed to the next.
         */
        public boolean isFullyRead()
        {
            return isFullyRead;
        }

        /**
         * @return true if commit log was skipped because it is before the startMarker or log is empty.
         */
        public boolean wasSkipped()
        {
            return skipped;
        }
    }
}
