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

package org.apache.cassandra.cdc.watermarker;

import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.Nullable;

/**
 * Watermarker that does nothing.
 */
public class DoNothingWatermarker implements Watermarker
{
    public static final DoNothingWatermarker INSTANCE = new DoNothingWatermarker();

    public Watermarker instance(String jobId)
    {
        return this;
    }

    public int size()
    {
        return 0;
    }

    public void recordReplicaCount(PartitionUpdateWrapper.Digest digest, int numReplicas)
    {

    }

    public int replicaCount(PartitionUpdateWrapper.Digest digest)
    {
        return 0;
    }

    public void untrackReplicaCount(PartitionUpdateWrapper.Digest digest)
    {

    }

    public boolean seenBefore(PartitionUpdateWrapper.Digest digest)
    {
        return false;
    }

    public void updateHighWaterMark(Marker marker)
    {

    }

    @Nullable
    public Marker highWaterMark(CassandraInstance instance)
    {
        return null;
    }

    public void persist(@Nullable Long minTimestampMicros, ICdcStats stats)
    {
    }

    public void clear()
    {

    }
}
