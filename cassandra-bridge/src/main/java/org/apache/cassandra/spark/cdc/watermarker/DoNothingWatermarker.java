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

package org.apache.cassandra.spark.cdc.watermarker;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.Nullable;

/**
 * Watermarker that does nothing
 */
public class DoNothingWatermarker implements Watermarker
{
    public static final DoNothingWatermarker INSTANCE = new DoNothingWatermarker();

    @Override
    public Watermarker instance(String jobId)
    {
        return this;
    }

    @Override
    public void recordReplicaCount(IPartitionUpdateWrapper update, int numReplicas)
    {
    }

    @Override
    public int replicaCount(IPartitionUpdateWrapper update)
    {
        return 0;
    }

    @Override
    public void untrackReplicaCount(IPartitionUpdateWrapper update)
    {
    }

    @Override
    public boolean seenBefore(IPartitionUpdateWrapper update)
    {
        return false;
    }

    @Override
    public void updateHighWaterMark(CommitLog.Marker marker)
    {
    }

    @Override
    @Nullable
    public CommitLog.Marker highWaterMark(CassandraInstance instance)
    {
        return null;
    }

    @Override
    public void persist(@Nullable Long maxAgeMicros)
    {
    }

    @Override
    public void clear()
    {
    }
}
