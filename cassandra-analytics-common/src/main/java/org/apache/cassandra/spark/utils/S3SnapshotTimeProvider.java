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

/**
 * A {@link TimeProvider} for S3-based reads that sets the reference epoch from autosnap timestamps.
 * <p>
 * The reference epoch drives TTL and tombstone evaluation in {@code CompactionIterator}'s
 * {@code cell.isLive(nowInSec)} checks. To avoid silently dropping cells that were still alive
 * at the time of an earlier node's snapshot, the reference epoch should be the <b>earliest</b>
 * (minimum) autosnap epoch across all nodes. This ensures conservative TTL behavior: a cell
 * is only expired if it was already expired at every node's snapshot time.
 */
public class S3SnapshotTimeProvider implements TimeProvider
{
    private final long referenceEpochInSeconds;

    /**
     * @param referenceEpochInSeconds the earliest autosnap epoch in seconds across all nodes
     */
    public S3SnapshotTimeProvider(long referenceEpochInSeconds)
    {
        this.referenceEpochInSeconds = referenceEpochInSeconds;
    }

    @Override
    public long referenceEpochInSeconds()
    {
        return referenceEpochInSeconds;
    }
}
