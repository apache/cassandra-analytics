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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;

public class DbUtils
{
    private DbUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    // C* 4.0 DeletionTime constructor requires int for localDeletionTime; checked cast will throw after Y2038
    public static DeletionTime deletionTime(long markedForDeleteAt, long localDeletionTime)
    {
        return new DeletionTime(markedForDeleteAt, Ints.checkedCast(localDeletionTime));
    }

    public static LivenessInfo livenessInfo(long timestamp, long nowInSeconds)
    {
        // C* 4.0 LivenessInfo.create requires int for nowInSeconds; checked cast will throw after Y2038
        return LivenessInfo.create(timestamp, Ints.checkedCast(nowInSeconds));
    }

    public static PartitionUpdate fullPartitionDeletion(TableMetadata metadata, ByteBuffer key, long timestamp, long nowInSec)
    {
        // C* 4.0 fullPartitionDelete requires int for nowInSec; checked cast will throw after Y2038
        return PartitionUpdate.fullPartitionDelete(metadata, key, timestamp, Ints.checkedCast(nowInSec));
    }

    public static PartitionUpdate.SimpleBuilder partitionUpdateBuilderWithNow(TableMetadata metadata, DecoratedKey key, long nowInSec)
    {
        // C* 4.0 simpleBuilder.nowInSec requires int; checked cast will throw after Y2038
        return PartitionUpdate.simpleBuilder(metadata, key).nowInSec(Ints.checkedCast(nowInSec));
    }
}
