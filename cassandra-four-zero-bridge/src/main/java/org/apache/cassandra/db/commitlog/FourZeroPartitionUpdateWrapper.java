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

package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.utils.TokenUtils;
import org.jetbrains.annotations.Nullable;

public class FourZeroPartitionUpdateWrapper extends PartitionUpdateWrapper
{
    private final PartitionUpdate update;

    public FourZeroPartitionUpdateWrapper(PartitionUpdate update, long maxTimestampMicros, @Nullable AsyncExecutor executor)
    {
        super(update.metadata().keyspace,
              update.metadata().name,
              TokenUtils.tokenToBigInteger(update.partitionKey().getToken()),
              update.dataSize(),
              () -> computeDigest(update),
              update.partitionKey().getKey(),
              maxTimestampMicros,
              executor);
        this.update = update;
    }

    public PartitionUpdate partitionUpdate()
    {
        return update;
    }

    public static byte[] computeDigest(PartitionUpdate update)
    {
        org.apache.cassandra.db.Digest digest = org.apache.cassandra.db.Digest.forReadResponse();
        UnfilteredRowIterators.digest(update.unfilteredIterator(), digest, MessagingService.current_version);
        return digest.digest();
    }
}
