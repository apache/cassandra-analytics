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

package org.apache.cassandra.spark.bulkwriter.util;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.spark.TaskContext;

public final class TaskContextUtils
{
    private TaskContextUtils()
    {
    }

    public static Range<BigInteger> getTokenRange(TaskContext taskContext, JobInfo job)
    {
        return job.getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    /**
     * Create a new stream session identifier
     * <p>
     * Note that the stream session id is used as part of filename. It cannot contain invalid characters for filename.
     * @param taskContext task context
     * @return a new stream ID
     */
    public static String createStreamSessionId(TaskContext taskContext)
    {
        return String.format("%d-%d-%s", taskContext.partitionId(), taskContext.attemptNumber(), UUID.randomUUID());
    }

    /**
     * Create a path that is unique to the spark partition
     * @return path
     */
    public static Path getPartitionUniquePath(String basePath, String jobId, TaskContext taskContext)
    {
        return Paths.get(basePath,
                         jobId,
                         Integer.toString(taskContext.stageAttemptNumber()),
                         Integer.toString(taskContext.attemptNumber()),
                         Integer.toString(taskContext.partitionId()));
    }
}
