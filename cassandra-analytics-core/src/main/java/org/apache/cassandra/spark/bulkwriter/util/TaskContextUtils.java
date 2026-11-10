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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.spark.TaskContext;

public final class TaskContextUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskContextUtils.class);

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
     * Read back the task attempt number encoded by {@link #createStreamSessionId(TaskContext)}.
     *
     * @param streamSessionId stream session id
     * @return the task attempt number, or {@code 0} for any id not produced by {@code createStreamSessionId}
     */
    public static int attemptNumberFromStreamSessionId(String streamSessionId)
    {
        if (streamSessionId == null || streamSessionId.isEmpty())
        {
            return 0;
        }
        int firstSeparator = streamSessionId.indexOf('-');
        if (firstSeparator < 0)
        {
            return undecodable(streamSessionId);
        }
        int secondSeparator = streamSessionId.indexOf('-', firstSeparator + 1);
        if (secondSeparator < 0)
        {
            return undecodable(streamSessionId);
        }
        try
        {
            return Integer.parseInt(streamSessionId.substring(firstSeparator + 1, secondSeparator));
        }
        catch (NumberFormatException exception)
        {
            return undecodable(streamSessionId);
        }
    }

    private static int undecodable(String streamSessionId)
    {
        LOGGER.warn("Could not read task attempt number from stream session id '{}'. Falling back to 0.",
                    streamSessionId);
        return 0;
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
