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

package org.apache.cassandra.spark.sparksql.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class TaskTotalS3GetObjectDuration implements CustomTaskMetric
{
    private final long value;

    private TaskTotalS3GetObjectDuration(long value)
    {
        this.value = value;
    }

    @Override
    public String name()
    {
        return TotalS3GetObjectDuration.NAME;
    }

    @Override
    public long value()
    {
        return value;
    }

    /**
     * Create a TaskTotalS3GetObjectDuration metric from accumulated timing data.
     * This should be called at the end of a task to capture the total S3 getObject time.
     *
     * @param totalS3GetObjectNanos total time spent on S3 getObject operations in nanoseconds
     * @return TaskTotalS3GetObjectDuration instance with the time converted to milliseconds
     */
    public static TaskTotalS3GetObjectDuration from(long totalS3GetObjectNanos)
    {
        long millis = totalS3GetObjectNanos > 0 ? TimeUnit.NANOSECONDS.toMillis(totalS3GetObjectNanos) : 0;
        return new TaskTotalS3GetObjectDuration(millis);
    }

    /**
     * Create a TaskTotalS3GetObjectDuration metric from a LongAdder accumulator.
     * Convenience method for working with thread-local accumulation counters.
     *
     * @param totalS3GetObjectNanos LongAdder containing accumulated S3 getObject time in nanoseconds
     * @return TaskTotalS3GetObjectDuration instance with the time converted to milliseconds
     */
    public static TaskTotalS3GetObjectDuration from(LongAdder totalS3GetObjectNanos)
    {
        return from(totalS3GetObjectNanos.sum());
    }
}
