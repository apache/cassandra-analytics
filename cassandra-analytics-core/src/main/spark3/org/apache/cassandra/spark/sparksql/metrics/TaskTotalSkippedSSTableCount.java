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

import java.util.concurrent.atomic.LongAdder;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class TaskTotalSkippedSSTableCount implements CustomTaskMetric
{
    private final long value;

    private TaskTotalSkippedSSTableCount(long value)
    {
        this.value = value;
    }

    @Override
    public String name()
    {
        return TotalSkippedSSTableCount.NAME;
    }

    @Override
    public long value()
    {
        return value;
    }

    /**
     * Create a TaskTotalSkippedSSTableCount metric from accumulated count data.
     * This should be called at the end of a task to capture the total skipped SSTable count.
     *
     * @param totalSkippedSSTableCount total count of SSTable files skipped
     * @return TaskTotalSkippedSSTableCount instance with the count
     */
    public static TaskTotalSkippedSSTableCount from(long totalSkippedSSTableCount)
    {
        return new TaskTotalSkippedSSTableCount(totalSkippedSSTableCount);
    }

    /**
     * Create a TaskTotalSkippedSSTableCount metric from a LongAdder accumulator.
     * Convenience method for working with thread-local accumulation counters.
     *
     * @param totalSkippedSSTableCount LongAdder containing accumulated skipped SSTable count
     * @return TaskTotalSkippedSSTableCount instance with the count
     */
    public static TaskTotalSkippedSSTableCount from(LongAdder totalSkippedSSTableCount)
    {
        return from(totalSkippedSSTableCount.sum());
    }
}
