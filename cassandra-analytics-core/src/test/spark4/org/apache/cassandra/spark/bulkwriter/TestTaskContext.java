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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Closeable;
import java.util.Properties;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.metrics.source.Source;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.TaskFailureListener;
import scala.Function0;
import scala.Option;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

public class TestTaskContext extends TaskContext
{
    @Override
    public boolean isCompleted()
    {
        return false;
    }

    @Override
    public boolean isFailed()
    {
        return false;
    }

    @Override
    public boolean isInterrupted()
    {
        return false;
    }

    @Deprecated
    public boolean isRunningLocally()
    {
        return false;
    }

    @Override
    public TaskContext addTaskCompletionListener(TaskCompletionListener listener)
    {
        return null;
    }

    @Override
    public TaskContext addTaskFailureListener(TaskFailureListener listener)
    {
        return null;
    }

    @Override
    public int stageId()
    {
        return 0;
    }

    @Override
    public int stageAttemptNumber()
    {
        return 0;
    }

    @Override
    public int partitionId()
    {
        return 0;
    }

    @Override
    public int numPartitions()
    {
        return 0;
    }

    @Override
    public int attemptNumber()
    {
        return 0;
    }

    @Override
    public long taskAttemptId()
    {
        return 0;
    }

    @Override
    public String getLocalProperty(String key)
    {
        return null;
    }

    @Override
    public int cpus()
    {
        return 1;
    }

    public Map<String, ResourceInformation> resources()
    {
        return null;
    }

    public java.util.Map<String, ResourceInformation> resourcesJMap()
    {
        return null;
    }

    @Override
    public TaskMetrics taskMetrics()
    {
        return null;
    }

    @Override
    public Seq<Source> getMetricsSources(String sourceName)
    {
        return null;
    }

    @Override
    public void killTaskIfInterrupted()
    {
    }

    @Override
    public Option<String> getKillReason()
    {
        return null;
    }

    @Override
    public TaskMemoryManager taskMemoryManager()
    {
        return null;
    }

    @Override
    public void registerAccumulator(AccumulatorV2 accumulator)
    {
    }

    @Override
    public void setFetchFailed(FetchFailedException fetchFailed)
    {
    }

    @Override
    public void markInterrupted(String reason)
    {
    }

    @Override
    public void markTaskFailed(Throwable error)
    {
    }

    @Override
    public void markTaskCompleted(Option<Throwable> error)
    {
    }

    @Override
    public Option<FetchFailedException> fetchFailed()
    {
        return null;
    }

    @Override
    public Properties getLocalProperties()
    {
        return null;
    }

    @Override
    public boolean interruptible()
    {
        return false;
    }

    @Override
    public void pendingInterrupt(Option<Thread> threadToInterrupt, String reason)
    {

    }

    @Override
    public <T extends Closeable> T createResourceUninterruptibly(Function0<T> resourceBuilder)
    {
        return null;
    }
}
