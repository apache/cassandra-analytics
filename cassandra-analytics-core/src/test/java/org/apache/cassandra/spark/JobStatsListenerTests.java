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

package org.apache.cassandra.spark;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.common.stats.JobStatsListener;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskEndReason;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
import org.mockito.Mockito;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

public class JobStatsListenerTests
{
    private SparkContext sparkContext;

    @BeforeEach
    public void setUp()
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        sparkContext = new SparkContext(conf);
    }

    @AfterEach
    public void tearDown()
    {
        sparkContext.stop();
    }

    @Test
    public void testOnEventHandling()
    {

        List<StageInfo> stageInfoList = Collections.emptyList();
        Seq<StageInfo> stageInfoSeq = JavaConverters.asScalaIteratorConverter(stageInfoList.iterator())
                                                    .asScala().toSeq();
        AtomicReference<JobEventDetail> jobEvent = new AtomicReference<>();
        String jobId = UUID.randomUUID().toString();

        JobStatsListener listener = new JobStatsListener((jobEventDetail) -> {
            System.out.println("Job End Called");
            jobEvent.set(jobEventDetail);
        });

        Properties p = new Properties();
        p.setProperty("spark.jobGroup.id", jobId);
        SparkListenerJobStart jobStartEvent = new SparkListenerJobStart(1,
                                                                        System.currentTimeMillis(),
                                                                        stageInfoSeq,
                                                                        p);
        listener.onJobStart(jobStartEvent);
        listener.onTaskEnd(createMockTaskEndEvent());
        SparkListenerJobEnd mockJobEnd = Mockito.mock(SparkListenerJobEnd.class);
        listener.onJobEnd(mockJobEnd);

        assertThat(jobEvent.get().internalJobID()).isEqualTo(jobId);
        assertThat(jobEvent.get().jobStats().get("jobId")).isEqualTo(jobId);
        assertThat(jobEvent.get().jobStats().get("jobStatus")).isEqualTo("Succeeded");
        assertThat(jobEvent.get().jobStats().get("maxTaskRetriesMillis")).isEqualTo("1");
        assertThat(jobEvent.get().jobStats().get("p50TaskRuntimeMillis")).isEqualTo("1.0");
        assertThat(jobEvent.get().jobStats().get("meanTaskRuntimeMillis")).isEqualTo("1.0");
        assertThat(jobEvent.get().jobStats().get("maxTaskRuntimeMillis")).isEqualTo("1.0");
        assertThat(jobEvent.get().jobStats().get("p95TaskRuntimeMillis")).isEqualTo("1.0");

        // Validate that the previous job's metrics are reset after job completion
        listener.onJobStart(jobStartEvent);
        listener.onJobEnd(mockJobEnd);
        assertFalse(jobEvent.get().jobStats().containsKey("maxTaskRetriesMillis"));
        assertFalse(jobEvent.get().jobStats().containsKey("p50TaskRuntimeMillis"));
        assertFalse(jobEvent.get().jobStats().containsKey("meanTaskRuntimeMillis"));
        assertFalse(jobEvent.get().jobStats().containsKey("maxTaskRuntimeMillis"));
        assertFalse(jobEvent.get().jobStats().containsKey("p95TaskRuntimeMillis"));

    }

    private SparkListenerTaskEnd createMockTaskEndEvent()
    {
        SparkListenerTaskEnd mockTaskEnd = Mockito.mock(SparkListenerTaskEnd.class);
        TaskMetrics mTaskMetrics = Mockito.mock(TaskMetrics.class);
        when(mTaskMetrics.executorRunTime()).thenReturn(1L);
        TaskInfo mTaskInfo = Mockito.mock(TaskInfo.class);
        when(mTaskInfo.taskId()).thenReturn(1L);
        when(mTaskInfo.attemptNumber()).thenReturn(1);

        TaskEndReason mTaskEndReason = Mockito.mock(TaskEndReason.class);
        when(mTaskEndReason.toString()).thenReturn("Success");

        when(mockTaskEnd.taskMetrics()).thenReturn(mTaskMetrics);
        when(mockTaskEnd.taskInfo()).thenReturn(mTaskInfo);
        when(mockTaskEnd.reason()).thenReturn(mTaskEndReason);
        return mockTaskEnd;
    }
}
