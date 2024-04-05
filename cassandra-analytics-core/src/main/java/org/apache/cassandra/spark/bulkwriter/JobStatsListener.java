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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.StatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.JobEventDetail;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * Spark listener implementation to capture stats on completion of jobs and tasks.
 */
public class JobStatsListener extends SparkListener
{
    private final Map<Integer, Integer> jobIdToTaskRetryStats = new HashMap<>();
    private final Map<Integer, Set<TaskMetrics>> jobIdToTaskMetrics = new HashMap<>();
    private final Map<Integer, Long> jobIdToStartTimes = new HashMap<>();
    private final Map<Integer, String> internalJobIdMapping = new HashMap<>();

    private static int jobId = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobStatsListener.class);
    private final Consumer<JobEventDetail> jobCompletionConsumer;

    public JobStatsListener(Consumer<JobEventDetail> jobCompletionConsumer)
    {
        this.jobCompletionConsumer = jobCompletionConsumer;
    }

    @Override
    public synchronized void onTaskEnd(SparkListenerTaskEnd taskEnd)
    {
        // Calculate max task retries across all tasks in job
        int attempt = taskEnd.taskInfo().attemptNumber();
        jobIdToTaskRetryStats.compute(jobId, (k, v) -> (v == null || attempt > v) ? attempt : v);
        // Persist all task metrics for the job - across all stages
        jobIdToTaskMetrics.computeIfAbsent(jobId, k -> new HashSet<>()).add(taskEnd.taskMetrics());
        LOGGER.debug("Task END for jobId:{} task:{} task attempt:{}} Reason:{}",
                    jobId,
                    taskEnd.taskInfo().taskId(),
                    taskEnd.taskInfo().attemptNumber(),
                    taskEnd.reason());
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart)
    {
        String internalJobId = (String) jobStart.properties().get("spark.jobGroup.id");
        jobId = Integer.valueOf(jobStart.jobId());
        internalJobIdMapping.put(jobId, internalJobId);
        jobIdToStartTimes.put(jobId, System.nanoTime());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd)
    {
        boolean jobFailed = false;
        String reason = "null";
        if (jobEnd.jobResult() instanceof JobFailed)
        {
            jobFailed = true;
            JobFailed result = (JobFailed) jobEnd.jobResult();
            reason = result.exception().getCause().getMessage();
        }

        long elapsedTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - jobIdToStartTimes.get(jobId));
        String internalJobId = internalJobIdMapping.get(jobId);
        String jobStatus = (jobFailed) ? "Failed" : "Succeeded";
        Map<String, String> jobStats = new HashMap<>();
        jobStats.put("jobId", internalJobId);
        jobStats.put("jobStatus", jobStatus);
        jobStats.put("failureReason", reason);
        jobStats.put("jobElapsedTimeMillis", String.valueOf(elapsedTimeMillis));

        LOGGER.debug("Job END for jobId:{} status:{} Reason:{} ElapsedTime: {}",
                    jobId,
                    jobStatus,
                    reason,
                    elapsedTimeMillis);

        jobStats.putAll(getJobMetrics(jobId));
        jobCompletionConsumer.accept(new JobEventDetail(internalJobId, jobStats));
        cleanup(jobId);
    }

    public synchronized Map<String, String> getJobMetrics(int jobId)
    {
        Map<String, String> jobMetrics = new HashMap<>();
        if (jobIdToTaskMetrics.containsKey(jobId))
        {
            List<Long> runTimes = jobIdToTaskMetrics.get(jobId)
                                                    .stream()
                                                    .map(TaskMetrics::executorRunTime)
                                                    .collect(Collectors.toList());

            double[] runTimesArray = runTimes.stream().mapToDouble(Long::doubleValue).toArray();
            jobMetrics.put("maxTaskRuntimeMillis", String.valueOf(StatUtils.max(runTimesArray)));
            jobMetrics.put("meanTaskRuntimeMillis", String.valueOf(StatUtils.mean(runTimesArray)));
            jobMetrics.put("p50TaskRuntimeMillis", String.valueOf(StatUtils.percentile(runTimesArray, 50)));
            jobMetrics.put("p95TaskRuntimeMillis", String.valueOf(StatUtils.percentile(runTimesArray, 95)));
            jobMetrics.put("maxTaskRetriesMillis", String.valueOf(jobIdToTaskRetryStats.get(jobId)));
        }
        return jobMetrics;
    }

    private void cleanup(int jobId)
    {
        jobIdToStartTimes.remove(jobId);
        internalJobIdMapping.remove(jobId);
        jobIdToTaskMetrics.remove(jobId);
        jobIdToTaskRetryStats.remove(jobId);
    }
}
