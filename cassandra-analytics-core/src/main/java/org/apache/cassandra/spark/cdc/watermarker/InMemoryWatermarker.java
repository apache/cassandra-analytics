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

package org.apache.cassandra.spark.cdc.watermarker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In-memory watermark implementation that caches position to start reading from each instance.
 *
 * WARNING: This implementation is for local testing only and should not be used in a Spark cluster.
 *          The task allocation in Spark cannot guarantee a partition will be assigned to the same executor.
 */
@ThreadSafe
public class InMemoryWatermarker implements Watermarker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryWatermarker.class);

    public static final InMemoryWatermarker INSTANCE = new InMemoryWatermarker();

    @VisibleForTesting
    public static String TEST_THREAD_NAME = null;
    // CHECKSTYLE IGNORE: Non-final to allow bypassing of TaskContext check by unit tests, this is still a constant

    // Store watermarker per Spark job
    protected final Map<String, JobWatermarker> jobs = new ConcurrentHashMap<>();

    @Override
    public Watermarker instance(String jobId)
    {
        return jobs.computeIfAbsent(jobId, this::newInstance).get();
    }

    @Override
    public void recordReplicaCount(IPartitionUpdateWrapper update, int numReplicas)
    {
        throw new IllegalAccessError();
    }

    @Override
    public int replicaCount(IPartitionUpdateWrapper update)
    {
        throw new IllegalAccessError();
    }

    // Allow sub-classes to override with own implementation
    public JobWatermarker newInstance(String jobId)
    {
        return new JobWatermarker(jobId);
    }

    @Override
    public void untrackReplicaCount(IPartitionUpdateWrapper update)
    {
        throw new IllegalAccessError();
    }

    @Override
    public boolean seenBefore(IPartitionUpdateWrapper update)
    {
        throw new IllegalAccessError();
    }

    @Override
    public void updateHighWaterMark(CommitLog.Marker marker)
    {
        throw new IllegalAccessError();
    }

    @Override
    @Nullable
    public CommitLog.Marker highWaterMark(CassandraInstance instance)
    {
        throw new IllegalAccessError();
    }

    @Override
    public void persist(@Nullable Long maxAgeMicros)
    {
        throw new IllegalAccessError();
    }

    @Override
    public void clear()
    {
        jobs.values().forEach(JobWatermarker::clear);
        jobs.clear();
    }

    /**
     * Stores per Spark partition watermarker for a given Spark job.
     */
    @ThreadSafe
    public static class JobWatermarker implements Watermarker
    {
        protected final String jobId;
        protected final Map<Integer, PartitionWatermarker> watermarkers = new ConcurrentHashMap<>();

        public JobWatermarker(String jobId)
        {
            this.jobId = jobId;
        }

        @SuppressWarnings("unused")
        public String jobId()
        {
            return jobId;
        }

        @Override
        public Watermarker instance(String jobId)
        {
            Preconditions.checkArgument(this.jobId.equals(jobId));
            return get();
        }

        @Override
        public void recordReplicaCount(IPartitionUpdateWrapper update, int numReplicas)
        {
            get().recordReplicaCount(update, numReplicas);
        }

        @Override
        public int replicaCount(IPartitionUpdateWrapper update)
        {
            return get().replicaCount(update);
        }

        @Override
        public void untrackReplicaCount(IPartitionUpdateWrapper update)
        {
            get().untrackReplicaCount(update);
        }

        @Override
        public boolean seenBefore(IPartitionUpdateWrapper update)
        {
            return get().seenBefore(update);
        }

        @Override
        public void updateHighWaterMark(CommitLog.Marker marker)
        {
            get().updateHighWaterMark(marker);
        }

        @Override
        public CommitLog.Marker highWaterMark(CassandraInstance instance)
        {
            return get().highWaterMark(instance);
        }

        @Override
        public void persist(@Nullable Long maxAgeMicros)
        {
            get().persist(maxAgeMicros);
        }

        @Override
        public void clear()
        {
            watermarkers.values().forEach(Watermarker::clear);
            watermarkers.clear();
        }

        public PartitionWatermarker get()
        {
            if (!Thread.currentThread().getName().equals(TEST_THREAD_NAME))
            {
                Preconditions.checkNotNull(TaskContext.get(), "This method must be called by a Spark executor thread");
            }
            return watermarkers.computeIfAbsent(TaskContext.getPartitionId(), this::newInstance);
        }

        // Allow sub-classes to override with own implementation
        public PartitionWatermarker newInstance(int partitionId)
        {
            return new PartitionWatermarker(partitionId);
        }
    }

    /**
     * Tracks highwater mark per instance and number of replicas previously received for updates that did not achieve the consistency level
     */
    public static class PartitionWatermarker implements Watermarker
    {
        // Tracks replica count for mutations with insufficient replica copies
        protected final Map<IPartitionUpdateWrapper, Integer> replicaCount = new ConcurrentHashMap<>(1024);
        // High watermark tracks how far we have read in the CommitLogs per CassandraInstance
        protected final Map<CassandraInstance, CommitLog.Marker> highWatermarks = new ConcurrentHashMap<>();

        int partitionId;

        public PartitionWatermarker(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int partitionId()
        {
            return partitionId;
        }

        @Override
        public Watermarker instance(String jobId)
        {
            return this;
        }

        @Override
        public void recordReplicaCount(IPartitionUpdateWrapper update, int numReplicas)
        {
            replicaCount.put(update, numReplicas);
        }

        @Override
        public int replicaCount(IPartitionUpdateWrapper update)
        {
            return replicaCount.getOrDefault(update, 0);
        }

        @Override
        public void untrackReplicaCount(IPartitionUpdateWrapper update)
        {
            replicaCount.remove(update);
        }

        @Override
        public boolean seenBefore(IPartitionUpdateWrapper update)
        {
            return replicaCount.containsKey(update);
        }

        @Override
        public void updateHighWaterMark(CommitLog.Marker marker)
        {
            // This method will be called by executor thread when reading through CommitLog
            // so use AtomicReference to ensure thread-safe and visible to other threads
            if (marker == highWatermarks.merge(marker.instance(), marker,
                    (oldValue, newValue) -> newValue.compareTo(oldValue) > 0 ? newValue : oldValue))
            {
                LOGGER.debug("Updated highwater mark instance={} marker='{}' partitionId={}",
                             marker.instance().nodeName(), marker, partitionId());
            }
        }

        @Override
        public CommitLog.Marker highWaterMark(CassandraInstance instance)
        {
            return highWatermarks.get(instance);
        }

        @Override
        public void persist(@Nullable Long maxAgeMicros)
        {
            replicaCount.keySet().removeIf(u -> isExpired(u, maxAgeMicros));
        }

        @Override
        public void clear()
        {
            replicaCount.clear();
            highWatermarks.clear();
        }

        public boolean isExpired(@NotNull IPartitionUpdateWrapper update, @Nullable Long maxAgeMicros)
        {
            return maxAgeMicros != null && update.maxTimestampMicros() < maxAgeMicros;
        }
    }
}
