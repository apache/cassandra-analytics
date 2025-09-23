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

package org.apache.cassandra.cdc.watermarker;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import java.util.stream.Stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In-memory watermark implementation that caches position to start reading from each instance.
 * WARNING: this implementation is for local testing only and should not be used in a Spark cluster.
 * The task allocation in Spark cannot guarantee a partition will be assigned to the same executor.
 */
public class InMemoryWatermarker implements Watermarker
{
    @VisibleForTesting
    public static String testThreadName = null; // allow unit tests to bypass TaskContext check as no easy way to set ThreadLocal TaskContext
    private final TaskContextProvider taskContextProvider;

    public interface TaskContextProvider
    {
        boolean hasTaskContext();

        int partitionId();
    }

    public InMemoryWatermarker(TaskContextProvider taskContextProvider)
    {
        this.taskContextProvider = taskContextProvider;
    }

    // store watermarker per Spark job
    protected final Map<String, JobWatermarker> jobs = new ConcurrentHashMap<>();

    public Watermarker instance(String jobId)
    {
        return jobs.computeIfAbsent(jobId, this::newInstance).get();
    }

    public int size()
    {
        throw new IllegalCallerException();
    }

    public void recordReplicaCount(PartitionUpdateWrapper.Digest digest, int numReplicas)
    {
        throw new IllegalCallerException();
    }

    public int replicaCount(PartitionUpdateWrapper.Digest digest)
    {
        throw new IllegalCallerException();
    }

    // allow sub-classes to override with own implementation
    public JobWatermarker newInstance(String jobId)
    {
        return new JobWatermarker(jobId, taskContextProvider);
    }

    public void untrackReplicaCount(PartitionUpdateWrapper.Digest digest)
    {
        throw new IllegalCallerException();
    }

    public boolean seenBefore(PartitionUpdateWrapper.Digest digest)
    {
        throw new IllegalCallerException();
    }

    public void persist(@Nullable final Long minTimestampMicros, ICdcStats stats)
    {
        throw new IllegalCallerException();
    }

    public void clear()
    {
        jobs.values().forEach(JobWatermarker::clear);
        jobs.clear();
    }

    public void apply(SerializationWrapper wrapper)
    {
        throw new IllegalCallerException();
    }

    public SerializationWrapper serializationWrapper()
    {
        throw new IllegalCallerException();
    }

    /**
     * Stores per Spark partition watermarker for a given Spark job.
     */
    public static class JobWatermarker implements Watermarker
    {
        protected final String jobId;
        protected final TaskContextProvider taskContextProvider;

        protected final Map<Integer, PartitionWatermarker> watermarkers = new ConcurrentHashMap<>();

        public JobWatermarker(String jobId, TaskContextProvider taskContextProvider)
        {
            this.jobId = jobId;
            this.taskContextProvider = taskContextProvider;
        }

        public String jobId()
        {
            return jobId;
        }

        public Watermarker instance(String jobId)
        {
            Preconditions.checkArgument(this.jobId.equals(jobId));
            return get();
        }

        public int size()
        {
            return get().size();
        }

        public void recordReplicaCount(PartitionUpdateWrapper.Digest digest, int numReplicas)
        {
            get().recordReplicaCount(digest, numReplicas);
        }

        public int replicaCount(PartitionUpdateWrapper.Digest digest)
        {
            return get().replicaCount(digest);
        }

        public void untrackReplicaCount(PartitionUpdateWrapper.Digest digest)
        {
            get().untrackReplicaCount(digest);
        }

        public boolean seenBefore(PartitionUpdateWrapper.Digest digest)
        {
            return get().seenBefore(digest);
        }

        public void persist(@Nullable final Long minTimestampMicros, ICdcStats stats)
        {
            get().persist(minTimestampMicros, stats);
        }

        public void clear()
        {
            watermarkers.values().forEach(Watermarker::clear);
            watermarkers.clear();
        }

        public PartitionWatermarker get()
        {
            if (!Thread.currentThread().getName().equals(testThreadName))
            {
                Preconditions.checkArgument(taskContextProvider.hasTaskContext(), "This method must be called by a Spark executor thread");
            }
            return watermarkers.computeIfAbsent(taskContextProvider.partitionId(), this::newInstance);
        }

        // allow sub-classes to override with own implementation
        public PartitionWatermarker newInstance(int partitionId)
        {
            return new PartitionWatermarker(partitionId);
        }

        public void apply(SerializationWrapper wrapper)
        {
            get().apply(wrapper);
        }

        public SerializationWrapper serializationWrapper()
        {
            return get().serializationWrapper();
        }
    }

    /**
     * Tracks highwater mark per instance and number of replicas previously received for updates that did not achieve the consistency level.
     */
    public static class PartitionWatermarker implements Watermarker
    {
        // tracks replica count for mutations with insufficient replica copies
        protected final Map<PartitionUpdateWrapper.Digest, Integer> replicaCount = new ConcurrentHashMap<>(1024);
        // high watermark tracks how far we have read in the CommitLogs per CassandraInstance

        final int partitionId;

        public PartitionWatermarker(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int partitionId()
        {
            return partitionId;
        }

        public Watermarker instance(String jobId)
        {
            return this;
        }

        public int size()
        {
            return replicaCount.size();
        }

        public void recordReplicaCount(PartitionUpdateWrapper.Digest digest, int numReplicas)
        {
            replicaCount.put(digest, numReplicas);
        }

        public int replicaCount(PartitionUpdateWrapper.Digest digest)
        {
            return replicaCount.getOrDefault(digest, 0);
        }

        public void untrackReplicaCount(PartitionUpdateWrapper.Digest digest)
        {
            replicaCount.remove(digest);
        }

        public boolean seenBefore(PartitionUpdateWrapper.Digest digest)
        {
            return replicaCount.containsKey(digest);
        }

        public void persist(@Nullable final Long minTimestampMicros, ICdcStats stats)
        {
            if (minTimestampMicros == null)
            {
                return;
            }
            final int[] count = {0};
            replicaCount.keySet().removeIf(u -> {
                if (isExpired(u, minTimestampMicros))
                {
                    count[0]++;
                    return true;
                }
                return false;
            });
            stats.droppedExpiredMutations(minTimestampMicros, count[0]);
        }

        public void clear()
        {
            replicaCount.clear();
        }

        public boolean isExpired(@NotNull final PartitionUpdateWrapper.Digest update,
                                 @Nullable final Long minTimestampMicros)
        {
            return minTimestampMicros != null && update.maxTimestampMicros() < minTimestampMicros;
        }

        public void apply(SerializationWrapper wrapper)
        {
            this.replicaCount.putAll(wrapper.replicaCount);
        }

        public SerializationWrapper serializationWrapper()
        {
            return new SerializationWrapper(replicaCount);
        }
    }

    public static class SerializationWrapper implements Serializable
    {
        public final ImmutableMap<PartitionUpdateWrapper.Digest, Integer> replicaCount;

        public SerializationWrapper()
        {
            this(ImmutableMap.of());
        }

        public SerializationWrapper(Map<PartitionUpdateWrapper.Digest, Integer> replicaCount)
        {
            this.replicaCount = ImmutableMap.copyOf(replicaCount);
        }

        /**
         * Filter SerializationWrapper to return a new SerializationWrapper that only contains mutations that overlap with given token range.
         *
         * @param byRange the token range that we are interested in.
         * @param wrapper original SerializationWrapper
         * @return new SerializationWrapper that only contains mutations that overlap with range.
         */
        public static SerializationWrapper filter(@Nullable Range<BigInteger> byRange, SerializationWrapper wrapper)
        {
            if (byRange == null)
            {
                return wrapper;
            }
            return new SerializationWrapper(
            wrapper.replicaCount.entrySet().stream()
                                .filter(e -> byRange.contains(e.getKey().token()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        /**
         * Merge two SerializationWrapper into one, taking the max replica copies received across both states.
         *
         * @param w1 SerializationWrapper
         * @param w2 SerializationWrapper
         * @return new SerializationWrapper that merges
         */
        public static SerializationWrapper merge(SerializationWrapper w1, SerializationWrapper w2)
        {
            final Map<PartitionUpdateWrapper.Digest, Integer> replicaCount =
            Stream.concat(w1.replicaCount.entrySet().stream(), w2.replicaCount.entrySet().stream())
                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max));
            return new SerializationWrapper(replicaCount);
        }

        public SerializationWrapper filter(Range<BigInteger> byRange)
        {
            return SerializationWrapper.filter(byRange, this);
        }

        public SerializationWrapper merge(SerializationWrapper with)
        {
            return SerializationWrapper.merge(this, with);
        }

        public static class Serializer extends com.esotericsoftware.kryo.Serializer<SerializationWrapper>
        {
            public static final Serializer INSTANCE = new Serializer();

            private static final PartitionUpdateWrapper.DigestSerializer DIGEST_SERIALIZER = new PartitionUpdateWrapper.DigestSerializer();

            public SerializationWrapper read(Kryo kryo, Input in, Class type)
            {
                // read replica counts
                final int numUpdates = in.readShort();
                final Map<PartitionUpdateWrapper.Digest, Integer> replicaCounts = new HashMap<>(numUpdates);
                for (int i = 0; i < numUpdates; i++)
                {
                    replicaCounts.put(kryo.readObject(in, PartitionUpdateWrapper.Digest.class, DIGEST_SERIALIZER), (int) in.readByte());
                }

                return new SerializationWrapper(replicaCounts);
            }

            public void write(Kryo kryo, Output out, SerializationWrapper o)
            {
                // write replica counts for late mutations
                out.writeShort(o.replicaCount.size());
                for (final Map.Entry<PartitionUpdateWrapper.Digest, Integer> entry : o.replicaCount.entrySet())
                {
                    PartitionUpdateWrapper.Digest digest = entry.getKey();
                    kryo.writeObject(out, digest, DIGEST_SERIALIZER);
                    out.writeByte(entry.getValue());
                }
            }
        }
    }
}
