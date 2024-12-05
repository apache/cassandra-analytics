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

package org.apache.cassandra.cdc.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.PerInstanceCommitLogMarkers;
import org.apache.cassandra.cdc.api.PerRangeCommitLogMarkers;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.util.CompressionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The CdcState object describes the cdc state that must be persisted between micro-batches to make cdc recoverable.
 * The key elements are of the state are:
 * 1) the commit log markers, indicating the maximal position reached for a given CassandraInstance.
 * Cdc should resume from this position after a restart to prevent replay published updates.
 * 2) the replica count, holds an MD5 digest and replica count for all updates that were not published because
 * insufficient replica copies were read in a previous microbatch.
 */
public class CdcState
{
    public static final Serializer SERIALIZER = new Serializer();
    public static final ReplicaCountSerializer REPLICA_COUNT_SERIALIZER = new ReplicaCountSerializer();
    public static final CdcState BLANK = CdcState.blank(null);

    // epoch - a monotonically increasing value for each microbatch of the cdc stream
    public final long epoch;
    // the token range for the cdc state
    @Nullable
    public final TokenRange range;
    // maximal position in the commit log successfully read to
    @NotNull
    public final CommitLogMarkers markers;
    // digest and count of mutations received where insufficient replica copies were read to satisfy the consistency level
    @NotNull
    public final Map<PartitionUpdateWrapper.Digest, Integer> replicaCount;

    public CdcState(long epoch,
                    @Nullable TokenRange range,
                    @NotNull CommitLogMarkers markers,
                    @NotNull Map<PartitionUpdateWrapper.Digest, Integer> replicaCount)
    {
        this.epoch = epoch;
        this.range = range;
        this.markers = markers;
        this.replicaCount = ImmutableMap.copyOf(replicaCount);
    }

    public int size()
    {
        return replicaCount.size();
    }

    public static CdcState blank(@Nullable TokenRange range)
    {
        return new CdcState(0L, range, CommitLogMarkers.EMPTY, ImmutableMap.of());
    }

    public static CdcState of(long epoch)
    {
        return of(epoch, null);
    }

    public static CdcState of(long epoch,
                              @Nullable TokenRange range)
    {
        return of(epoch, range, CommitLogMarkers.EMPTY, ImmutableMap.of());
    }

    public static CdcState of(long epoch,
                              @Nullable TokenRange range,
                              @NotNull CommitLogMarkers markers,
                              @NotNull Map<PartitionUpdateWrapper.Digest, Integer> replicaCount)
    {
        return new CdcState(epoch, range, markers, replicaCount);
    }

    public CdcState merge(@Nullable TokenRange range, CdcState other)
    {
        return CdcState.merge(range, this, other);
    }

    public Mutator mutate()
    {
        return new Mutator(this);
    }

    public boolean isFull(CdcOptions cdcOptions)
    {
        int maxCdcStateSize = cdcOptions.maxCdcStateSize();
        return maxCdcStateSize > 0 && size() >= maxCdcStateSize;
    }

    public CdcState purgeIfFull(ICdcStats stats, CdcOptions cdcOptions)
    {
        if (isFull(cdcOptions))
        {
            return mutate()
                   .purge(stats, cdcOptions.minimumTimestampMicros())
                   .build();
        }
        return this;
    }

    public String toString()
    {
        return "CdcState{" +
               "epoch=" + epoch +
               ", range=" + range +
               ", markers=" + markers +
               ", replicaCount=" + replicaCount +
               '}';
    }

    @SuppressWarnings("UnusedReturnValue") // Builder pattern
    public static class Mutator
    {
        @NotNull
        private final CdcState start;
        private long epoch;
        @NotNull
        private final PerInstanceCommitLogMarkers.PerInstanceBuilder markerBuilder;
        @Nullable
        private TokenRange range;
        @NotNull
        private final Map<PartitionUpdateWrapper.Digest, Integer> replicaCount;

        private Mutator(@NotNull CdcState start)
        {
            this.start = start;
            this.epoch = start.epoch;
            this.range = start.range;
            this.markerBuilder = start.markers.mutate();
            this.replicaCount = new HashMap<>(start.replicaCount);
        }

        public Mutator advanceMarker(CassandraInstance instance, Marker marker)
        {
            this.markerBuilder.advanceMarker(instance, marker);
            return this;
        }

        public int replicaCount(PartitionUpdateWrapper update)
        {
            return this.replicaCount(update.digest());
        }

        public int replicaCount(PartitionUpdateWrapper.Digest digest)
        {
            return replicaCount.getOrDefault(digest, 0);
        }

        public Mutator recordReplicaCount(PartitionUpdateWrapper updateWrapper, int replicaCount)
        {
            this.replicaCount.put(updateWrapper.digest(), replicaCount);
            return this;
        }

        public Mutator untrackReplicaCount(PartitionUpdateWrapper update)
        {
            return untrackReplicaCount(update.digest());
        }

        public Mutator untrackReplicaCount(PartitionUpdateWrapper.Digest digest)
        {
            replicaCount.remove(digest);
            return this;
        }

        public boolean seenBefore(PartitionUpdateWrapper update)
        {
            return seenBefore(update.digest());
        }

        public boolean seenBefore(PartitionUpdateWrapper.Digest digest)
        {
            return this.start.replicaCount.containsKey(digest);
        }

        public int size()
        {
            return replicaCount.size();
        }

        public boolean isFull(CdcOptions cdcOptions)
        {
            int maxCdcStateSize = cdcOptions.maxCdcStateSize();
            return maxCdcStateSize > 0 && size() >= maxCdcStateSize;
        }

        public Mutator nextEpoch()
        {
            this.epoch++;
            return this;
        }

        public Mutator withRange(@Nullable TokenRange range)
        {
            this.range = range;
            return this;
        }

        /**
         * Purge CdcState of expired mutations
         *
         * @param cdcStats             ICdcStats instance to emit KPIs about dropped mutations
         * @param startTimestampMicros the minimum timestamp, any mutation before this timestamp will be purged.
         * @return this Mutator
         */
        public Mutator purge(ICdcStats cdcStats, @Nullable Long startTimestampMicros)
        {
            if (startTimestampMicros == null)
            {
                return this;
            }

            final int[] count = {
            0
            };
            replicaCount
            .keySet()
            .removeIf(update -> {
                if (isExpired(update, startTimestampMicros))
                {
                    count[0]++;
                    return true;
                }
                return false;
            });
            cdcStats.droppedExpiredMutations(startTimestampMicros, count[0]);
            return this;
        }

        public CdcState build()
        {
            return new CdcState(epoch, range, markerBuilder.build(), replicaCount);
        }
    }

    public static boolean isExpired(@NotNull PartitionUpdateWrapper.Digest update,
                                    long minTimestampMicros)
    {
        return update.maxTimestampMicros() < minTimestampMicros;
    }

    /**
     * Merge two previous cdc states, filtered for the new token range.
     *
     * @param range  the new token range, if null then merge without filtering by token range.
     * @param state1 cdc state.
     * @param state2 cdc state.
     * @return a new cdc state object that merges the markers and watermarker state for the new token range.
     */
    public static CdcState merge(@Nullable TokenRange range, CdcState state1, CdcState state2)
    {
        return new CdcState(Math.max(state1.epoch, state2.epoch),
                            range,
                            mergeMarkers(state1, state2),
                            mergeReplicaCount(state1.replicaCount, state2.replicaCount));
    }

    public static Map<PartitionUpdateWrapper.Digest, Integer> mergeReplicaCount(Map<PartitionUpdateWrapper.Digest, Integer> w1,
                                                                                Map<PartitionUpdateWrapper.Digest, Integer> w2)
    {
        Map<PartitionUpdateWrapper.Digest, Integer> mergedMap = new HashMap<>(w1);
        for (Map.Entry<PartitionUpdateWrapper.Digest, Integer> entry : w2.entrySet())
        {
            mergedMap.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return mergedMap;
    }

    /**
     * @param state1 cdc state.
     * @param state2 cdc state.
     * @return a ICommitLogMarkers.PerRange merged object that tracks the min. position per token range.
     */
    public static CommitLogMarkers mergeMarkers(@NotNull CdcState state1, @NotNull CdcState state2)
    {
        return mergeMarkers(state1.markers, state1.range, state2.markers, state2.range);
    }

    public static CommitLogMarkers mergeMarkers(@NotNull CommitLogMarkers markers1,
                                                @Nullable TokenRange range1,
                                                @NotNull CommitLogMarkers markers2,
                                                @Nullable TokenRange range2)
    {
        if (range1 == null || range2 == null)
        {
            return CommitLogMarkers.of(markers1, markers2);
        }

        PerRangeCommitLogMarkers.PerRangeBuilder builder = CommitLogMarkers.perRangeBuilder();
        markers1.values().forEach(marker -> builder.add(range1, marker));
        markers2.values().forEach(marker -> builder.add(range2, marker));
        return builder.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epoch, range, markers, replicaCount);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (!(other instanceof CdcState))
        {
            return false;
        }

        CdcState that = (CdcState) other;
        return epoch == that.epoch
               && Objects.equals(range, that.range)
               && Objects.equals(markers, that.markers)
               && Objects.equals(replicaCount, that.replicaCount);
    }

    // Kryo Serializer

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CdcState>
    {
        @Override
        public void write(Kryo kryo, Output out, CdcState state)
        {
            out.writeLong(state.epoch);

            KryoUtils.writeRange(out, state.range);
            kryo.writeObject(out, state.markers, CommitLogMarkers.SERIALIZER);

            kryo.writeObject(out, state.replicaCount, CdcState.REPLICA_COUNT_SERIALIZER);
        }

        @Override
        public CdcState read(Kryo kryo, Input in, Class<CdcState> type)
        {
            long epoch = in.readLong();
            TokenRange range = KryoUtils.readRange(in);
            CommitLogMarkers markers = kryo.readObject(in, CommitLogMarkers.class, CommitLogMarkers.SERIALIZER);
            @SuppressWarnings("unchecked") Map<PartitionUpdateWrapper.Digest, Integer> replicaCount =
            (Map<PartitionUpdateWrapper.Digest, Integer>) kryo.readObject(in, Map.class, CdcState.REPLICA_COUNT_SERIALIZER);

            return new CdcState(epoch, range, markers, replicaCount);
        }
    }

    public static CdcState deserialize(Kryo kryo, CompressionUtil compressionUtil, byte[] compressed)
    {
        try
        {
            return KryoUtils.deserialize(kryo, compressionUtil.uncompress(compressed), CdcState.class, CdcState.SERIALIZER);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class ReplicaCountSerializer extends com.esotericsoftware.kryo.Serializer<Map<PartitionUpdateWrapper.Digest, Integer>>
    {
        public Map<PartitionUpdateWrapper.Digest, Integer> read(Kryo kryo, Input in, Class type)
        {
            // read replica counts
            int numUpdates = in.readShort();
            Map<PartitionUpdateWrapper.Digest, Integer> replicaCounts = new HashMap<>(numUpdates);
            for (int i = 0; i < numUpdates; i++)
            {
                replicaCounts.put(kryo.readObject(in, PartitionUpdateWrapper.Digest.class, PartitionUpdateWrapper.DIGEST_SERIALIZER), (int) in.readByte());
            }

            return replicaCounts;
        }

        public void write(Kryo kryo, Output out, Map<PartitionUpdateWrapper.Digest, Integer> o)
        {
            // write replica counts for late mutations
            out.writeShort(o.size());
            for (Map.Entry<PartitionUpdateWrapper.Digest, Integer> entry : o.entrySet())
            {
                PartitionUpdateWrapper.Digest digest = entry.getKey();
                kryo.writeObject(out, digest, PartitionUpdateWrapper.DIGEST_SERIALIZER);
                out.writeByte(entry.getValue());
            }
        }
    }
}
