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

package org.apache.cassandra.cdc.api;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CommitLogMarkers extends Serializable
{
    PerInstance EMPTY = new PerInstance(ImmutableMap.of());
    Serializer SERIALIZER = new Serializer();

    @NotNull
    default Marker startMarker(CommitLog log)
    {
        return startMarker(log.instance());
    }

    /**
     * @param instance CassandraInstance
     * @return minimum CommitLog Marker for a CassandraInstance to start reading from.
     */
    @NotNull
    Marker startMarker(CassandraInstance instance);

    /**
     * @param position current position in the CommitLog
     * @param token    Cassandra token of current mutation.
     * @return true if we can ignore this token if we have already read passed this position.
     */
    boolean canIgnore(Marker position, BigInteger token);

    static PerInstance of(@Nullable final Marker marker)
    {
        if (marker == null)
        {
            return EMPTY;
        }
        return of(ImmutableMap.of(marker.instance(), marker));
    }

    static PerInstance of(Map<CassandraInstance, Marker> markers)
    {
        return new PerInstance(markers);
    }

    static PerInstance of(CommitLogMarkers markers1,
                          CommitLogMarkers markers2)
    {
        return new PerInstance(markers1.values(), markers2.values());
    }

    boolean isEmpty();

    Collection<Marker> values();

    PerInstanceBuilder mutate();

    static PerInstanceBuilder perInstanceBuilder()
    {
        return new PerInstanceBuilder();
    }

    static PerRangeBuilder perRangeBuilder()
    {
        return new PerRangeBuilder();
    }

    class PerRangeBuilder
    {
        private final Map<CassandraInstance, Map<TokenRange, Marker>> markers;

        public PerRangeBuilder()
        {
            this.markers = new HashMap<>();
        }

        public PerRangeBuilder add(@NotNull final TokenRange tokenRange,
                                   @NotNull final Marker marker)
        {
            markers.computeIfAbsent(marker.instance(), (inst) -> new HashMap<>()).put(tokenRange, marker);
            return this;
        }

        public PerRange build()
        {
            return new PerRange(this.markers);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(markers);
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this)
            {
                return true;
            }
            if (!(other instanceof PerRangeBuilder))
            {
                return false;
            }

            PerRangeBuilder that = (PerRangeBuilder) other;
            return Objects.equals(markers, that.markers);
        }
    }

    /**
     * Stores CommitLog markers per CassandraInstance, taking the minimum marker when there are duplicates.
     */
    class PerInstance implements CommitLogMarkers
    {
        final Map<CassandraInstance, Marker> markers;

        public PerInstance(Collection<Marker> markers1, Collection<Marker> markers2)
        {
            this(
            Stream.concat(markers1.stream(), markers2.stream())
                  .collect(Collectors.toMap(Marker::instance, Function.identity(), Marker::min))
            );
        }

        public PerInstance(Map<CassandraInstance, Marker> markers)
        {
            this.markers = ImmutableMap.copyOf(markers);
        }

        @NotNull
        public Marker startMarker(CassandraInstance instance)
        {
            return Optional.ofNullable(markers.get(instance))
                           .orElseGet(instance::zeroMarker);
        }

        public boolean canIgnore(Marker position, BigInteger token)
        {
            return false;
        }

        public int size()
        {
            return markers.size();
        }

        public boolean isEmpty()
        {
            return markers.isEmpty();
        }

        public Collection<Marker> values()
        {
            return markers.values();
        }

        public PerInstanceBuilder mutate()
        {
            return new PerInstanceBuilder(markers);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(markers);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final PerInstance rhs = (PerInstance) obj;
            return markers.equals(rhs.markers);
        }

        @Override
        public String toString()
        {
            return "PerInstance{" +
                   "markers=" + markers +
                   '}';
        }
    }

    class PerInstanceBuilder
    {
        private final Map<CassandraInstance, Marker> markers;

        public PerInstanceBuilder()
        {
            this(ImmutableMap.of());
        }

        public PerInstanceBuilder(Map<CassandraInstance, Marker> markers)
        {
            this.markers = new HashMap<>(markers);
        }

        /**
         * Advance the commit log marker for a CassandraInstance only if the marker is greater than the current value.
         *
         * @param instance CassandraInstance
         * @param marker   CommitLog marker
         * @return this builder
         */
        public PerInstanceBuilder advanceMarker(CassandraInstance instance, Marker marker)
        {
            markers.compute(instance, (key, previous) -> {
                if (previous == null || marker.compareTo(previous) > 0)
                {
                    return marker;
                }
                return previous;
            });

            return this;
        }

        public PerInstance build()
        {
            return new PerInstance(markers);
        }
    }

    class Serializer extends com.esotericsoftware.kryo.Serializer<CommitLogMarkers>
    {
        @Override
        public CommitLogMarkers read(Kryo kryo, Input in, Class type)
        {
            int size = in.readShort();
            Map<CassandraInstance, Marker> markers = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                Marker marker = kryo.readObject(in, Marker.class, Marker.SERIALIZER);
                markers.put(marker.instance(), marker);
            }
            return new PerInstance(markers);
        }

        @Override
        public void write(Kryo kryo, Output out, CommitLogMarkers markers)
        {
            Collection<Marker> allMarkers = markers.values();
            out.writeShort(allMarkers.size());
            for (Marker marker : allMarkers)
            {
                kryo.writeObject(out, marker, Marker.SERIALIZER);
            }
        }
    }

    /**
     * Stores CommitLog markers per CassandraInstance, storing per token range
     */
    class PerRange implements CommitLogMarkers
    {
        private final Map<CassandraInstance, Map<TokenRange, Marker>> markers;

        public PerRange(Map<CassandraInstance, Map<TokenRange, Marker>> markers)
        {
            this.markers = ImmutableMap.copyOf(markers);
        }

        @NotNull
        public Marker startMarker(CassandraInstance instance)
        {
            return markers.getOrDefault(instance, ImmutableMap.of())
                          .values()
                          .stream()
                          .min(Marker::compareTo)
                          .orElseGet(instance::zeroMarker);
        }

        public boolean canIgnore(Marker position, BigInteger token)
        {
            Map<TokenRange, Marker> instMarkers = markers.get(position.instance());
            if (instMarkers == null || instMarkers.isEmpty())
            {
                return false;
            }

            // if position is before any previously consumed range (that overlaps with token) then we can ignore as already published
            return instMarkers.entrySet()
                              .stream()
                              .filter(entry -> entry.getKey().contains(token))
                              .map(Map.Entry::getValue)
                              .anyMatch(position::isBefore);
        }

        public int size()
        {
            return markers.size();
        }

        public boolean isEmpty()
        {
            return markers.isEmpty();
        }

        public Collection<Marker> values()
        {
            return markers.keySet().stream()
                          .map(this::startMarker)
                          .collect(Collectors.toList());
        }

        public PerInstanceBuilder mutate()
        {
            return new PerInstanceBuilder(
            markers.entrySet().stream().collect(
            Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().values().stream().min(Marker::compareTo).orElse(e.getKey().zeroMarker())
            )));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(markers);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            PerRange rhs = (PerRange) obj;
            return markers.equals(rhs.markers);
        }

        public String toString()
        {
            return "PerRange{" +
                   "markers=" + markers +
                   '}';
        }
    }
}
