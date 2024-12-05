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

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;

/**
 * Stores CommitLog markers per CassandraInstance, storing per token range
 */
public class PerRangeCommitLogMarkers implements CommitLogMarkers
{
    private final Map<CassandraInstance, Map<TokenRange, Marker>> markers;

    public PerRangeCommitLogMarkers(Map<CassandraInstance, Map<TokenRange, Marker>> markers)
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

    public PerInstanceCommitLogMarkers.PerInstanceBuilder mutate()
    {
        return new PerInstanceCommitLogMarkers.PerInstanceBuilder(
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

        PerRangeCommitLogMarkers rhs = (PerRangeCommitLogMarkers) obj;
        return markers.equals(rhs.markers);
    }

    public String toString()
    {
        return "PerRange{" +
               "markers=" + markers +
               '}';
    }

    public static class PerRangeBuilder
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

        public PerRangeCommitLogMarkers build()
        {
            return new PerRangeCommitLogMarkers(this.markers);
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
}
