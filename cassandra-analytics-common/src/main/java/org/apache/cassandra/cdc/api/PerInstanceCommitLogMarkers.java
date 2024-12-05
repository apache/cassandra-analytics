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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;

/**
 * Stores CommitLog markers per CassandraInstance, taking the minimum marker when there are duplicates.
 */
public class PerInstanceCommitLogMarkers implements CommitLogMarkers
{
    final Map<CassandraInstance, Marker> markers;

    public PerInstanceCommitLogMarkers(Collection<Marker> markers1, Collection<Marker> markers2)
    {
        this(
        Stream.concat(markers1.stream(), markers2.stream())
              .collect(Collectors.toMap(Marker::instance, Function.identity(), Marker::min))
        );
    }

    public PerInstanceCommitLogMarkers(Map<CassandraInstance, Marker> markers)
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

        final PerInstanceCommitLogMarkers rhs = (PerInstanceCommitLogMarkers) obj;
        return markers.equals(rhs.markers);
    }

    @Override
    public String toString()
    {
        return "PerInstance{" +
               "markers=" + markers +
               '}';
    }

    public static class PerInstanceBuilder
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

        public PerInstanceCommitLogMarkers build()
        {
            return new PerInstanceCommitLogMarkers(markers);
        }
    }
}
