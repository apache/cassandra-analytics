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

import com.google.common.collect.ImmutableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CommitLogMarkers extends Serializable
{
    PerInstanceCommitLogMarkers EMPTY = new PerInstanceCommitLogMarkers(ImmutableMap.of());
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

    static PerInstanceCommitLogMarkers of(@Nullable final Marker marker)
    {
        if (marker == null)
        {
            return EMPTY;
        }
        return of(ImmutableMap.of(marker.instance(), marker));
    }

    static PerInstanceCommitLogMarkers of(Map<CassandraInstance, Marker> markers)
    {
        return new PerInstanceCommitLogMarkers(markers);
    }

    static PerInstanceCommitLogMarkers of(CommitLogMarkers markers1,
                                          CommitLogMarkers markers2)
    {
        return new PerInstanceCommitLogMarkers(markers1.values(), markers2.values());
    }

    boolean isEmpty();

    Collection<Marker> values();

    PerInstanceCommitLogMarkers.PerInstanceBuilder mutate();

    static PerInstanceCommitLogMarkers.PerInstanceBuilder perInstanceBuilder()
    {
        return new PerInstanceCommitLogMarkers.PerInstanceBuilder();
    }

    static PerRangeCommitLogMarkers.PerRangeBuilder perRangeBuilder()
    {
        return new PerRangeCommitLogMarkers.PerRangeBuilder();
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
            return new PerInstanceCommitLogMarkers(markers);
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
}
