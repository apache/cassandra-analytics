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
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;

public class Marker implements Comparable<Marker>, Serializable
{
    public static final Serializer SERIALIZER = new Serializer();

    public static Marker origin(CassandraInstance instance)
    {
        return new Marker(instance, 0, 0);
    }

    public final CassandraInstance instance;
    public final long segmentId;
    public final int position;

    @JsonCreator
    public Marker(@JsonProperty("instance") final CassandraInstance instance,
                  @JsonProperty("segmentId") final long segmentId,
                  @JsonProperty("position") final int position)
    {
        this.instance = instance;
        this.segmentId = segmentId;
        this.position = position;
    }

    /**
     * Marks the start position of the section.
     *
     * @return position in CommitLog of the section.
     */
    @JsonGetter("segmentId")
    public long segmentId()
    {
        return segmentId;
    }

    @JsonGetter("instance")
    public CassandraInstance instance()
    {
        return instance;
    }

    /**
     * The offset into the section where the mutation starts.
     *
     * @return mutation offset within the section
     */
    @JsonGetter("position")
    public int position()
    {
        return position;
    }

    public static Marker min(@NotNull final Marker m1, @NotNull final Marker m2)
    {
        return m1.compareTo(m2) < 0 ? m1 : m2;
    }

    public boolean isBefore(@NotNull Marker o)
    {
        return this.compareTo(o) < 0;
    }

    @Override
    public int compareTo(@NotNull Marker o)
    {
        if (!instance.equals(o.instance))
        {
            throw new IllegalArgumentException("CommitLog Markers should be on the same instance");
        }
        final int c = Long.compare(segmentId, o.segmentId());
        if (c == 0)
        {
            return Integer.compare(position, o.position());
        }
        return c;
    }

    public String toString()
    {
        return String.format('{' +
                             "\"segmentId\": %d, " +
                             "\"position\": %d" +
                             " }", segmentId, position);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(instance, segmentId, position);
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (!(other instanceof Marker))
        {
            return false;
        }

        final Marker that = (Marker) other;
        return Objects.equals(instance, that.instance)
               && segmentId == that.segmentId
               && position == that.position;
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<Marker>
    {
        public void write(Kryo kryo, Output out, Marker o)
        {
            kryo.writeObject(out, o.instance, CassandraInstance.SERIALIZER);
            out.writeLong(o.segmentId);
            out.writeInt(o.position);
        }

        public Marker read(Kryo kryo, Input in, Class<Marker> type)
        {
            return new Marker(kryo.readObject(in, CassandraInstance.class, CassandraInstance.SERIALIZER), in.readLong(), in.readInt());
        }
    }
}
