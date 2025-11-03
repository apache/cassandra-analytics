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

package org.apache.cassandra.spark.data.partitioner;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.spark.data.model.TokenOwner;

@SuppressWarnings("WeakerAccess")
public class CassandraInstance implements TokenOwner, Serializable
{
    public static final CassandraInstance.Serializer SERIALIZER = new CassandraInstance.Serializer();

    private static final long serialVersionUID = 6767636627576239773L;
    private final Set<String> tokens;
    private final String node;
    private final String dataCenter;

    public CassandraInstance(Set<String> tokens, String node, String dataCenter)
    {
        this.tokens = tokens;
        this.node = node;
        this.dataCenter = dataCenter;
    }

    public Set<String> tokens()
    {
        return tokens;
    }

    public String nodeName()
    {
        return node;
    }

    public String dataCenter()
    {
        return dataCenter;
    }

    public Marker zeroMarker()
    {
        return markerAt(0, 0);
    }

    public Marker markerAt(long section, int position)
    {
        return new Marker(this, section, position);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        CassandraInstance that = (CassandraInstance) other;
        return Objects.equals(this.tokens, that.tokens)
               && Objects.equals(this.node, that.node)
               && Objects.equals(this.dataCenter, that.dataCenter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tokens, node, dataCenter);
    }

    @Override
    public String toString()
    {
        return String.format("{\"tokens\"=\"%s\", \"node\"=\"%s\", \"dc\"=\"%s\"}", tokens, node, dataCenter);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraInstance>
    {
        @Override
        public CassandraInstance read(Kryo kryo, Input in, Class type)
        {
            Set<String> tokens = new HashSet<>();
            int numTokens = in.readInt();
            for (int i = 0; i < numTokens; i++)
            {
                tokens.add(in.readString());
            }
            return new CassandraInstance(tokens, in.readString(), in.readString());
        }

        @Override
        public void write(Kryo kryo, Output out, CassandraInstance instance)
        {
            out.writeInt(instance.tokens().size());
            for (String token : instance.tokens())
            {
                out.writeString(token);
            }
            out.writeString(instance.nodeName());
            out.writeString(instance.dataCenter());
        }
    }
}
