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
import java.util.Objects;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.model.TokenOwner;

@SuppressWarnings("WeakerAccess")
public class CassandraInstance implements TokenOwner, Serializable
{
    private static final long serialVersionUID = 6767636627576239773L;
    private final String token;
    private final String node;
    private final String dataCenter;

    public CassandraInstance(String token, String node, String dataCenter)
    {
        this.token = token;
        this.node = node;
        this.dataCenter = dataCenter;
    }

    public String token()
    {
        return token;
    }

    public String nodeName()
    {
        return node;
    }

    public String dataCenter()
    {
        return dataCenter;
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
        return new EqualsBuilder()
               .append(this.token, that.token)
               .append(this.node, that.node)
               .append(this.dataCenter, that.dataCenter)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, node, dataCenter);
    }

    @Override
    public String toString()
    {
        return String.format("{\"token\"=\"%s\", \"node\"=\"%s\", \"dc\"=\"%s\"}", token, node, dataCenter);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraInstance>
    {
        @Override
        public CassandraInstance read(Kryo kryo, Input in, Class type)
        {
            return new CassandraInstance(in.readString(), in.readString(), in.readString());
        }

        @Override
        public void write(Kryo kryo, Output out, CassandraInstance instance)
        {
            out.writeString(instance.token());
            out.writeString(instance.nodeName());
            out.writeString(instance.dataCenter());
        }
    }
}
