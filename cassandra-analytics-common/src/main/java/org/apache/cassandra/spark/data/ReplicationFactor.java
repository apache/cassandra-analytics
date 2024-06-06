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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jetbrains.annotations.NotNull;

/**
 * Replication factor object, expected format:
 *     {
 *         "class" : "NetworkTopologyStrategy",
 *         "options" : {
 *             "DC1" : 2,
 *             "DC2" : 2
 *         }
 *     }
 *     {
 *         "class" : "SimpleStrategy",
 *         "options" : {
 *             "replication_factor" : 1
 *         }
 *     }
 */
public class ReplicationFactor implements Serializable
{
    private static final long serialVersionUID = -2017022813595983257L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationFactor.class);

    public enum ReplicationStrategy
    {
        LocalStrategy(0),
        SimpleStrategy(1),
        NetworkTopologyStrategy(2);

        public final int value;

        ReplicationStrategy(int value)
        {
            this.value = value;
        }

        public static ReplicationStrategy valueOf(int value)
        {
            switch (value)
            {
                case 0:
                    return LocalStrategy;
                case 1:
                    return SimpleStrategy;
                case 2:
                    return NetworkTopologyStrategy;
                default:
                    throw new IllegalStateException("Unknown ReplicationStrategy: " + value);
            }
        }

        public static ReplicationStrategy getEnum(String value)
        {
            for (ReplicationStrategy v : values())
            {
                if (value.equalsIgnoreCase(v.name()) || value.endsWith("." + v.name()))
                {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }
    }

    @NotNull
    private final ReplicationStrategy replicationStrategy;
    @NotNull
    private final Map<String, Integer> options;

    public ReplicationFactor(@NotNull Map<String, String> options)
    {
        this.replicationStrategy = ReplicationFactor.ReplicationStrategy.getEnum(options.get("class"));
        this.options = new LinkedHashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            if ("class".equals(entry.getKey()))
            {
                continue;
            }

            try
            {
                this.options.put(entry.getKey(), Integer.parseInt(entry.getValue()));
            }
            catch (NumberFormatException exception)
            {
                LOGGER.warn("Could not parse replication option: {} = {}", entry.getKey(), entry.getValue());
            }
        }
    }

    public ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy, @NotNull Map<String, Integer> options)
    {
        this.replicationStrategy = replicationStrategy;
        this.options = new LinkedHashMap<>(options.size());

        if (!replicationStrategy.equals(ReplicationStrategy.LocalStrategy) && options.isEmpty())
        {
            throw new RuntimeException(String.format("Could not find replication info in schema map: %s.", options));
        }

        for (Map.Entry<String, Integer> entry : options.entrySet())
        {
            if ("class".equals(entry.getKey()))
            {
                continue;
            }
            this.options.put(entry.getKey(), entry.getValue());
        }
    }

    public Integer getTotalReplicationFactor()
    {
        return options.values().stream()
                      .mapToInt(Integer::intValue)
                      .sum();
    }

    @NotNull
    public Map<String, Integer> getOptions()
    {
        return options;
    }

    @NotNull
    public ReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        ReplicationFactor that = (ReplicationFactor) other;
        return new EqualsBuilder()
               .append(this.replicationStrategy, that.replicationStrategy)
               .append(this.options, that.options)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(replicationStrategy)
               .append(options)
               .toHashCode();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<ReplicationFactor>
    {
        @Override
        public void write(Kryo kryo, Output out, ReplicationFactor replicationFactor)
        {
            out.writeByte(replicationFactor.replicationStrategy.value);
            out.writeByte(replicationFactor.options.size());
            for (Map.Entry<String, Integer> entry : replicationFactor.options.entrySet())
            {
                out.writeString(entry.getKey());
                out.writeByte(entry.getValue());
            }
        }

        @Override
        public ReplicationFactor read(Kryo kryo, Input in, Class<ReplicationFactor> type)
        {
            ReplicationStrategy strategy = ReplicationStrategy.valueOf(in.readByte());
            int numOptions = in.readByte();
            Map<String, Integer> options = new HashMap<>(numOptions);
            for (int option = 0; option < numOptions; option++)
            {
                options.put(in.readString(), (int) in.readByte());
            }
            return new ReplicationFactor(strategy, options);
        }
    }
}
