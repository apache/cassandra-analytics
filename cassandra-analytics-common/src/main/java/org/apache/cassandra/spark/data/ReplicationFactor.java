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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.Objects;

import com.google.common.collect.ImmutableMap;
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
 * <p>
 * Replica counts may also use the {@code <replicas>/<transient>} form, e.g. {@code "DC1" : "3/1"}, meaning three
 * replicas of which one is transient. Witness replicas under mutation tracking (CEP-45/CEP-46) reuse this form, so
 * {@code "3/1"} describes two full replicas and one witness. {@link #getTotalReplicationFactor()} continues to
 * report all three; use {@link #getFullReplicationFactor()} for the count that holds the full data set.
 */
public class ReplicationFactor implements Serializable
{
    public static final Serializer SERIALIZER = new Serializer();
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

    public static ReplicationFactor simpleStrategy(int rf)
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                     ImmutableMap.of("replication_factor", rf));
    }

    @NotNull
    private final ReplicationStrategy replicationStrategy;
    @NotNull
    private final Map<String, Integer> options;
    /**
     * Per-datacenter count of transient (witness) replicas, parsed from the {@code <replicas>/<transient>} form.
     * A datacenter absent from this map has no transient replicas. Always empty for untracked keyspaces using the
     * plain {@code <replicas>} form, which keeps behaviour identical for those keyspaces.
     */
    @NotNull
    private final Map<String, Integer> transientOptions;

    /**
     * Lenient parse: a replication value that cannot be parsed is logged and its datacenter omitted. Retained for
     * callers that tolerate a partial replication factor.
     *
     * @param options the raw replication map, including the {@code class} entry
     */
    public ReplicationFactor(@NotNull Map<String, String> options)
    {
        this(options, false);
    }

    /**
     * Strict parse: a replication value that cannot be parsed raises {@link IllegalArgumentException} naming the
     * offending datacenter, rather than silently omitting it. Prefer this when a partial replication factor would
     * produce a misleading failure later.
     *
     * @param options the raw replication map, including the {@code class} entry
     * @return the parsed replication factor
     * @throws IllegalArgumentException when any replication value cannot be parsed
     */
    public static ReplicationFactor parseStrict(@NotNull Map<String, String> options)
    {
        return new ReplicationFactor(options, true);
    }

    private ReplicationFactor(@NotNull Map<String, String> options, boolean strict)
    {
        this.replicationStrategy = ReplicationFactor.ReplicationStrategy.getEnum(options.get("class"));
        this.options = new LinkedHashMap<>(options.size());
        this.transientOptions = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            if ("class".equals(entry.getKey()))
            {
                continue;
            }

            try
            {
                ReplicaCounts counts = ReplicaCounts.parse(entry.getValue());
                this.options.put(entry.getKey(), counts.allReplicas);
                if (counts.transientReplicas > 0)
                {
                    this.transientOptions.put(entry.getKey(), counts.transientReplicas);
                }
            }
            catch (IllegalArgumentException exception)
            {
                if (strict)
                {
                    throw new IllegalArgumentException(String.format("Could not parse replication option: %s = %s",
                                                                     entry.getKey(), entry.getValue()), exception);
                }
                LOGGER.warn("Could not parse replication option: {} = {}", entry.getKey(), entry.getValue());
            }
        }

        // Mirrors the guard on the (strategy, options) constructor. A strategy other than LocalStrategy with no
        // datacenter entries is not usable, and reporting it here keeps the failure at parse time rather than
        // surfacing later as a misleading "DC not found in replication factor". Strict-only, so the lenient
        // constructor's behaviour is unchanged for callers that tolerate a partial replication factor.
        if (strict && replicationStrategy != ReplicationStrategy.LocalStrategy && this.options.isEmpty())
        {
            throw new IllegalArgumentException("Could not find replication info in schema map: " + options);
        }
    }

    public ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy, @NotNull Map<String, Integer> options)
    {
        this(replicationStrategy, options, Collections.emptyMap());
    }

    public ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy,
                             @NotNull Map<String, Integer> options,
                             @NotNull Map<String, Integer> transientOptions)
    {
        this.replicationStrategy = replicationStrategy;
        this.options = new LinkedHashMap<>(options.size());
        this.transientOptions = new LinkedHashMap<>(transientOptions.size());

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

        for (Map.Entry<String, Integer> entry : transientOptions.entrySet())
        {
            if (entry.getValue() == null || entry.getValue() == 0)
            {
                continue;
            }
            Integer allReplicas = this.options.get(entry.getKey());
            if (allReplicas == null)
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas specified for %s but it has no replication factor", entry.getKey()));
            }
            ReplicaCounts.validate(entry.getKey(), allReplicas, entry.getValue());
            this.transientOptions.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * @return the total number of replicas across all datacenters, including transient (witness) replicas.
     *         Semantics are unchanged from before transient replica support was added.
     */
    public Integer getTotalReplicationFactor()
    {
        return options.values().stream()
                      .mapToInt(Integer::intValue)
                      .sum();
    }

    /**
     * @return the number of replicas across all datacenters that hold the full data set, i.e. the total
     *         replication factor minus transient (witness) replicas
     */
    public Integer getFullReplicationFactor()
    {
        return getTotalReplicationFactor() - getTransientReplicationFactor();
    }

    /**
     * @return the number of transient (witness) replicas across all datacenters, {@code 0} when none are configured
     */
    public Integer getTransientReplicationFactor()
    {
        return transientOptions.values().stream()
                               .mapToInt(Integer::intValue)
                               .sum();
    }

    /**
     * @return {@code true} if any datacenter is configured with transient (witness) replicas
     */
    public boolean hasTransientReplicas()
    {
        return !transientOptions.isEmpty();
    }

    /**
     * @param datacenter the datacenter to look up
     * @return the number of transient (witness) replicas in {@code datacenter}, {@code 0} when none are configured
     */
    public int getTransientReplicas(@NotNull String datacenter)
    {
        return transientOptions.getOrDefault(datacenter, 0);
    }

    /**
     * @param datacenter the datacenter to look up
     * @return the number of replicas in {@code datacenter} holding the full data set
     * @throws IllegalArgumentException when {@code datacenter} has no replication factor
     */
    public int getFullReplicas(@NotNull String datacenter)
    {
        Integer allReplicas = options.get(datacenter);
        if (allReplicas == null)
        {
            throw new IllegalArgumentException(String.format("Datacenter %s not found in replication factor %s",
                                                             datacenter, options.keySet()));
        }
        return allReplicas - getTransientReplicas(datacenter);
    }

    /**
     * @return per-datacenter total replica counts, including transient (witness) replicas
     */
    @NotNull
    public Map<String, Integer> getOptions()
    {
        return options;
    }

    /**
     * @return per-datacenter transient (witness) replica counts. Datacenters without transient replicas are absent.
     */
    @NotNull
    public Map<String, Integer> getTransientOptions()
    {
        return transientOptions;
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
        return this.replicationStrategy == that.replicationStrategy
               && java.util.Objects.equals(this.options, that.options)
               && java.util.Objects.equals(this.transientOptions, that.transientOptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(replicationStrategy, options, transientOptions);
    }

    /**
     * {@link #serialVersionUID} is pinned, so an instance serialized before transient replica support was added
     * deserializes with a {@code null} {@code transientOptions}. Normalise it to an empty map so the accessors do not
     * NPE. Only reachable under driver/executor version skew, which is not a supported configuration.
     *
     * @return this instance, or a normalised copy when deserialized from the older form
     */
    private Object readResolve()
    {
        if (transientOptions != null)
        {
            return this;
        }
        return new ReplicationFactor(replicationStrategy, options, Collections.emptyMap());
    }

    /**
     * Parsed form of a single datacenter's replication value. Cassandra accepts either {@code <replicas>} or
     * {@code <replicas>/<transient>}, the latter reused by witness replicas under mutation tracking. See
     * {@code org.apache.cassandra.locator.ReplicationFactor} in Cassandra.
     */
    private static final class ReplicaCounts
    {
        private static final String TRANSIENT_SEPARATOR = "/";

        private final int allReplicas;
        private final int transientReplicas;

        private ReplicaCounts(int allReplicas, int transientReplicas)
        {
            this.allReplicas = allReplicas;
            this.transientReplicas = transientReplicas;
        }

        /**
         * @param value the raw replication value, e.g. {@code "3"} or {@code "3/1"}
         * @return the parsed replica counts
         * @throws NumberFormatException    when either component is not an integer
         * @throws IllegalArgumentException when the value is malformed or the counts are inconsistent
         */
        static ReplicaCounts parse(@NotNull String value)
        {
            String trimmed = value.trim();
            int separator = trimmed.indexOf(TRANSIENT_SEPARATOR);
            if (separator < 0)
            {
                int allReplicas = Integer.parseInt(trimmed);
                validate(null, allReplicas, 0);
                return new ReplicaCounts(allReplicas, 0);
            }

            if (trimmed.indexOf(TRANSIENT_SEPARATOR, separator + 1) >= 0)
            {
                throw new IllegalArgumentException(String.format(
                "Replication factor format is <replicas> or <replicas>/<transient>, found '%s'", value));
            }

            int allReplicas = Integer.parseInt(trimmed.substring(0, separator).trim());
            int transientReplicas = Integer.parseInt(trimmed.substring(separator + 1).trim());
            validate(null, allReplicas, transientReplicas);
            return new ReplicaCounts(allReplicas, transientReplicas);
        }

        /**
         * Mirrors the constraints Cassandra enforces in {@code ReplicationFactor.validate}: transient replicas must
         * be non-negative and strictly fewer than the total, so at least one full replica always exists.
         *
         * @param datacenter        datacenter name for the error message, may be {@code null}
         * @param allReplicas       total replicas
         * @param transientReplicas transient (witness) replicas
         */
        static void validate(String datacenter, int allReplicas, int transientReplicas)
        {
            String where = datacenter == null ? "" : String.format(" for datacenter %s", datacenter);
            if (allReplicas < 0)
            {
                throw new IllegalArgumentException(String.format(
                "Replication factor must be non-negative, found %d%s", allReplicas, where));
            }
            if (transientReplicas < 0)
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas must be non-negative, found %d%s", transientReplicas, where));
            }
            if (transientReplicas > 0 && transientReplicas >= allReplicas)
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas must be zero, or less than the total replication factor. For %d/%d%s",
                allReplicas, transientReplicas, where));
            }
        }
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
            // Transient (witness) replica counts, written after the totals so the common
            // no-transient-replicas case costs a single zero byte
            out.writeByte(replicationFactor.transientOptions.size());
            for (Map.Entry<String, Integer> entry : replicationFactor.transientOptions.entrySet())
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
            int numTransientOptions = in.readByte();
            Map<String, Integer> transientOptions = new HashMap<>(numTransientOptions);
            for (int option = 0; option < numTransientOptions; option++)
            {
                transientOptions.put(in.readString(), (int) in.readByte());
            }
            return new ReplicationFactor(strategy, options, transientOptions);
        }
    }
}
