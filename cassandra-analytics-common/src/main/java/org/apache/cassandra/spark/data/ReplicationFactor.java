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

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.Objects;

import com.google.common.collect.ImmutableMap;

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
 * replicas of which one is transient. Witness replicas (CEP-46) reuse this form, so {@code "3/1"} describes two full
 * replicas and one witness. {@link #getTotalReplicationFactor()} continues to report all three; use
 * {@link #getFullReplicationFactor()} for the count that holds the full data set.
 */
public class ReplicationFactor implements Serializable
{
    public static final Serializer SERIALIZER = new Serializer();
    private static final long serialVersionUID = -2017022813595983257L;

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
    /**
     * Per-datacenter replica counts. Holding the total and the transient count together, rather than in two
     * parallel maps, means the two cannot drift apart.
     */
    @NotNull
    private final Map<String, ReplicaCounts> replication;

    // Derived from replication, computed once because this class is immutable. Marked transient so they are absent
    // from the serialized form: readResolve rebuilds through the canonical constructor, and the Kryo serializer
    // already goes through it, so neither path can leave these out of step with replication.
    private final transient int totalReplicationFactor;
    private final transient int fullReplicationFactor;
    private final transient int transientReplicationFactor;
    private final transient Map<String, Integer> options;
    private final transient Map<String, Integer> transientOptions;

    /**
     * Parses a raw replication map. A value that cannot be parsed, or that Cassandra itself would reject, raises
     * {@link IllegalArgumentException} naming the offending datacenter: a partial replication factor is not usable
     * and reporting it here avoids a misleading failure later, such as "DC not found in replication factor".
     *
     * @param options the raw replication map, including the {@code class} entry
     * @throws IllegalArgumentException when any replication value cannot be parsed
     */
    public ReplicationFactor(@NotNull Map<String, String> options)
    {
        this(ReplicationStrategy.getEnum(options.get("class")), parse(options), options);
    }

    public ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy, @NotNull Map<String, Integer> options)
    {
        this(replicationStrategy, options, Collections.emptyMap());
    }

    public ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy,
                             @NotNull Map<String, Integer> options,
                             @NotNull Map<String, Integer> transientOptions)
    {
        this(replicationStrategy, merge(options, transientOptions), options);
    }

    /**
     * Canonical constructor. Every other constructor resolves its input to per-datacenter counts and delegates here.
     *
     * @param replicationStrategy the replication strategy
     * @param replication         per-datacenter replica counts, already validated individually
     * @param raw                 the caller's original input, used only in the error message
     */
    private ReplicationFactor(@NotNull ReplicationStrategy replicationStrategy,
                              @NotNull Map<String, ReplicaCounts> replication,
                              @NotNull Object raw)
    {
        // A strategy other than LocalStrategy with no datacenter entries is not usable
        if (replicationStrategy != ReplicationStrategy.LocalStrategy && replication.isEmpty())
        {
            throw new IllegalArgumentException("Could not find replication info in schema map: " + raw);
        }
        this.replicationStrategy = replicationStrategy;
        this.replication = Collections.unmodifiableMap(new LinkedHashMap<>(replication));

        int total = 0;
        int transientCount = 0;
        Map<String, Integer> allByDatacenter = new LinkedHashMap<>(this.replication.size());
        Map<String, Integer> transientByDatacenter = new LinkedHashMap<>();
        for (Map.Entry<String, ReplicaCounts> entry : this.replication.entrySet())
        {
            ReplicaCounts counts = entry.getValue();
            total += counts.allReplicas();
            transientCount += counts.transientReplicas();
            allByDatacenter.put(entry.getKey(), counts.allReplicas());
            if (counts.transientReplicas() > 0)
            {
                transientByDatacenter.put(entry.getKey(), counts.transientReplicas());
            }
        }
        this.totalReplicationFactor = total;
        this.transientReplicationFactor = transientCount;
        this.fullReplicationFactor = total - transientCount;
        this.options = Collections.unmodifiableMap(allByDatacenter);
        this.transientOptions = Collections.unmodifiableMap(transientByDatacenter);
    }

    /**
     * Resolves raw string values, e.g. {@code "3"} or {@code "3/1"}, to per-datacenter counts.
     */
    private static Map<String, ReplicaCounts> parse(@NotNull Map<String, String> options)
    {
        Map<String, ReplicaCounts> parsed = new LinkedHashMap<>(options.size());
        options.forEach((datacenter, value) -> {
            if ("class".equals(datacenter))
            {
                return;
            }
            try
            {
                parsed.put(datacenter, ReplicaCounts.parse(value));
            }
            catch (IllegalArgumentException exception)
            {
                throw new IllegalArgumentException(String.format("Could not parse replication option: %s = %s",
                                                                 datacenter, value), exception);
            }
        });
        return parsed;
    }

    /**
     * Builds counts for one datacenter, adding the datacenter name to any validation failure.
     * <p>
     * {@link ReplicaCounts} is a plain value type and deliberately knows nothing about datacenters, so naming the
     * datacenter is the caller's job - see the discussion on CASSANALYTICS-194.
     *
     * @param datacenter        datacenter the counts belong to, used only to describe a failure
     * @param allReplicas       total replicas
     * @param transientReplicas transient (witness) replicas
     * @return validated counts
     * @throws IllegalArgumentException when the counts are inconsistent, naming the datacenter
     */
    private static ReplicaCounts countsFor(String datacenter, int allReplicas, int transientReplicas)
    {
        try
        {
            return ReplicaCounts.of(allReplicas, transientReplicas);
        }
        catch (IllegalArgumentException exception)
        {
            throw new IllegalArgumentException(
            String.format("%s for datacenter %s", exception.getMessage(), datacenter), exception);
        }
    }

    /**
     * Resolves separate total and transient maps to per-datacenter counts, rejecting a transient entry for a
     * datacenter that has no replication factor.
     */
    private static Map<String, ReplicaCounts> merge(@NotNull Map<String, Integer> options,
                                                    @NotNull Map<String, Integer> transientOptions)
    {
        Map<String, ReplicaCounts> merged = new LinkedHashMap<>(options.size());
        options.forEach((datacenter, allReplicas) -> {
            if (!"class".equals(datacenter))
            {
                merged.put(datacenter, countsFor(datacenter, allReplicas,
                                                transientOptions.getOrDefault(datacenter, 0)));
            }
        });
        transientOptions.forEach((datacenter, transientReplicas) -> {
            if (!"class".equals(datacenter) && transientReplicas != null && transientReplicas != 0
                && !merged.containsKey(datacenter))
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas specified for %s but it has no replication factor", datacenter));
            }
        });
        return merged;
    }

    /**
     * @return the total number of replicas across all datacenters, including transient (witness) replicas.
     *         Semantics are unchanged from before transient replica support was added.
     */
    public Integer getTotalReplicationFactor()
    {
        return totalReplicationFactor;
    }

    /**
     * @return the number of replicas across all datacenters that hold the full data set, i.e. the total
     *         replication factor minus transient (witness) replicas
     */
    public Integer getFullReplicationFactor()
    {
        return fullReplicationFactor;
    }

    /**
     * @return the number of transient (witness) replicas across all datacenters, {@code 0} when none are configured
     */
    public Integer getTransientReplicationFactor()
    {
        return transientReplicationFactor;
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
     * @return the number of transient (witness) replicas in {@code datacenter}, {@code 0} when it has none
     * @throws IllegalArgumentException when {@code datacenter} has no replication factor
     */
    public int getTransientReplicas(@NotNull String datacenter)
    {
        return counts(datacenter).transientReplicas();
    }

    /**
     * @param datacenter the datacenter to look up
     * @return the number of replicas in {@code datacenter} holding the full data set
     * @throws IllegalArgumentException when {@code datacenter} has no replication factor
     */
    public int getFullReplicas(@NotNull String datacenter)
    {
        return counts(datacenter).fullReplicas();
    }

    private ReplicaCounts counts(@NotNull String datacenter)
    {
        ReplicaCounts counts = replication.get(datacenter);
        if (counts == null)
        {
            throw new IllegalArgumentException(String.format("Datacenter %s not found in replication factor %s",
                                                             datacenter, replication.keySet()));
        }
        return counts;
    }

    /**
     * @return per-datacenter replica counts, unmodifiable
     */
    @NotNull
    public Map<String, ReplicaCounts> getReplication()
    {
        return replication;
    }

    /**
     * @return per-datacenter total replica counts, including transient (witness) replicas. Unmodifiable, and derived
     *         from {@link #getReplication()}.
     */
    @NotNull
    public Map<String, Integer> getOptions()
    {
        return options;
    }

    /**
     * @return per-datacenter transient (witness) replica counts, unmodifiable. Datacenters without transient
     *         replicas are absent.
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
               && Objects.equals(this.replication, that.replication);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(replicationStrategy, replication);
    }

    /**
     * {@link #serialVersionUID} is pinned, so an instance serialized before the per-datacenter counts were combined
     * deserializes with a {@code null} {@code replication}. Only reachable under driver/executor version skew, which
     * is not a supported configuration, so this reports the mismatch rather than silently losing the counts.
     *
     * @return this instance
     * @throws InvalidObjectException when deserialized from an incompatible older form
     */
    private Object readResolve() throws InvalidObjectException
    {
        if (replication == null)
        {
            throw new InvalidObjectException(
            "ReplicationFactor was serialized by an incompatible version: per-datacenter replica counts are absent. "
            + "Driver and executors must run the same version.");
        }
        // Rebuild through the canonical constructor so the derived fields, which are transient, are populated
        return new ReplicationFactor(replicationStrategy, replication, replication);
    }

    /**
     * Replica counts for a single datacenter. Cassandra accepts either {@code <replicas>} or
     * {@code <replicas>/<transient>}; transient replication predates mutation tracking, and witness replicas
     * (CEP-46) reuse the same form. See {@code org.apache.cassandra.locator.ReplicationFactor} in Cassandra.
     */
    public static final class ReplicaCounts implements Serializable
    {
        private static final long serialVersionUID = 2026091500000000001L;
        private static final String TRANSIENT_SEPARATOR = "/";

        private final int allReplicas;
        private final int transientReplicas;

        private ReplicaCounts(int allReplicas, int transientReplicas)
        {
            this.allReplicas = allReplicas;
            this.transientReplicas = transientReplicas;
        }

        /**
         * @param allReplicas       total replicas
         * @param transientReplicas transient (witness) replicas
         * @return validated counts
         * @throws IllegalArgumentException when the counts are inconsistent. The message describes the counts
         *                                 only; callers that know which datacenter the counts came from are
         *                                 responsible for adding that context.
         */
        public static ReplicaCounts of(int allReplicas, int transientReplicas)
        {
            validate(allReplicas, transientReplicas);
            return new ReplicaCounts(allReplicas, transientReplicas);
        }

        /**
         * @return total replicas, including transient (witness) replicas
         */
        public int allReplicas()
        {
            return allReplicas;
        }

        /**
         * @return replicas holding the full data set
         */
        public int fullReplicas()
        {
            return allReplicas - transientReplicas;
        }

        /**
         * @return transient (witness) replicas, {@code 0} when none are configured
         */
        public int transientReplicas()
        {
            return transientReplicas;
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
                return of(null, Integer.parseInt(trimmed), 0);
            }

            if (trimmed.indexOf(TRANSIENT_SEPARATOR, separator + 1) >= 0)
            {
                throw new IllegalArgumentException(String.format(
                "Replication factor format is <replicas> or <replicas>/<transient>, found '%s'", value));
            }

            return of(null,
                      Integer.parseInt(trimmed.substring(0, separator).trim()),
                      Integer.parseInt(trimmed.substring(separator + 1).trim()));
        }

        /**
         * Mirrors the constraints Cassandra enforces in {@code ReplicationFactor.validate}: transient replicas must
         * be non-negative and strictly fewer than the total, so at least one full replica always exists.
         *
         * @param datacenter        datacenter name for the error message, may be {@code null}
         * @param allReplicas       total replicas
         * @param transientReplicas transient (witness) replicas
         */
        static void validate(int allReplicas, int transientReplicas)
        {
            if (allReplicas < 0)
            {
                throw new IllegalArgumentException(String.format(
                "Replication factor must be non-negative, found %d", allReplicas));
            }
            if (transientReplicas < 0)
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas must be non-negative, found %d", transientReplicas));
            }
            if (transientReplicas > 0 && transientReplicas >= allReplicas)
            {
                throw new IllegalArgumentException(String.format(
                "Transient replicas must be zero, or less than the total replication factor. For %d/%d",
                allReplicas, transientReplicas));
            }
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            ReplicaCounts that = (ReplicaCounts) other;
            return allReplicas == that.allReplicas && transientReplicas == that.transientReplicas;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(allReplicas, transientReplicas);
        }

        @Override
        public String toString()
        {
            return transientReplicas > 0 ? allReplicas + TRANSIENT_SEPARATOR + transientReplicas
                                         : String.valueOf(allReplicas);
        }
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<ReplicationFactor>
    {
        /**
         * Incremented whenever the Kryo format below changes. Version 1 added the per-datacenter transient
         * (witness) replica count, taking each datacenter from one byte to two.
         * <p>
         * Without the marker an older stream is not merely rejected but silently misread: the reader would take
         * the next datacenter's string-length byte as a transient count and desync for the rest of the stream,
         * yielding wrong replication factors rather than an error. Kryo is the path Spark uses to ship this to
         * executors, so it gets the same protection as the hand-rolled JDK format in
         * {@link org.apache.cassandra.spark.data.partitioner.CassandraRing}.
         */
        private static final byte SERIALIZATION_FORMAT_VERSION = 1;

        @Override
        public void write(Kryo kryo, Output out, ReplicationFactor replicationFactor)
        {
            out.writeByte(SERIALIZATION_FORMAT_VERSION);
            out.writeByte(replicationFactor.replicationStrategy.value);
            out.writeByte(replicationFactor.replication.size());
            for (Map.Entry<String, ReplicaCounts> entry : replicationFactor.replication.entrySet())
            {
                out.writeString(entry.getKey());
                out.writeByte(entry.getValue().allReplicas());
                out.writeByte(entry.getValue().transientReplicas());
            }
        }

        @Override
        public ReplicationFactor read(Kryo kryo, Input in, Class<ReplicationFactor> type)
        {
            byte formatVersion = in.readByte();
            if (formatVersion != SERIALIZATION_FORMAT_VERSION)
            {
                throw new IllegalStateException(String.format(
                "Unsupported ReplicationFactor Kryo serialization format version %d, expected %d",
                formatVersion, SERIALIZATION_FORMAT_VERSION));
            }
            ReplicationStrategy strategy = ReplicationStrategy.valueOf(in.readByte());
            int numDatacenters = in.readByte();
            Map<String, ReplicaCounts> replication = new LinkedHashMap<>(numDatacenters);
            for (int datacenter = 0; datacenter < numDatacenters; datacenter++)
            {
                String name = in.readString();
                int allReplicas = in.readByte();
                int transientReplicas = in.readByte();
                replication.put(name, countsFor(name, allReplicas, transientReplicas));
            }
            return new ReplicationFactor(strategy, replication, replication);
        }
    }
}
