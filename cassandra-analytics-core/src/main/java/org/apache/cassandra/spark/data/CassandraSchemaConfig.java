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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.utils.MapUtils;

/**
 * Configuration for Cassandra schema-related settings.
 * This class contains settings needed to build CQL schema and connect to a Cassandra table.
 * It is shared across batch and streaming configurations.
 */
public class CassandraSchemaConfig implements Serializable
{
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSchemaConfig.class);

    // Option keys
    public static final String CLUSTER_NAME_KEY = "clusterName";
    public static final String CLUSTER_ID_KEY = "clusterId";
    public static final String KEYSPACE_KEY = "keyspace";
    public static final String TABLE_KEY = "table";
    public static final String DATACENTER_KEY = "datacenter";
    public static final String DC_KEY = "dc";  // Alias for datacenter
    public static final String TABLE_CREATE_STMT_KEY = "tableCreateStmt";
    public static final String CASSANDRA_VERSION_KEY = "cassandraVersion";
    public static final String UDTS_KEY = "udts";
    public static final String REPLICATION_STRATEGY_KEY = "replicationStrategy";
    public static final String REPLICATION_FACTOR_KEY = "replicationFactor";

    // Defaults
    public static final String DEFAULT_REPLICATION_STRATEGY = "NetworkTopologyStrategy";
    public static final String DEFAULT_REPLICATION_FACTOR = "usw2:3";
    public static final String DEFAULT_CASSANDRA_VERSION = CassandraVersion.FOURZERO.name();

    @NotNull
    private final String clusterName;
    @NotNull
    private final String keyspace;
    @NotNull
    private final String table;
    @Nullable
    private final String datacenter;
    @NotNull
    private final String tableCreateStmt;
    @NotNull
    private final String cassandraVersion;
    @NotNull
    private final String udts;
    @NotNull
    private final String replicationStrategy;
    @NotNull
    private final String replicationFactor;

    private CassandraSchemaConfig(@NotNull String clusterName,
                                  @NotNull String keyspace,
                                  @NotNull String table,
                                  @Nullable String datacenter,
                                  @NotNull String tableCreateStmt,
                                  @NotNull String cassandraVersion,
                                  @NotNull String udts,
                                  @NotNull String replicationStrategy,
                                  @NotNull String replicationFactor)
    {
        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.table = table;
        this.datacenter = datacenter;
        this.tableCreateStmt = tableCreateStmt;
        this.cassandraVersion = cassandraVersion;
        this.udts = udts;
        this.replicationStrategy = replicationStrategy;
        this.replicationFactor = replicationFactor;
    }

    /**
     * Create a CassandraSchemaConfig from a map of options.
     * <p>
     * Cluster identity is a single string downstream: it flows into the {@link
     * org.apache.cassandra.spark.data.backup.BackupReader} contract as one {@code clusterName}
     * parameter, and the {@link BackupReader} implementation is free to interpret it however it
     * needs (UUID, human-readable name, etc.). For caller convenience, two option keys are
     * accepted at the DataSource boundary and aliased onto that single field: {@code clusterId}
     * and {@code clusterName}. At least one must be provided; when both are set, {@code
     * clusterId} wins.
     *
     * @param options Configuration options map (case-insensitive keys supported via MapUtils)
     * @return New CassandraSchemaConfig instance
     * @throws IllegalArgumentException if required options are missing
     */
    public static CassandraSchemaConfig create(Map<String, String> options)
    {
        String clusterNameOpt = MapUtils.getOrDefault(options, CLUSTER_NAME_KEY, null);
        String clusterIdOpt = MapUtils.getOrDefault(options, CLUSTER_ID_KEY, null);

        if ((clusterNameOpt == null || clusterNameOpt.trim().isEmpty())
            && (clusterIdOpt == null || clusterIdOpt.trim().isEmpty()))
        {
            throw new IllegalArgumentException(
                "At least one of 'clusterName' or 'clusterId' must be provided");
        }

        // clusterId, if provided, wins. Otherwise pass clusterName through unmodified — specific backup reader
        // implementations may interpret it however they need.
        String resolvedClusterId;
        if (clusterIdOpt != null && !clusterIdOpt.trim().isEmpty())
        {
            resolvedClusterId = clusterIdOpt;
            if (clusterNameOpt != null && !clusterNameOpt.trim().isEmpty())
            {
                LOGGER.info("Both 'clusterName' ({}) and 'clusterId' ({}) provided; "
                            + "'clusterId' takes priority",
                            clusterNameOpt, clusterIdOpt);
            }
        }
        else
        {
            resolvedClusterId = clusterNameOpt;
        }

        String keyspace = MapUtils.getOrThrow(options, KEYSPACE_KEY, "keyspace");
        String table = MapUtils.getOrThrow(options, TABLE_KEY, "table");

        // Support both "datacenter" and "dc" keys (case-insensitive)
        String datacenter = MapUtils.getOrDefault(options, DATACENTER_KEY, null);
        if (datacenter == null || datacenter.isEmpty())
        {
            datacenter = MapUtils.getOrDefault(options, DC_KEY, null);
        }

        String tableCreateStmt = MapUtils.getOrThrow(options, TABLE_CREATE_STMT_KEY, "tableCreateStmt");
        String cassandraVersion = MapUtils.getOrDefault(options, CASSANDRA_VERSION_KEY, DEFAULT_CASSANDRA_VERSION);
        String udts = MapUtils.getOrDefault(options, UDTS_KEY, "");
        String replicationStrategy = MapUtils.getOrDefault(options,
            REPLICATION_STRATEGY_KEY, DEFAULT_REPLICATION_STRATEGY);
        String replicationFactor = MapUtils.getOrDefault(options,
            REPLICATION_FACTOR_KEY, DEFAULT_REPLICATION_FACTOR);

        return new CassandraSchemaConfig(resolvedClusterId, keyspace, table, datacenter,
            tableCreateStmt, cassandraVersion, udts, replicationStrategy, replicationFactor);
    }

    /**
     * Get the cluster identifier. Resolved eagerly at config creation: the {@code clusterId}
     * option wins when present, otherwise {@code clusterName} is passed through verbatim. The
     * concrete {@link org.apache.cassandra.spark.data.backup.BackupReader} decides how to
     * interpret the value (UUID, human-readable name, etc.).
     */
    @NotNull
    public String clusterName()
    {
        return clusterName;
    }

    /**
     * Get the Cassandra keyspace name.
     */
    @NotNull
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * Get the Cassandra table name.
     */
    @NotNull
    public String table()
    {
        return table;
    }

    /**
     * Get the datacenter name.
     *
     * @return Datacenter name or null if not specified
     */
    @Nullable
    public String datacenter()
    {
        return datacenter;
    }

    /**
     * Get the CREATE TABLE statement for the Cassandra table.
     */
    @NotNull
    public String tableCreateStmt()
    {
        return tableCreateStmt;
    }

    /**
     * Get the Cassandra version as a string.
     */
    @NotNull
    public String cassandraVersionString()
    {
        return cassandraVersion;
    }

    /**
     * Get the Cassandra version as an enum.
     */
    @NotNull
    public CassandraVersion cassandraVersion()
    {
        return CassandraVersion.valueOf(cassandraVersion);
    }

    /**
     * Get the raw UDT definitions string.
     * UDTs are separated by newlines.
     */
    @NotNull
    public String udts()
    {
        return udts;
    }

    /**
     * Parse UDT definitions into a set of individual UDT statements.
     *
     * @return Set of UDT definition strings
     */
    @NotNull
    public Set<String> parsedUdts()
    {
        return Arrays.stream(udts.split("\n"))
                     .filter(StringUtils::isNotEmpty)
                     .collect(Collectors.toSet());
    }

    /**
     * Get the replication strategy name.
     */
    @NotNull
    public String replicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * Get the raw replication factor string.
     * Format depends on strategy: "3" for SimpleStrategy, "dc1:3,dc2:3" for NetworkTopologyStrategy.
     */
    @NotNull
    public String replicationFactorString()
    {
        return replicationFactor;
    }

    /**
     * Parse the replication factor configuration and create a ReplicationFactor object.
     * The replicationFactor string format is "datacenter:factor,datacenter:factor"
     * e.g., "usw2:3,euw1:3" for NetworkTopologyStrategy or "3" for SimpleStrategy.
     *
     * @return ReplicationFactor object based on the configured strategy and factors
     * @throws IllegalArgumentException if the format is invalid
     */
    @NotNull
    public ReplicationFactor getParsedReplicationFactor()
    {
        ReplicationFactor.ReplicationStrategy strategy =
            ReplicationFactor.ReplicationStrategy.getEnum(replicationStrategy);

        if (strategy == ReplicationFactor.ReplicationStrategy.SimpleStrategy)
        {
            try
            {
                int factor = Integer.parseInt(replicationFactor);
                return ReplicationFactor.simpleStrategy(factor);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(
                    "For SimpleStrategy, replicationFactor must be a single integer, got: " + replicationFactor, e);
            }
        }
        else if (strategy == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            Map<String, Integer> dcFactors = new HashMap<>();

            if (replicationFactor.trim().isEmpty())
            {
                throw new IllegalArgumentException("Replication factor cannot be empty for NetworkTopologyStrategy");
            }

            String[] pairs = replicationFactor.split(",");
            for (String pair : pairs)
            {
                String trimmedPair = pair.trim();
                if (trimmedPair.isEmpty())
                {
                    continue;
                }

                String[] parts = trimmedPair.split(":");
                if (parts.length != 2)
                {
                    throw new IllegalArgumentException(
                        "Invalid replication factor format. Expected 'datacenter:factor', got: " + trimmedPair);
                }
                try
                {
                    String dc = parts[0].trim();
                    String factorStr = parts[1].trim();

                    if (dc.isEmpty())
                    {
                        throw new IllegalArgumentException("Datacenter name cannot be empty in: " + trimmedPair);
                    }
                    if (factorStr.isEmpty())
                    {
                        throw new IllegalArgumentException("Replication factor cannot be empty in: " + trimmedPair);
                    }

                    int factor = Integer.parseInt(factorStr);
                    dcFactors.put(dc, factor);
                }
                catch (NumberFormatException e)
                {
                    throw new IllegalArgumentException("Invalid replication factor number in: " + trimmedPair, e);
                }
            }
            return new ReplicationFactor(strategy, dcFactors);
        }
        else if (strategy == ReplicationFactor.ReplicationStrategy.LocalStrategy)
        {
            return new ReplicationFactor(strategy, new HashMap<>());
        }
        else
        {
            throw new IllegalStateException("Unknown ReplicationStrategy: " + strategy);
        }
    }
}
