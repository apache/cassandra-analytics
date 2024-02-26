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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.BigNumberConfigImpl;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.utils.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.data.CassandraDataLayer.aliasLastModifiedTimestamp;

public class ClientConfig
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String SIDECAR_INSTANCES = "sidecar_instances";
    public static final String KEYSPACE_KEY = "keyspace";
    public static final String TABLE_KEY = "table";
    public static final String SNAPSHOT_NAME_KEY = "snapshotName";
    public static final String DC_KEY = "dc";
    public static final String CREATE_SNAPSHOT_KEY = "createSnapshot";
    public static final String CLEAR_SNAPSHOT_KEY = "clearSnapshot";
    /**
     * Format of clearSnapshotStrategy is {strategy [snapshotTTLvalue]}, clearSnapshotStrategy holds both the strategy
     * and in case of TTL based strategy, TTL value. For e.g. onCompletionOrTTL 2d, TTL 2d, noOp, onCompletion. For
     * clear snapshot strategies allowed check {@link ClearSnapshotStrategy}
     */
    public static final String CLEAR_SNAPSHOT_STRATEGY_KEY = "clearSnapshotStrategy";
    /**
     * TTL value is time to live option for the snapshot (available since Cassandra 4.1+). TTL value specified must
     * contain unit along. For e.g. 2d represents a TTL for 2 days; 1h represents a TTL of 1 hour, etc.
     * Valid units are {@code d}, {@code h}, {@code s} and {@code m}.
     */
    public static final String DEFAULT_SNAPSHOT_TTL_VALUE = "2d";
    public static final String SNAPSHOT_TTL_PATTERN = "\\d+(d|h|m|s)";
    public static final String DEFAULT_PARALLELISM_KEY = "defaultParallelism";
    public static final String NUM_CORES_KEY = "numCores";
    public static final String CONSISTENCY_LEVEL_KEY = "consistencyLevel";
    public static final String ENABLE_STATS_KEY = "enableStats";
    public static final String LAST_MODIFIED_COLUMN_NAME_KEY = "lastModifiedColumnName";
    public static final String READ_INDEX_OFFSET_KEY = "readIndexOffset";
    public static final String SIZING_KEY = "sizing";
    public static final String SIZING_DEFAULT = "default";
    public static final String MAX_PARTITION_SIZE_KEY = "maxPartitionSize";
    public static final String USE_INCREMENTAL_REPAIR = "useIncrementalRepair";
    public static final String ENABLE_EXPANSION_SHRINK_CHECK_KEY = "enableExpansionShrinkCheck";
    public static final String SIDECAR_PORT = "sidecar_port";
    public static final String QUOTE_IDENTIFIERS = "quote_identifiers";
    public static final int DEFAULT_SIDECAR_PORT = 9043;

    protected String sidecarInstances;
    @Nullable
    protected String keyspace;
    @Nullable
    protected String table;
    protected String snapshotName;
    protected String datacenter;
    protected boolean createSnapshot;
    protected boolean clearSnapshot;
    protected ClearSnapshotStrategy clearSnapshotStrategy;
    protected int defaultParallelism;
    protected int numCores;
    protected ConsistencyLevel consistencyLevel;
    protected Map<String, BigNumberConfigImpl> bigNumberConfigMap;
    protected boolean enableStats;
    protected boolean readIndexOffset;
    protected String sizing;
    protected int maxPartitionSize;
    protected boolean useIncrementalRepair;
    protected List<SchemaFeature> requestedFeatures;
    protected String lastModifiedTimestampField;
    protected Boolean enableExpansionShrinkCheck;
    protected int sidecarPort;
    protected boolean quoteIdentifiers;

    protected ClientConfig(Map<String, String> options)
    {
        this.sidecarInstances = parseSidecarInstances(options);
        this.keyspace = MapUtils.getOrThrow(options, KEYSPACE_KEY, "keyspace");
        this.table = MapUtils.getOrThrow(options, TABLE_KEY, "table");
        this.snapshotName = MapUtils.getOrDefault(options, SNAPSHOT_NAME_KEY, "sbr_" + UUID.randomUUID().toString().replace("-", ""));
        this.datacenter = options.get(MapUtils.lowerCaseKey(DC_KEY));
        this.createSnapshot = MapUtils.getBoolean(options, CREATE_SNAPSHOT_KEY, true);
        this.clearSnapshot = MapUtils.getBoolean(options, CLEAR_SNAPSHOT_KEY, createSnapshot);
        String clearSnapshotStrategyOption = MapUtils.getOrDefault(options, CLEAR_SNAPSHOT_STRATEGY_KEY, null);

        this.clearSnapshotStrategy = parseClearSnapshotStrategy(MapUtils.containsKey(options, CLEAR_SNAPSHOT_KEY),
                                                                clearSnapshot,
                                                                clearSnapshotStrategyOption);
        this.defaultParallelism = MapUtils.getInt(options, DEFAULT_PARALLELISM_KEY, 1);
        this.numCores = MapUtils.getInt(options, NUM_CORES_KEY, 1);
        this.consistencyLevel = Optional.ofNullable(options.get(MapUtils.lowerCaseKey(CONSISTENCY_LEVEL_KEY)))
                                        .map(ConsistencyLevel::valueOf)
                                        .orElse(null);
        this.bigNumberConfigMap = BigNumberConfigImpl.build(options);
        this.enableStats = MapUtils.getBoolean(options, ENABLE_STATS_KEY, true);
        this.readIndexOffset = MapUtils.getBoolean(options, READ_INDEX_OFFSET_KEY, true);
        this.sizing = MapUtils.getOrDefault(options, SIZING_KEY, SIZING_DEFAULT);
        this.maxPartitionSize = MapUtils.getInt(options, MAX_PARTITION_SIZE_KEY, 1);
        this.useIncrementalRepair = MapUtils.getBoolean(options, USE_INCREMENTAL_REPAIR, true);
        this.lastModifiedTimestampField = MapUtils.getOrDefault(options, LAST_MODIFIED_COLUMN_NAME_KEY, null);
        this.enableExpansionShrinkCheck = MapUtils.getBoolean(options, ENABLE_EXPANSION_SHRINK_CHECK_KEY, false);
        this.requestedFeatures = initRequestedFeatures(options);
        this.sidecarPort = MapUtils.getInt(options, SIDECAR_PORT, DEFAULT_SIDECAR_PORT);
        this.quoteIdentifiers = MapUtils.getBoolean(options, QUOTE_IDENTIFIERS, false);
    }

    protected String parseSidecarInstances(Map<String, String> options)
    {
        return MapUtils.getOrThrow(options, SIDECAR_INSTANCES, "sidecar_instances");
    }

    protected ClearSnapshotStrategy parseClearSnapshotStrategy(boolean hasDeprecatedOption,
                                                               boolean clearSnapshot,
                                                               String clearSnapshotStrategyOption)
    {
        if (hasDeprecatedOption)
        {
            LOGGER.warn("The deprecated option 'clearSnapshot' is set. Please set 'clearSnapshotStrategy' instead.");
            if (clearSnapshotStrategyOption == null)
            {
                return clearSnapshot ? ClearSnapshotStrategy.defaultStrategy() : new ClearSnapshotStrategy.NoOp();
            }
        }
        return ClearSnapshotStrategy.parse(clearSnapshotStrategyOption);
    }

    public String sidecarInstances()
    {
        return sidecarInstances;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public String snapshotName()
    {
        return snapshotName;
    }

    public String datacenter()
    {
        return datacenter;
    }

    public boolean createSnapshot()
    {
        return createSnapshot;
    }

    public boolean clearSnapshot()
    {
        return clearSnapshot;
    }

    public ClearSnapshotStrategy clearSnapshotStrategy()
    {
        return clearSnapshotStrategy;
    }

    public int defaultParallelism()
    {
        return defaultParallelism;
    }

    public int numCores()
    {
        return numCores;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    public Map<String, BigNumberConfigImpl> bigNumberConfigMap()
    {
        return bigNumberConfigMap;
    }

    public boolean enableStats()
    {
        return enableStats;
    }

    public boolean readIndexOffset()
    {
        return readIndexOffset;
    }

    public String sizing()
    {
        return sizing;
    }

    public int maxPartitionSize()
    {
        return maxPartitionSize;
    }

    public boolean useIncrementalRepair()
    {
        return useIncrementalRepair;
    }

    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    public String lastModifiedTimestampField()
    {
        return lastModifiedTimestampField;
    }

    public Boolean enableExpansionShrinkCheck()
    {
        return enableExpansionShrinkCheck;
    }

    public int sidecarPort()
    {
        return sidecarPort;
    }

    public boolean quoteIdentifiers()
    {
        return quoteIdentifiers;
    }

    public static ClientConfig create(Map<String, String> options)
    {
        return new ClientConfig(options);
    }

    protected List<SchemaFeature> initRequestedFeatures(Map<String, String> options)
    {
        Map<String, String> optionsCopy = new HashMap<>(options);
        String lastModifiedColumnName = MapUtils.getOrDefault(options, LAST_MODIFIED_COLUMN_NAME_KEY, null);
        if (lastModifiedColumnName != null)
        {
            optionsCopy.put(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName(), "true");
        }
        List<SchemaFeature> requestedFeatures = SchemaFeatureSet.initializeFromOptions(optionsCopy);
        if (lastModifiedColumnName != null)
        {
            // Create alias to LAST_MODIFICATION_TIMESTAMP
            aliasLastModifiedTimestamp(requestedFeatures, lastModifiedColumnName);
        }
        return requestedFeatures;
    }

    public abstract static class ClearSnapshotStrategy
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(ClearSnapshotStrategy.class);
        private final String snapshotTTL;

        public ClearSnapshotStrategy(String snapshotTTL)
        {
            this.snapshotTTL = snapshotTTL;
        }

        public abstract boolean shouldClearOnCompletion();

        protected void validateTTLPresence(boolean expectTTL)
        {
            if (expectTTL && !hasTTL())
            {
                throw new IllegalArgumentException("Incorrect value set for clearSnapshotStrategy, expected format " +
                                                   "is {strategy [snapshotTTLvalue]}. TTL value specified must " +
                                                   "contain unit along. For e.g. 2d represents a TTL for 2 days. " +
                                                   "Allowed units are d, h, m and s.");
            }
        }

        public boolean hasTTL()
        {
            return snapshotTTL != null && !snapshotTTL.isEmpty();
        }

        @Nullable
        public String ttl()
        {
            return snapshotTTL;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + (hasTTL() ? ' ' + ttl() : "");
        }

        public static ClearSnapshotStrategy parse(String clearSnapshotStrategyOption)
        {
            if (clearSnapshotStrategyOption == null)
            {
                LOGGER.debug("No clearSnapshotStrategy is set. Using the default strategy");
                return ClearSnapshotStrategy.defaultStrategy();
            }
            String[] strategyParts = clearSnapshotStrategyOption.split(" ", 2);
            String strategyName;
            String snapshotTTL = null;
            if (strategyParts.length == 1)
            {
                strategyName = strategyParts[0].trim();
            }
            else if (strategyParts.length == 2)
            {
                strategyName = strategyParts[0].trim();
                snapshotTTL = strategyParts[1].trim();
                if (!Pattern.matches(SNAPSHOT_TTL_PATTERN, snapshotTTL))
                {
                    String msg = "Incorrect value set for clearSnapshotStrategy, expected format is " +
                                 "{strategy [snapshotTTLvalue]}. TTL value specified must contain unit along. " +
                                 "For e.g. 2d represents a TTL for 2 days. Allowed units are d, h, m and s.";
                    throw new IllegalArgumentException(msg);
                }
            }
            else
            {
                LOGGER.error("Invalid value for ClearSnapshotStrategy: '{}'", clearSnapshotStrategyOption);
                throw new IllegalArgumentException("Invalid value: " + clearSnapshotStrategyOption);
            }
            return ClearSnapshotStrategy.create(strategyName, snapshotTTL);
        }

        public static ClearSnapshotStrategy create(String name, String snapshotTTL)
        {
            String stripped = name.trim();
            if (stripped.equalsIgnoreCase(OnCompletion.class.getSimpleName()))
            {
                return new OnCompletion();
            }
            else if (stripped.equalsIgnoreCase(TTL.class.getSimpleName()))
            {
                return new TTL(snapshotTTL);
            }
            else if (stripped.equalsIgnoreCase(OnCompletionOrTTL.class.getSimpleName()))
            {
                return new OnCompletionOrTTL(snapshotTTL);
            }
            else if (stripped.equalsIgnoreCase(NoOp.class.getSimpleName()))
            {
                return new NoOp();
            }
            else
            {
                ClearSnapshotStrategy defaultStrategy = defaultStrategy();
                LOGGER.warn("Unknown ClearSnapshotStrategy {} is passed. Fall back to default strategy {}.",
                            name, defaultStrategy);
                throw new IllegalArgumentException("Invalid ClearSnapshotStrategy " + name + " passed");
            }
        }

        public static ClearSnapshotStrategy defaultStrategy()
        {
            LOGGER.info("A default TTL value of {} is added to the snapshot. If the job takes longer than {}, " +
                        "the snapshot will be cleared before job completion leading to errors.",
                        DEFAULT_SNAPSHOT_TTL_VALUE, DEFAULT_SNAPSHOT_TTL_VALUE);
            return new OnCompletionOrTTL(DEFAULT_SNAPSHOT_TTL_VALUE);
        }

        public static class OnCompletion extends ClearSnapshotStrategy
        {
            public OnCompletion()
            {
                super(null);
            }

            @Override
            public boolean shouldClearOnCompletion()
            {
                return true;
            }
        }

        public static class NoOp extends ClearSnapshotStrategy
        {
            public NoOp()
            {
                super(null);
            }

            @Override
            public boolean shouldClearOnCompletion()
            {
                return false;
            }
        }

        public static class OnCompletionOrTTL extends ClearSnapshotStrategy
        {
            public OnCompletionOrTTL(@NotNull String snapshotTTL)
            {
                super(snapshotTTL);
                validateTTLPresence(true);
            }

            @Override
            public boolean shouldClearOnCompletion()
            {
                return true;
            }
        }

        public static class TTL extends ClearSnapshotStrategy
        {
            public TTL(@NotNull String snapshotTTL)
            {
                super(snapshotTTL);
                validateTTLPresence(true);
            }

            @Override
            public boolean shouldClearOnCompletion()
            {
                return false;
            }
        }
    }
}
