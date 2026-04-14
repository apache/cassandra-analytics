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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.exception.UnsupportedAnalyticsOperationException;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.maybeQuotedIdentifier;

/**
 * Schema information for bulk write operations.
 * <p>
 * This class does NOT implement Serializable (Logger is not serializable).
 * For broadcast to executors, {@link BroadcastableTableSchema} is used instead,
 * and executors reconstruct TableSchema from the broadcastable data.
 */
public class TableSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchema.class);

    final String createStatement;
    final String modificationStatement;
    final List<String> partitionKeyColumns;
    final List<ColumnType<?>> partitionKeyColumnTypes;
    final List<SqlToCqlTypeConverter.Converter<?>> converters;
    final List<Integer> keyFieldPositions;
    final WriteMode writeMode;
    final TTLOption ttlOption;
    final TimestampOption timestampOption;
    final String lowestCassandraVersion;
    final boolean quoteIdentifiers;
    final Set<String> indexStatements;

    public TableSchema(StructType dfSchema,
                       TableInfoProvider tableInfo,
                       WriteMode writeMode,
                       TTLOption ttlOption,
                       TimestampOption timestampOption,
                       String lowestCassandraVersion,
                       boolean quoteIdentifiers,
                       boolean skipSecondaryIndexCheck)
    {
        this.writeMode = writeMode;
        this.ttlOption = ttlOption;

        this.timestampOption = timestampOption;
        this.lowestCassandraVersion = lowestCassandraVersion;
        this.quoteIdentifiers = quoteIdentifiers;
        this.indexStatements = tableInfo.getIndexStatements();

        validateDataFrameCompatibility(dfSchema, tableInfo);
        validateSecondaryIndexes(tableInfo, skipSecondaryIndexCheck, indexStatements, lowestCassandraVersion);
        validateUserAddedColumns(lowestCassandraVersion, quoteIdentifiers, ttlOption, timestampOption);

        this.createStatement = getCreateStatement(tableInfo);
        this.modificationStatement = getModificationStatement(dfSchema, tableInfo);
        this.partitionKeyColumns = getPartitionKeyColumnNames(tableInfo);
        this.partitionKeyColumnTypes = getPartitionKeyColumnTypes(tableInfo);
        this.converters = getConverters(dfSchema, tableInfo, ttlOption, timestampOption);
        LOGGER.info("Converters: {}", converters);
        this.keyFieldPositions = getKeyFieldPositions(dfSchema, tableInfo.getColumnNames(), getRequiredKeyColumns(tableInfo));
    }

    /**
     * Reconstruct TableSchema from BroadcastableTableSchema on executor.
     * This constructor is used only on executors when reconstructing from broadcast data.
     *
     * @param broadcastable the broadcastable table schema from broadcast
     */
    public TableSchema(BroadcastableTableSchema broadcastable)
    {
        this.createStatement = broadcastable.getCreateStatement();
        this.modificationStatement = broadcastable.getModificationStatement();
        this.partitionKeyColumns = broadcastable.getPartitionKeyColumns();
        this.partitionKeyColumnTypes = broadcastable.getPartitionKeyColumnTypes();
        this.converters = broadcastable.getConverters();
        this.keyFieldPositions = broadcastable.getKeyFieldPositions();
        this.writeMode = broadcastable.getWriteMode();
        this.ttlOption = broadcastable.getTtlOption();
        this.timestampOption = broadcastable.getTimestampOption();
        this.lowestCassandraVersion = broadcastable.getLowestCassandraVersion();
        this.quoteIdentifiers = broadcastable.isQuoteIdentifiers();
        this.indexStatements = broadcastable.getIndexStatements();
    }

    private List<String> getRequiredKeyColumns(TableInfoProvider tableInfo)
    {
        switch (writeMode)
        {
            case INSERT:
                // Inserts require all primary key columns
                return tableInfo.getPrimaryKeyColumnNames();
            case DELETE_PARTITION:
                // To delete a partition, we only need the partition key columns, not all primary key columns
                return tableInfo.getPartitionKeyColumnNames();
            default:
                throw new UnsupportedOperationException("Unknown WriteMode provided");
        }
    }

    private static List<SqlToCqlTypeConverter.Converter<?>> getConverters(StructType dfSchema,
                                                                          TableInfoProvider tableInfo,
                                                                          TTLOption ttlOption,
                                                                          TimestampOption timestampOption)
    {
        return Arrays.stream(dfSchema.fieldNames())
                     .map(fieldName -> {
                         if (fieldName.equals(ttlOption.columnName()))
                         {
                             return SqlToCqlTypeConverter.integerConverter();
                         }
                         if (fieldName.equals(timestampOption.columnName()))
                         {
                             return SqlToCqlTypeConverter.microsecondsTimestampConverter();
                         }
                         CqlField.CqlType cqlType = tableInfo.getColumnType(fieldName);
                         return SqlToCqlTypeConverter.getConverter(cqlType);
                     })
                     .collect(Collectors.toList());
    }

    private static List<ColumnType<?>> getPartitionKeyColumnTypes(TableInfoProvider tableInfo)
    {
        return tableInfo.getPartitionKeyTypes();
    }

    private static List<String> getPartitionKeyColumnNames(TableInfoProvider tableInfo)
    {
        return tableInfo.getPartitionKeyColumnNames();
    }

    private static String getCreateStatement(TableInfoProvider tableInfo)
    {
        String createStatement = tableInfo.getCreateStatement();
        LOGGER.info("CQL create statement for the table {}", createStatement);
        return createStatement;
    }

    private String getModificationStatement(StructType dfSchema, TableInfoProvider tableInfo)
    {
        switch (writeMode)
        {
            case INSERT:
                return getInsertStatement(dfSchema, tableInfo, ttlOption, timestampOption);
            case DELETE_PARTITION:
                return getDeleteStatement(dfSchema, tableInfo);
            default:
                throw new UnsupportedOperationException("Unknown WriteMode provided");
        }
    }

    private String getInsertStatement(StructType dfSchema,
                                      TableInfoProvider tableInfo,
                                      TTLOption ttlOption,
                                      TimestampOption timestampOption)
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(lowestCassandraVersion);

        List<String> columnNames = Arrays.stream(dfSchema.fieldNames())
                                         .filter(fieldName -> !fieldName.equals(ttlOption.columnName()))
                                         .filter(fieldName -> !fieldName.equals(timestampOption.columnName()))
                                         .collect(Collectors.toList());
        StringBuilder stringBuilder = new StringBuilder("INSERT INTO ")
                                      .append(maybeQuotedIdentifier(bridge, quoteIdentifiers, tableInfo.getKeyspaceName()))
                                      .append(".")
                                      .append(maybeQuotedIdentifier(bridge, quoteIdentifiers, tableInfo.getName()))
                                      .append(columnNames.stream()
                                                         .map(columnName -> maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName))
                                                         .collect(Collectors.joining(",", " (", ") ")));

        stringBuilder.append("VALUES")
                     .append(columnNames.stream()
                                        .map(columnName -> ":" + maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName))
                                        .collect(Collectors.joining(",", " (", ")")));
        if (ttlOption.withTTl() && timestampOption.withTimestamp())
        {
            stringBuilder.append(" USING TIMESTAMP ")
                         .append(timestampOption.toCQLString(columnName -> maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName)))
                         .append(" AND TTL ")
                         .append(ttlOption.toCQLString(columnName -> maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName)));
        }
        else if (timestampOption.withTimestamp())
        {
            stringBuilder.append(" USING TIMESTAMP ")
                         .append(timestampOption.toCQLString(columnName -> maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName)));
        }
        else if (ttlOption.withTTl())
        {
            stringBuilder.append(" USING TTL ")
                         .append(ttlOption.toCQLString(columnName -> maybeQuotedIdentifier(bridge, quoteIdentifiers, columnName)));
        }
        stringBuilder.append(";");
        String insertStatement = stringBuilder.toString();

        LOGGER.info("CQL insert statement for the RDD {}", insertStatement);
        return insertStatement;
    }

    private String getDeleteStatement(StructType dfSchema, TableInfoProvider tableInfo)
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(lowestCassandraVersion);
        Stream<String> fieldEqualityStatements = Arrays.stream(dfSchema.fieldNames()).map(key -> maybeQuotedIdentifier(bridge, quoteIdentifiers, key) + "=?");
        String deleteStatement = String.format("DELETE FROM %s.%s where %s;",
                                               maybeQuotedIdentifier(bridge, quoteIdentifiers, tableInfo.getKeyspaceName()),
                                               maybeQuotedIdentifier(bridge, quoteIdentifiers, tableInfo.getName()),
                                               fieldEqualityStatements.collect(Collectors.joining(" AND ")));

        LOGGER.info("CQL delete statement for the RDD {}", deleteStatement);
        return deleteStatement;
    }

    private void validateDataFrameCompatibility(StructType dfSchema, TableInfoProvider tableInfo)
    {
        Set<String> dfFields = new LinkedHashSet<>();
        Collections.addAll(dfFields, dfSchema.fieldNames());

        validatePrimaryKeyColumnsProvided(tableInfo, dfFields);

        switch (writeMode)
        {
            case INSERT:
                validateDataframeFieldsInTable(tableInfo, dfFields, ttlOption, timestampOption);
                return;
            case DELETE_PARTITION:
                validateOnlyPartitionKeyColumnsInDataframe(tableInfo, dfFields);
                return;
            default:
                LOGGER.warn("Unrecognized write mode {}", writeMode);
        }
    }

    private void validateOnlyPartitionKeyColumnsInDataframe(TableInfoProvider tableInfo, Set<String> dfFields)
    {
        Set<String> requiredKeyColumns = new LinkedHashSet<>(getRequiredKeyColumns(tableInfo));
        Preconditions.checkArgument(requiredKeyColumns.equals(dfFields),
                                    String.format("Only partition key columns (%s) are supported in the input Dataframe"
                                                  + " when WRITE_MODE=DELETE_PARTITION but (%s) columns were provided",
                                                  String.join(",", requiredKeyColumns), String.join(",", dfFields)));
    }

    private void validatePrimaryKeyColumnsProvided(TableInfoProvider tableInfo, Set<String> dfFields)
    {
        // Make sure all primary key columns are provided
        List<String> requiredKeyColumns = getRequiredKeyColumns(tableInfo);
        Preconditions.checkArgument(dfFields.containsAll(requiredKeyColumns),
                                    "Missing some required key components in DataFrame => "
                                    + requiredKeyColumns
                                      .stream()
                                      .filter(column -> !dfFields.contains(column))
                                      .collect(Collectors.joining(",")));
    }

    private static void validateDataframeFieldsInTable(TableInfoProvider tableInfo, Set<String> dfFields,
                                                       TTLOption ttlOption, TimestampOption timestampOption)
    {
        // Make sure all fields in DF schema are part of table
        List<String> unknownFields = dfFields
                                     .stream()
                                     .filter(columnName -> !tableInfo.columnExists(columnName))
                                     .filter(columnName -> !columnName.equals(ttlOption.columnName()))
                                     .filter(columnName -> !columnName.equals(timestampOption.columnName()))
                                     .collect(Collectors.toList());

        Preconditions.checkArgument(unknownFields.isEmpty(), "Unknown fields in data frame => " + unknownFields);
    }

    /**
     * Validates secondary index constraints for bulk write operations.
     * <p>
     * When the cluster is Cassandra 5.0+ and ALL indexes are SAI, the write is allowed because
     * SAI index components are generated alongside SSTables and are immediately queryable after import.
     * <p>
     * When any index is non-SAI (legacy 2i), the write is blocked unless SKIP_SECONDARY_INDEX_CHECK is set.
     *
     * @param tableInfo               the table info provider
     * @param skipSecondaryIndexCheck  whether the user explicitly opted out of the check
     * @param indexStatements          the CREATE INDEX statements for the table
     * @param lowestCassandraVersion   the lowest Cassandra version in the cluster
     */
    static void validateSecondaryIndexes(TableInfoProvider tableInfo,
                                         boolean skipSecondaryIndexCheck,
                                         Set<String> indexStatements,
                                         String lowestCassandraVersion)
    {
        if (!tableInfo.hasSecondaryIndex())
        {
            return; // No indexes — nothing to validate
        }

        boolean allSai = !indexStatements.isEmpty() && indexStatements.stream().allMatch(CqlUtils::isSaiIndex);
        boolean isCassandra5OrLater = isCassandra5OrLater(lowestCassandraVersion);

        if (allSai && isCassandra5OrLater)
        {
            LOGGER.info("Table has SAI indexes only on Cassandra 5.0+. SAI index components will be generated "
                      + "alongside SSTables for immediate queryability after import. indexCount={}", indexStatements.size());
            return;
        }

        if (skipSecondaryIndexCheck)
        {
            LOGGER.warn("Bulk writing to tables with SecondaryIndexes will have an asynchronous index rebuild "
                      + "take place automatically after writing. Reads against the index during this time "
                      + "window will produce inconsistent or stale results until index rebuild is complete.");
            return;
        }

        throw new UnsupportedAnalyticsOperationException("Bulkwriter doesn't support secondary indexes");
    }

    static void validateNoSecondaryIndexes(TableInfoProvider tableInfo)
    {
        if (tableInfo.hasSecondaryIndex())
        {
            throw new UnsupportedAnalyticsOperationException("Bulkwriter doesn't support secondary indexes");
        }
    }

    public Set<String> getIndexStatements()
    {
        return indexStatements;
    }

    @VisibleForTesting
    static boolean isCassandra5OrLater(String version)
    {
        if (version == null || version.isEmpty())
        {
            return false;
        }
        try
        {
            int majorVersion = Integer.parseInt(version.split("\\.")[0]);
            return majorVersion >= 5;
        }
        catch (NumberFormatException exception)
        {
            return false;
        }
    }

    private static List<Integer> getKeyFieldPositions(StructType dfSchema,
                                                      List<String> columnNames,
                                                      List<String> keyFieldNames)
    {
        List<String> dfFieldNames = Arrays.asList(dfSchema.fieldNames());
        return columnNames.stream()
                          .filter(keyFieldNames::contains)
                          .map(dfFieldNames::indexOf)
                          .collect(Collectors.toList());
    }

    private static void validateUserAddedColumns(String lowestCassandraVersion, boolean quoteIdentifiers,
                                                 TTLOption ttlOption, TimestampOption timestampOption)
    {
        if (!quoteIdentifiers)
        {
            CassandraBridge bridge = CassandraBridgeFactory.get(lowestCassandraVersion);
            validateColumnName(bridge, ttlOption.columnName(), WriterOptions.TTL.name());
            validateColumnName(bridge, timestampOption.columnName(), WriterOptions.TIMESTAMP.name());
        }
    }

    /**
     * Validates that the provided column name matches what would be produced by maybeQuoteIdentifier. If they don't
     * match, it means the user provided a column name that needs quoting but didn't enable QUOTE_IDENTIFIERS option.
     * We throw early to avoid scenarios such as, mismatches in column names leads to bulk write overwriting existing
     * TTL values to null.
     *
     * @param bridge     the Cassandra bridge
     * @param columnName the column name to validate
     * @param optionName the option name for error messages
     * @throws IllegalArgumentException if the column name requires quoting but QUOTE_IDENTIFIERS is not enabled
     */
    private static void validateColumnName(CassandraBridge bridge, String columnName, String optionName)
    {
        if (columnName == null || columnName.isEmpty())
        {
            return;
        }

        String quotedName = bridge.maybeQuoteIdentifier(columnName);
        if (!columnName.equals(quotedName))
        {
            throw new IllegalArgumentException(
            String.format("The %s column name %s requires spark option %s set to true for correct conversion. Bulk " +
                          "write should provide a column name that matches CQL requirements, or set %s to true to " +
                          "enable quoting for all identifiers.", optionName, columnName,
                          WriterOptions.QUOTE_IDENTIFIERS.name(), WriterOptions.QUOTE_IDENTIFIERS.name()));
        }
    }
}
