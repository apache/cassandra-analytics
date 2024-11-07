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

import java.io.Serializable;
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
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.maybeQuotedIdentifier;

public class TableSchema implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchema.class);

    final String createStatement;
    final String modificationStatement;
    final List<String> partitionKeyColumns;
    final List<ColumnType<?>> partitionKeyColumnTypes;
    final List<SqlToCqlTypeConverter.Converter<?>> converters;
    private final List<Integer> keyFieldPositions;
    private final WriteMode writeMode;
    private final TTLOption ttlOption;
    private final TimestampOption timestampOption;
    private final String lowestCassandraVersion;
    private final boolean quoteIdentifiers;

    public TableSchema(StructType dfSchema,
                       TableInfoProvider tableInfo,
                       WriteMode writeMode,
                       TTLOption ttlOption,
                       TimestampOption timestampOption,
                       String lowestCassandraVersion,
                       boolean quoteIdentifiers)
    {
        this.writeMode = writeMode;
        this.ttlOption = ttlOption;
        this.timestampOption = timestampOption;
        this.lowestCassandraVersion = lowestCassandraVersion;
        this.quoteIdentifiers = quoteIdentifiers;

        validateDataFrameCompatibility(dfSchema, tableInfo);
        validateNoSecondaryIndexes(tableInfo);

        this.createStatement = getCreateStatement(tableInfo);
        this.modificationStatement = getModificationStatement(dfSchema, tableInfo);
        this.partitionKeyColumns = getPartitionKeyColumnNames(tableInfo);
        this.partitionKeyColumnTypes = getPartitionKeyColumnTypes(tableInfo);
        this.converters = getConverters(dfSchema, tableInfo, ttlOption, timestampOption);
        LOGGER.info("Converters: {}", converters);
        this.keyFieldPositions = getKeyFieldPositions(dfSchema, tableInfo.getColumnNames(), getRequiredKeyColumns(tableInfo));
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

    public Object[] normalize(Object[] row)
    {
        for (int index = 0; index < row.length; index++)
        {
            row[index] = converters.get(index).convert(row[index]);
        }
        return row;
    }

    public Object[] getKeyColumns(Object[] allColumns)
    {
        return getKeyColumns(allColumns, keyFieldPositions);
    }

    @VisibleForTesting
    @NotNull
    public static Object[] getKeyColumns(Object[] allColumns, List<Integer> keyFieldPositions)
    {
        Object[] result = new Object[keyFieldPositions.size()];
        for (int keyFieldPosition = 0; keyFieldPosition < keyFieldPositions.size(); keyFieldPosition++)
        {
            Object colVal = allColumns[keyFieldPositions.get(keyFieldPosition)];
            Preconditions.checkNotNull(colVal, "Found a null primary or composite key column in source data. All key columns must be non-null.");
            result[keyFieldPosition] = colVal;
        }
        return result;
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
                                    "Missing some required key components in DataFrame => " + requiredKeyColumns
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

    static void validateNoSecondaryIndexes(TableInfoProvider tableInfo)
    {
        if (tableInfo.hasSecondaryIndex())
        {
            throw new UnsupportedAnalyticsOperationException("Bulkwriter doesn't support secondary indexes");
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
}
