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
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.jetbrains.annotations.NotNull;

/**
 * Broadcastable wrapper for TableSchema with ZERO transient fields to optimize Spark broadcasting.
 * <p>
 * Contains all essential fields from TableSchema needed on executors, but without the Logger reference.
 * Executors will reconstruct TableSchema from these fields.
 * <p>
 * <b>Why ZERO transient fields matters:</b><br>
 * Spark's {@link org.apache.spark.util.SizeEstimator} uses reflection to estimate object sizes before broadcasting.
 * Each transient field forces SizeEstimator to inspect the field's type hierarchy, which is expensive.
 * Logger references are particularly costly due to their deep object graphs (appenders, layouts, contexts).
 * By eliminating ALL transient fields and Logger references, we:
 * <ul>
 *   <li>Minimize SizeEstimator reflection overhead during broadcast preparation</li>
 *   <li>Reduce broadcast variable serialization size</li>
 *   <li>Avoid accidental serialization of non-serializable objects</li>
 * </ul>
 */
public final class BroadcastableTableSchema implements Serializable
{
    private static final long serialVersionUID = 2L;

    // All fields from TableSchema needed for reconstruction on executors
    private final String createStatement;
    private final String modificationStatement;
    private final List<String> partitionKeyColumns;
    private final List<ColumnType<?>> partitionKeyColumnTypes;
    private final List<SqlToCqlTypeConverter.Converter<?>> converters;
    private final List<Integer> keyFieldPositions;
    private final WriteMode writeMode;
    private final TTLOption ttlOption;
    private final TimestampOption timestampOption;
    private final CassandraVersion bridgeVersion;
    private final boolean quoteIdentifiers;
    private final Set<String> indexStatements;

    /**
     * Creates a BroadcastableTableSchema from a source TableSchema.
     * Extracts all essential fields but excludes the Logger.
     *
     * @param source the source TableSchema (driver-only)
     * @return broadcastable version without Logger
     */
    public static BroadcastableTableSchema from(@NotNull TableSchema source)
    {
        return new BroadcastableTableSchema(
            source.createStatement,
            source.modificationStatement,
            source.partitionKeyColumns,
            source.partitionKeyColumnTypes,
            source.converters,
            source.keyFieldPositions,
            source.writeMode,
            source.ttlOption,
            source.timestampOption,
            source.bridgeVersion,
            source.quoteIdentifiers,
            source.indexStatements
        );
    }

    private BroadcastableTableSchema(String createStatement,
                                     String modificationStatement,
                                     List<String> partitionKeyColumns,
                                     List<ColumnType<?>> partitionKeyColumnTypes,
                                     List<SqlToCqlTypeConverter.Converter<?>> converters,
                                     List<Integer> keyFieldPositions,
                                     WriteMode writeMode,
                                     TTLOption ttlOption,
                                     TimestampOption timestampOption,
                                     CassandraVersion bridgeVersion,
                                     boolean quoteIdentifiers,
                                     Set<String> indexStatements)
    {
        this.createStatement = createStatement;
        this.modificationStatement = modificationStatement;
        this.partitionKeyColumns = partitionKeyColumns;
        this.partitionKeyColumnTypes = partitionKeyColumnTypes;
        this.converters = converters;
        this.keyFieldPositions = keyFieldPositions;
        this.writeMode = writeMode;
        this.ttlOption = ttlOption;
        this.timestampOption = timestampOption;
        this.bridgeVersion = bridgeVersion;
        this.quoteIdentifiers = quoteIdentifiers;
        this.indexStatements = indexStatements;
    }

    public String getCreateStatement()
    {
        return createStatement;
    }

    public String getModificationStatement()
    {
        return modificationStatement;
    }

    public List<String> getPartitionKeyColumns()
    {
        return partitionKeyColumns;
    }

    public List<ColumnType<?>> getPartitionKeyColumnTypes()
    {
        return partitionKeyColumnTypes;
    }

    public List<SqlToCqlTypeConverter.Converter<?>> getConverters()
    {
        return converters;
    }

    public List<Integer> getKeyFieldPositions()
    {
        return keyFieldPositions;
    }

    public WriteMode getWriteMode()
    {
        return writeMode;
    }

    public TTLOption getTtlOption()
    {
        return ttlOption;
    }

    public TimestampOption getTimestampOption()
    {
        return timestampOption;
    }

    public CassandraVersion getBridgeVersion()
    {
        return bridgeVersion;
    }

    public boolean isQuoteIdentifiers()
    {
        return quoteIdentifiers;
    }

    public Set<String> getIndexStatements()
    {
        return indexStatements;
    }

    /**
     * Normalizes a row by applying type converters to each field.
     * This mirrors the normalize method in TableSchema but uses the broadcast-safe converters list.
     *
     * @param row the row data to normalize
     * @return the normalized row (same array instance, mutated in place)
     */
    public Object[] normalize(Object[] row)
    {
        for (int index = 0; index < row.length; index++)
        {
            row[index] = converters.get(index).convert(row[index]);
        }
        return row;
    }

    /**
     * Extracts key columns from all columns based on key field positions.
     * This mirrors the getKeyColumns method in TableSchema but uses the broadcast-safe keyFieldPositions list.
     *
     * @param allColumns all columns in the row
     * @return array containing only the key columns
     */
    public Object[] getKeyColumns(Object[] allColumns)
    {
        return getKeyColumns(allColumns, keyFieldPositions);
    }

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
}
