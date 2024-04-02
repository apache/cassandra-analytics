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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.ASCII;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BIGINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BLOB;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BOOLEAN;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DOUBLE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TEXT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMESTAMP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMEUUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.UUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;

/**
 * An implementation of the {@link TableInfoProvider} interface that leverages the {@link CqlTable} to
 * provide table information
 */
public class CqlTableInfoProvider implements TableInfoProvider
{
    // CHECKSTYLE IGNORE: Long line
    private static final Pattern DEPRECATED_OPTIONS  = Pattern.compile("(?:\\s+AND)?\\s+(?:dclocal_)?read_repair_chance\\s*=\\s*[+\\-.\\dE]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern LEADING_CONJUNCTION = Pattern.compile("(?<=\\bWITH)\\s+AND\\b", Pattern.CASE_INSENSITIVE);

    private final String createTableStatement;
    private final CqlTable cqlTable;

    /**
     * Mapping from CQL Type Names to Java types
     */
    private static final Map<String, ColumnType<?>> DATA_TYPES = ImmutableMap.<String, ColumnType<?>>builder()
                                                                             .put(BIGINT, ColumnTypes.LONG)
                                                                             .put(BLOB, ColumnTypes.BYTES)
                                                                             .put(DOUBLE, ColumnTypes.DOUBLE)
                                                                             .put(INT, ColumnTypes.INT)
                                                                             .put(BOOLEAN, ColumnTypes.BOOLEAN)
                                                                             .put(TEXT, ColumnTypes.STRING)
                                                                             .put(TIMESTAMP, ColumnTypes.LONG)
                                                                             .put(UUID, ColumnTypes.UUID)
                                                                             .put(VARCHAR, ColumnTypes.STRING)
                                                                             .put(ASCII, ColumnTypes.STRING)
                                                                             .put(TIMEUUID, ColumnTypes.UUID)
                                                                             .build();

    public CqlTableInfoProvider(String createTableStatement, CqlTable cqlTable)
    {
        this.createTableStatement = createTableStatement;
        this.cqlTable = cqlTable;
    }

    @Override
    public CqlField.CqlType getColumnType(String columnName)
    {
        CqlField cqlField = cqlTable.column(columnName);
        if (cqlField == null)
        {
            throw new IllegalArgumentException("Unknown fields in data frame => " + columnName);
        }
        else
        {
            return cqlField.type();
        }
    }

    @Override
    public List<ColumnType<?>> getPartitionKeyTypes()
    {
        return cqlTable.partitionKeys().stream()
                       .map(cqlField -> DATA_TYPES.get(cqlField.type().cqlName().toLowerCase()))
                       .collect(Collectors.toList());
    }

    @Override
    public boolean columnExists(String columnName)
    {
        return cqlTable.column(columnName) != null;
    }

    @Override
    public List<String> getPartitionKeyColumnNames()
    {
        return cqlTable.partitionKeys().stream()
                       .map(CqlField::name)
                       .collect(Collectors.toList());
    }

    @Override
    public String getCreateStatement()
    {
        return removeDeprecatedOptions(createTableStatement);
    }

    @Override
    public List<String> getPrimaryKeyColumnNames()
    {
        return cqlTable.primaryKey().stream()
                       .map(CqlField::name)
                       .collect(Collectors.toList());
    }

    @Override
    public String getName()
    {
        return cqlTable.table();
    }

    @Override
    public String getKeyspaceName()
    {
        return cqlTable.keyspace();
    }

    @Override
    public boolean hasSecondaryIndex()
    {
        return cqlTable.indexCount() > 0;
    }

    @Override
    public List<String> getColumnNames()
    {
        return cqlTable.columns().stream()
                       .map(CqlField::name)
                       .collect(Collectors.toList());
    }

    /**
     * Removes table options {@code read_repair_chance} and {@code dclocal_read_repair_chance}
     * that were deprecated in Cassandra 4.0 from the table creation CQL statement
     *
     * @param cql table creation CQL statement
     * @return table creation CQL statement with deprecated table options removed
     */
    @VisibleForTesting
    @NotNull
    static String removeDeprecatedOptions(@NotNull String cql)
    {
        cql = DEPRECATED_OPTIONS.matcher(cql).replaceAll("");
        cql = LEADING_CONJUNCTION.matcher(cql).replaceAll("");
        return cql;
    }
}
