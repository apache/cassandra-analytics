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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.CUSTOM;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.LIST;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.MAP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.SET;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class TableSchemaTestCommon
{
    private TableSchemaTestCommon()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static Pair<StructType, ImmutableMap<String, CqlField.CqlType>> buildMatchedDataframeAndCqlColumns(
            String[] fieldNames,
            org.apache.spark.sql.types.DataType[] sparkTypes,
            CqlField.CqlType[] cqlTypes)
    {
        StructType dataFrameSchema = new StructType();
        ImmutableMap.Builder<String, CqlField.CqlType> cqlColumnsBuilder = ImmutableMap.builder();
        for (int field = 0; field < fieldNames.length; field++)
        {
            dataFrameSchema = dataFrameSchema.add(fieldNames[field], sparkTypes[field]);
            cqlColumnsBuilder.put(fieldNames[field], cqlTypes[field]);
        }

        ImmutableMap<String, CqlField.CqlType> cqlColumns = cqlColumnsBuilder.build();
        return Pair.of(dataFrameSchema, cqlColumns);
    }

    @NotNull
    public static CqlField.CqlType mockCqlType(String cqlName)
    {
        CqlField.CqlType mock = mock(CqlField.CqlType.class);
        when(mock.name()).thenReturn(cqlName);
        return mock;
    }

    @NotNull
    public static CqlField.CqlCustom mockCqlCustom(String customTypeClassName)
    {
        CqlField.CqlCustom mock = mock(CqlField.CqlCustom.class);
        when(mock.name()).thenReturn(CUSTOM);
        when(mock.customTypeClassName()).thenReturn(customTypeClassName);
        return mock;
    }

    @NotNull
    public static CqlField.CqlCollection mockSetCqlType(String collectionCqlType)
    {
        return mockCollectionCqlType(SET, mockCqlType(collectionCqlType));
    }

    @NotNull
    public static CqlField.CqlCollection mockListCqlType(String collectionCqlType)
    {
        return mockCollectionCqlType(LIST, mockCqlType(collectionCqlType));
    }

    @NotNull
    public static CqlField.CqlCollection mockCollectionCqlType(String cqlName, CqlField.CqlType collectionType)
    {
        CqlField.CqlCollection mock = mock(CqlField.CqlCollection.class);
        when(mock.name()).thenReturn(cqlName);
        when(mock.type()).thenReturn(collectionType);
        return mock;
    }

    @NotNull
    public static CqlField.CqlType mockMapCqlType(String keyCqlName, String valueCqlName)
    {
        return mockMapCqlType(mockCqlType(keyCqlName), mockCqlType(valueCqlName));
    }

    @NotNull
    public static CqlField.CqlMap mockMapCqlType(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        CqlField.CqlMap mock = mock(CqlField.CqlMap.class);
        when(mock.name()).thenReturn(MAP);
        when(mock.keyType()).thenReturn(keyType);
        when(mock.valueType()).thenReturn(valueType);
        return mock;
    }

    public static TableSchema buildSchema(String[] fieldNames,
                                          org.apache.spark.sql.types.DataType[] sparkTypes,
                                          CqlField.CqlType[] driverTypes,
                                          String[] partitionKeyColumns,
                                          ColumnType<?>[] partitionKeyColumnTypes,
                                          String[] primaryKeyColumnNames)
    {
        Pair<StructType, ImmutableMap<String, CqlField.CqlType>> pair = buildMatchedDataframeAndCqlColumns(fieldNames, sparkTypes, driverTypes);
        ImmutableMap<String, CqlField.CqlType> cqlColumns = pair.getValue();
        StructType dataFrameSchema = pair.getKey();
        String cassandraVersion = "4.0.0";
        return
            new MockTableSchemaBuilder(CassandraBridgeFactory.get(cassandraVersion))
                .withCqlColumns(cqlColumns)
                .withPartitionKeyColumns(partitionKeyColumns)
                .withPrimaryKeyColumnNames(primaryKeyColumnNames)
                .withCassandraVersion(cassandraVersion)
                .withPartitionKeyColumnTypes(partitionKeyColumnTypes)
                .withWriteMode(WriteMode.INSERT)
                .withDataFrameSchema(dataFrameSchema)
                .build();
    }

    public static class MockTableSchemaBuilder
    {
        private final CassandraBridge bridge;
        private ImmutableMap<String, CqlField.CqlType> cqlColumns;
        private String[] partitionKeyColumns;
        private String[] primaryKeyColumnNames;
        private String cassandraVersion;
        private ColumnType[] partitionKeyColumnTypes;
        private StructType dataFrameSchema;
        private WriteMode writeMode = null;
        private TTLOption ttlOption = TTLOption.forever();
        private TimestampOption timestampOption = TimestampOption.now();
        private boolean quoteIdentifiers = false;

        public MockTableSchemaBuilder(CassandraBridge bridge)
        {
            this.bridge = bridge;
        }

        public MockTableSchemaBuilder withCqlColumns(@NotNull Map<String, CqlField.CqlType> cqlColumns)
        {
            Preconditions.checkNotNull(cqlColumns, "cqlColumns cannot be null");
            Preconditions.checkArgument(!cqlColumns.isEmpty(), "cqlColumns cannot be empty");
            this.cqlColumns = ImmutableMap.copyOf(cqlColumns);
            return this;
        }

        public MockTableSchemaBuilder withPartitionKeyColumns(@NotNull String... partitionKeyColumns)
        {
            Preconditions.checkNotNull(partitionKeyColumns, "partitionKeyColumns cannot be null");
            Preconditions.checkArgument(partitionKeyColumns.length > 0, "partitionKeyColumns cannot be empty");
            this.partitionKeyColumns = partitionKeyColumns;
            return this;
        }

        public MockTableSchemaBuilder withPrimaryKeyColumnNames(@NotNull String... primaryKeyColumnNames)
        {
            Preconditions.checkNotNull(primaryKeyColumnNames, "primaryKeyColumnNames cannot be null");
            Preconditions.checkArgument(primaryKeyColumnNames.length > 0, "primaryKeyColumnNames cannot be empty");
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            return this;
        }

        public MockTableSchemaBuilder withCassandraVersion(@NotNull String cassandraVersion)
        {
            Preconditions.checkNotNull(cassandraVersion, "cassandraVersion cannot be null");
            Preconditions.checkArgument(!cassandraVersion.isEmpty(), "cassandraVersion cannot be an empty string");
            this.cassandraVersion = cassandraVersion;
            return this;
        }

        public MockTableSchemaBuilder withPartitionKeyColumnTypes(@NotNull ColumnType<?>... partitionKeyColumnTypes)
        {
            Preconditions.checkNotNull(partitionKeyColumnTypes, "partitionKeyColumnTypes cannot be null");
            Preconditions.checkArgument(partitionKeyColumnTypes.length > 0, "partitionKeyColumnTypes cannot be empty");
            this.partitionKeyColumnTypes = Arrays.copyOf(partitionKeyColumnTypes, partitionKeyColumnTypes.length);
            return this;
        }

        public MockTableSchemaBuilder withWriteMode(@NotNull WriteMode writeMode)
        {
            Preconditions.checkNotNull(writeMode, "writeMode cannot be null");
            this.writeMode = writeMode;
            return this;
        }

        public MockTableSchemaBuilder withDataFrameSchema(StructType dataFrameSchema)
        {
            Preconditions.checkNotNull(dataFrameSchema, "dataFrameSchema cannot be null");
            Preconditions.checkArgument(dataFrameSchema.nonEmpty(), "dataFrameSchema cannot be empty");
            this.dataFrameSchema = dataFrameSchema;
            return this;
        }

        public MockTableSchemaBuilder withTTLSetting(TTLOption ttlOption)
        {
            this.ttlOption = ttlOption;
            return this;
        }

        public MockTableSchemaBuilder withTimeStampSetting(TimestampOption timestampOption)
        {
            this.timestampOption = timestampOption;
            return this;
        }

        public MockTableSchemaBuilder withQuotedIdentifiers()
        {
            this.quoteIdentifiers = true;
            return this;
        }

        public TableSchema build()
        {
            Objects.requireNonNull(cqlColumns,
                                   "cqlColumns cannot be null. Please provide a list of columns by calling #withCqlColumns");
            Objects.requireNonNull(partitionKeyColumns,
                                   "partitionKeyColumns cannot be null. Please provide a list of columns by calling #withPartitionKeyColumns");
            Objects.requireNonNull(primaryKeyColumnNames,
                                   "primaryKeyColumnNames cannot be null. Please provide a list of columns by calling #withPrimaryKeyColumnNames");
            Objects.requireNonNull(cassandraVersion,
                                   "cassandraVersion cannot be null. Please provide a list of columns by calling #withCassandraVersion");
            Objects.requireNonNull(partitionKeyColumnTypes,
                                   "partitionKeyColumnTypes cannot be null. Please provide a list of columns by calling #withPartitionKeyColumnTypes");
            Objects.requireNonNull(writeMode,
                                   "writeMode cannot be null. Please provide the write mode by calling #withWriteMode");
            Objects.requireNonNull(dataFrameSchema,
                                   "dataFrameSchema cannot be null. Please provide the write mode by calling #withDataFrameSchema");
            MockTableInfoProvider tableInfoProvider = new MockTableInfoProvider(bridge,
                                                                                cqlColumns,
                                                                                partitionKeyColumns,
                                                                                partitionKeyColumnTypes,
                                                                                primaryKeyColumnNames,
                                                                                cassandraVersion,
                                                                                quoteIdentifiers);
            if (ttlOption.withTTl() && ttlOption.columnName() != null)
            {
                dataFrameSchema = dataFrameSchema.add("ttl", DataTypes.IntegerType);
            }
            if (timestampOption.withTimestamp() && timestampOption.columnName() != null)
            {
                dataFrameSchema = dataFrameSchema.add("timestamp", DataTypes.IntegerType);
            }
            return new TableSchema(dataFrameSchema, tableInfoProvider, writeMode, ttlOption, timestampOption, cassandraVersion, quoteIdentifiers);
        }
    }

    public static class MockTableInfoProvider implements TableInfoProvider
    {
        public static final String TEST_TABLE_PREFIX = "test_table_";
        public static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);
        private final CassandraBridge bridge;
        private final ImmutableMap<String, CqlField.CqlType> cqlColumns;
        private final String[] partitionKeyColumns;
        private final ColumnType[] partitionKeyColumnTypes;
        private final String[] primaryKeyColumnNames;
        private final String uniqueTableName;
        Map<String, CqlField.CqlType> columns;
        private final String cassandraVersion;
        private final boolean quoteIdentifiers;

        public MockTableInfoProvider(CassandraBridge bridge,
                                     ImmutableMap<String, CqlField.CqlType> cqlColumns,
                                     String[] partitionKeyColumns,
                                     ColumnType[] partitionKeyColumnTypes,
                                     String[] primaryKeyColumnNames,
                                     String cassandraVersion,
                                     boolean quoteIdentifiers)
        {
            this.bridge = bridge;
            this.cqlColumns = cqlColumns;
            this.partitionKeyColumns = partitionKeyColumns;
            this.partitionKeyColumnTypes = partitionKeyColumnTypes;
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            columns = cqlColumns;
            this.cassandraVersion = cassandraVersion.replaceAll("(\\w+-)*cassandra-", "");
            this.quoteIdentifiers = quoteIdentifiers;
            this.uniqueTableName = TEST_TABLE_PREFIX + TEST_TABLE_ID.getAndIncrement();
        }

        @Override
        public CqlField.CqlType getColumnType(String columnName)
        {
            return columns.get(columnName);
        }

        @Override
        public List<ColumnType<?>> getPartitionKeyTypes()
        {
            return Lists.newArrayList(partitionKeyColumnTypes);
        }

        @Override
        public boolean columnExists(String columnName)
        {
            return columns.containsKey(columnName);
        }

        @Override
        public List<String> getPartitionKeyColumnNames()
        {
            return Arrays.asList(partitionKeyColumns);
        }

        @Override
        public String getCreateStatement()
        {
            String keyDef = getKeyDef();
            String createStatement = "CREATE TABLE test." + uniqueTableName + " (" + cqlColumns.entrySet()
                                            .stream()
                                            .map(column -> maybeQuoteIdentifierIfRequested(column.getKey()) + " " + column.getValue().name())
                                            .collect(Collectors.joining(",\n")) + ", " + keyDef + ") "
                                   + "WITH COMPRESSION = {'class': '" + getCompression() + "'};";
            System.out.println("Create Table:" + createStatement);
            return createStatement;
        }

        private String getCompression()
        {
            switch (cassandraVersion.charAt(0))
            {
                case '4':
                    return "ZstdCompressor";
                case '3':
                    return "LZ4Compressor";
                default:
                    return "LZ4Compressor";
            }
        }

        private String getKeyDef()
        {
            List<String> partitionColumns = Lists.newArrayList(partitionKeyColumns);
            List<String> primaryColumns = Lists.newArrayList(primaryKeyColumnNames);
            primaryColumns.removeAll(partitionColumns);
            String partitionKey = Arrays.stream(partitionKeyColumns)
                                        .map(this::maybeQuoteIdentifierIfRequested)
                                        .collect(Collectors.joining(",", "(", ")"));
            String clusteringKey = primaryColumns.stream()
                                                 .map(this::maybeQuoteIdentifierIfRequested)
                                                 .collect(Collectors.joining(","));
            return "PRIMARY KEY (" + partitionKey + clusteringKey + ")";
        }

        @Override
        public List<String> getPrimaryKeyColumnNames()
        {
            return Arrays.asList(primaryKeyColumnNames);
        }

        @Override
        public String getName()
        {
            return uniqueTableName;
        }

        @Override
        public String getKeyspaceName()
        {
            return "test";
        }

        @Override
        public boolean hasSecondaryIndex()
        {
            return false;
        }

        @Override
        public List<String> getColumnNames()
        {
            return cqlColumns.keySet().asList();
        }

        private String maybeQuoteIdentifierIfRequested(String identifier)
        {
            return quoteIdentifiers
                   ? bridge.maybeQuoteIdentifier(identifier)
                   : identifier;
        }
    }
}
