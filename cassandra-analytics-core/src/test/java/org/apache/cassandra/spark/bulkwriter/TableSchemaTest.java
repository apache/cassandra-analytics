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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.exception.UnsupportedAnalyticsOperationException;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.CqlUtilsTest;
import org.apache.cassandra.spark.utils.ResourceUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableSchemaTest
{
    @TempDir
    private static Path tempPath;

    public TableSchemaTest()
    {
        Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair = TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
                new String[]{"id", "date", "course", "marks"},
                new org.apache.spark.sql.types.DataType[]{DataTypes.IntegerType, DataTypes.TimestampType, DataTypes.StringType, DataTypes.IntegerType},
                new CqlField.CqlType[]{mockCqlType(INT), mockCqlType(DATE), mockCqlType(VARCHAR), mockCqlType(INT)});
        validDataFrameSchema = validPair.getKey();
        validCqlColumns = validPair.getValue();
    }

    private StructType validDataFrameSchema;

    private ImmutableMap<String, CqlField.CqlType> validCqlColumns;

    private final String[] partitionKeyColumns = {"id"};
    private final String[] primaryKeyColumnNames = {"id", "date"};
    private final ColumnType<?>[] partitionKeyColumnTypes = {ColumnTypes.INT};

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatement(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .build();
        assertThat(trimUniqueTableName(schema.modificationStatement))
                .isEqualTo("INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks);");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatementWithConstantTTL(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion).withTTLSetting(TTLOption.from("1000")).build();
        assertThat(trimUniqueTableName(schema.modificationStatement))
                .isEqualTo("INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks) USING TTL 1000;");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatementWithTTLColumn(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion).withTTLSetting(TTLOption.from("ttl")).build();
        assertThat(trimUniqueTableName(schema.modificationStatement))
                .isEqualTo("INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks) USING TTL :ttl;");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatementWithConstantTimestamp(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion).withTimeStampSetting(TimestampOption.from("1000")).build();
        String expectedQuery = "INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks) USING TIMESTAMP 1000;";
        assertThat(trimUniqueTableName(schema.modificationStatement)).isEqualTo(expectedQuery);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatementWithTimestampColumn(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion).withTimeStampSetting(TimestampOption.from("timestamp")).build();
        String expectedQuery = "INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks) USING TIMESTAMP :timestamp;";
        assertThat(trimUniqueTableName(schema.modificationStatement)).isEqualTo(expectedQuery);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testInsertStatementWithTTLAndTimestampColumn(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                             .withTTLSetting(TTLOption.from("ttl"))
                             .withTimeStampSetting(TimestampOption.from("timestamp"))
                             .build();
        String expectedQuery = "INSERT INTO test.test (id,date,course,marks) VALUES (:id,:date,:course,:marks) USING TIMESTAMP :timestamp AND TTL :ttl;";
        assertThat(trimUniqueTableName(schema.modificationStatement)).isEqualTo(expectedQuery);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testDeleteStatement(String cassandraVersion)
    {
        Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair = TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
                new String[]{"id"},
                new org.apache.spark.sql.types.DataType[]{DataTypes.IntegerType},
                new CqlField.CqlType[]{mockCqlType(INT)});
        validDataFrameSchema = validPair.getKey();
        validCqlColumns = validPair.getValue();
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .withWriteMode(WriteMode.DELETE_PARTITION)
                .build();
        assertThat(trimUniqueTableName(schema.modificationStatement)).isEqualTo("DELETE FROM test.test where id=?;");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testDeleteWithNonPartitionKeyFieldsInDfFails(String cassandraVersion)
    {
        assertThatThrownBy(() -> getValidSchemaBuilder(cassandraVersion)
                .withWriteMode(WriteMode.DELETE_PARTITION)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only partition key columns (id) are supported in the input Dataframe when "
                            + "WRITE_MODE=DELETE_PARTITION but (id,date,course,marks) columns were provided");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testPartitionKeyColumnNames(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .build();
        assertThat(schema.partitionKeyColumns).isEqualTo(Arrays.asList("id"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testPartitionKeyColumnTypes(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .build();
        assertThat(schema.partitionKeyColumnTypes).isEqualTo(Arrays.asList(ColumnTypes.INT));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void normalizeConvertsValidTable(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion).build();
        BroadcastableTableSchema broadcastable = BroadcastableTableSchema.from(schema);

        assertThat(broadcastable.normalize(new Object[]{1, 1L, "foo", 2}))
        .isEqualTo(new Object[]{1, -2147483648, "foo", 2});
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testExtraFieldsInDataFrameFails(String cassandraVersion)
    {
        StructType extraFieldsDataFrameSchema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("date", DataTypes.TimestampType)
                .add("extra_field", DataTypes.StringType)
                .add("course", DataTypes.StringType)
                .add("marks", DataTypes.IntegerType);

        assertThatThrownBy(() -> getValidSchemaBuilder(cassandraVersion)
                .withDataFrameSchema(extraFieldsDataFrameSchema)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unknown fields");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testGetKeyColumnsFindsCorrectValues(String cassandraVersion)
    {
        StructType outOfOrderDataFrameSchema = new StructType()
                                               .add("date", DataTypes.TimestampType)
                                               .add("id", DataTypes.IntegerType)
                                               .add("course", DataTypes.StringType)
                                               .add("marks", DataTypes.IntegerType);

        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                             .withDataFrameSchema(outOfOrderDataFrameSchema)
                             .build();
        BroadcastableTableSchema broadcastable = BroadcastableTableSchema.from(schema);
        assertThat(broadcastable.getKeyColumns(new Object[]{"date_val", "id_val", "course_val", "marks_val"}))
        .isEqualTo(new Object[]{"id_val", "date_val"});
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testGetKeyColumnsFailsWhenNullKeyValues(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                             .build();
        BroadcastableTableSchema broadcastable = BroadcastableTableSchema.from(schema);
        assertThatThrownBy(() -> broadcastable.getKeyColumns(new Object[]{"foo", null, "baz", "boo"}))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Found a null primary or composite key column in source data. All key columns must be non-null.");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMissingPrimaryKeyFieldFails(String cassandraVersion)
    {
        StructType missingFieldsDataFrame = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("course", DataTypes.StringType)
                .add("marks", DataTypes.IntegerType);

        assertThatThrownBy(() -> getValidSchemaBuilder(cassandraVersion)
                .withWriteMode(WriteMode.INSERT)
                .withDataFrameSchema(missingFieldsDataFrame)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing some required key components in DataFrame => date");
    }

    @Test
    public void testSecondaryIndexIsUnsupported() throws Exception
    {
        Path fullSchemaSampleFile = ResourceUtils.writeResourceToPath(CqlUtilsTest.class.getClassLoader(), tempPath, "cql/fullSchema.cql");
        String fullSchemaSample = FileUtils.readFileToString(fullSchemaSampleFile.toFile(), StandardCharsets.UTF_8);
        Set<String> indexStatements = CqlUtils.extractIndexStatements(fullSchemaSample, "cycling", "rank_by_year_and_name");
        assertThat(indexStatements).hasSize(3);
        CqlTable table = mock(CqlTable.class);
        when(table.indexStatements()).thenReturn(indexStatements);
        TableInfoProvider tableInfoProvider = new CqlTableInfoProvider("", table);
        assertThatThrownBy(() -> TableSchema.validateNoSecondaryIndexes(tableInfoProvider))
                .isInstanceOf(UnsupportedAnalyticsOperationException.class)
                .hasMessage("Bulkwriter doesn't support secondary indexes");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testSecondaryIndexAllowedWithSkipCheck(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .withHasSecondaryIndex()
                .withSkipSecondaryIndexCheck()
                .build();
        assertThat(schema).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMixedCaseTTLColumnNameWithoutQuoteIdentifiersFails(String cassandraVersion)
    {
        assertThatThrownBy(() -> getValidSchemaBuilder(cassandraVersion)
                .withTTLSetting(TTLOption.from("updatedTTL"))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The TTL column name updatedTTL requires spark option QUOTE_IDENTIFIERS set to true for correct conversion");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMixedCaseTimestampColumnNameWithoutQuoteIdentifiersFails(String cassandraVersion)
    {
        assertThatThrownBy(() -> getValidSchemaBuilder(cassandraVersion)
                .withTimeStampSetting(TimestampOption.from("updatedTimestamp"))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The TIMESTAMP column name updatedTimestamp requires spark option QUOTE_IDENTIFIERS set to true for correct conversion");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMixedCaseTTLColumnNameWithQuoteIdentifiersSucceeds(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .withTTLSetting(TTLOption.from("updatedTTL"))
                .withQuotedIdentifiers()
                .build();
        assertThat(schema).isNotNull();
        assertThat(trimUniqueTableName(schema.modificationStatement))
                .contains("USING TTL :\"updatedTTL\"");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#supportedVersions")
    public void testMixedCaseTimestampColumnNameWithQuoteIdentifiersSucceeds(String cassandraVersion)
    {
        TableSchema schema = getValidSchemaBuilder(cassandraVersion)
                .withTimeStampSetting(TimestampOption.from("updatedTimestamp"))
                .withQuotedIdentifiers()
                .build();
        assertThat(schema).isNotNull();
        assertThat(trimUniqueTableName(schema.modificationStatement))
                .contains("USING TIMESTAMP :\"updatedTimestamp\"");
    }

    @Test
    public void testIsCassandra5OrLater()
    {
        assertThat(TableSchema.isCassandra5OrLater("5.0.5")).isTrue();
        assertThat(TableSchema.isCassandra5OrLater("cassandra-5.0.5")).isTrue();

        assertThat(TableSchema.isCassandra5OrLater("4.0.17")).isFalse();
        assertThat(TableSchema.isCassandra5OrLater("4.1.4")).isFalse();
        assertThat(TableSchema.isCassandra5OrLater("3.0.29")).isFalse();

        // Regression: 3.11.x must not be treated as 5.0+ — its version code (311) would pass a naive ">= 50" test.
        assertThat(TableSchema.isCassandra5OrLater("3.11.0")).isFalse();

        // Null / empty / unparseable versions are treated as "not 5.0+".
        assertThat(TableSchema.isCassandra5OrLater(null)).isFalse();
        assertThat(TableSchema.isCassandra5OrLater("")).isFalse();
        assertThat(TableSchema.isCassandra5OrLater("not-a-version")).isFalse();
    }

    @Test
    public void testIsSaiWrite()
    {
        Set<String> saiIndexes = ImmutableSet.of(
                "CREATE CUSTOM INDEX i1 ON test.test (course) USING 'StorageAttachedIndex';");
        Set<String> legacyIndexes = ImmutableSet.of("CREATE INDEX i1 ON test.test (course);");

        assertThat(TableSchema.isSaiWrite(saiIndexes, "5.0.5")).isTrue();
        assertThat(TableSchema.isSaiWrite(saiIndexes, "4.0.17")).isFalse();
        assertThat(TableSchema.isSaiWrite(legacyIndexes, "5.0.5")).isFalse();
        assertThat(TableSchema.isSaiWrite(Collections.emptySet(), "5.0.5")).isFalse();
    }

    @Test
    public void testValidateSecondaryIndexesNoIndexesIsNoOp()
    {
        // No exception expected when there are no index statements.
        TableSchema.validateSecondaryIndexes(false, Collections.emptySet(), "5.0.5");
    }

    @Test
    public void testValidateSecondaryIndexesAllowsSaiOnCassandra5()
    {
        Set<String> saiIndexes = ImmutableSet.of(
                "CREATE CUSTOM INDEX i1 ON test.test (course) USING 'StorageAttachedIndex';");
        // SAI on 5.0+ is allowed without the skip flag.
        TableSchema.validateSecondaryIndexes(false, saiIndexes, "5.0.5");
    }

    @Test
    public void testValidateSecondaryIndexesBlocksNonSai()
    {
        Set<String> legacyIndexes = ImmutableSet.of("CREATE INDEX i1 ON test.test (course);");
        assertThatThrownBy(() -> TableSchema.validateSecondaryIndexes(false, legacyIndexes, "5.0.5"))
                .isInstanceOf(UnsupportedAnalyticsOperationException.class)
                .hasMessage("Bulkwriter doesn't support non-SAI indexes.");
    }

    @Test
    public void testValidateSecondaryIndexesBlocksMixedSaiAndNonSai()
    {
        // A single non-SAI index makes the whole table fail the all-SAI check and be blocked.
        Set<String> mixedIndexes = ImmutableSet.of(
                "CREATE CUSTOM INDEX i1 ON test.test (course) USING 'StorageAttachedIndex';",
                "CREATE INDEX i2 ON test.test (marks);");
        assertThatThrownBy(() -> TableSchema.validateSecondaryIndexes(false, mixedIndexes, "5.0.5"))
                .isInstanceOf(UnsupportedAnalyticsOperationException.class)
                .hasMessage("Bulkwriter doesn't support non-SAI indexes.");
    }

    @Test
    public void testValidateSecondaryIndexesBlocksSaiOnPreCassandra5()
    {
        Set<String> saiIndexes = ImmutableSet.of(
                "CREATE CUSTOM INDEX i1 ON test.test (course) USING 'StorageAttachedIndex';");
        // SAI components are only generated on 5.0+, so SAI on 4.0 is blocked like any other 2i.
        assertThatThrownBy(() -> TableSchema.validateSecondaryIndexes(false, saiIndexes, "4.0.17"))
                .isInstanceOf(UnsupportedAnalyticsOperationException.class)
                .hasMessage("Bulkwriter doesn't support non-SAI indexes.");
    }

    @Test
    public void testValidateSecondaryIndexesAllowsNonSaiWithSkipCheck()
    {
        Set<String> legacyIndexes = ImmutableSet.of("CREATE INDEX i1 ON test.test (course);");
        // Explicit opt-out lets non-SAI indexes through (with an async-rebuild warning).
        TableSchema.validateSecondaryIndexes(true, legacyIndexes, "5.0.5");
    }

    private TableSchemaTestCommon.MockTableSchemaBuilder getValidSchemaBuilder(String cassandraVersion)
    {
        return new TableSchemaTestCommon.MockTableSchemaBuilder(CassandraBridgeFactory.get(cassandraVersion))
                .withCqlColumns(validCqlColumns)
                .withPartitionKeyColumns(partitionKeyColumns)
                .withPrimaryKeyColumnNames(primaryKeyColumnNames)
                .withCassandraVersion(cassandraVersion)
                .withPartitionKeyColumnTypes(partitionKeyColumnTypes)
                .withWriteMode(WriteMode.INSERT)
                .withDataFrameSchema(validDataFrameSchema);
    }

    // Removes the unique table name to make validation consistent
    private static String trimUniqueTableName(String statement)
    {
        return statement.replaceAll("test.test_table_\\d+", "test.test");
    }
}
