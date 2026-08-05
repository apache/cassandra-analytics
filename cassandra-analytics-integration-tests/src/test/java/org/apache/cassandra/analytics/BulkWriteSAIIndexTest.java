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

package org.apache.cassandra.analytics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import com.vdurmont.semver4j.Semver;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Integration test for bulk write and read operations on tables with Storage Attached Indexes (SAI).
 * Covers an all-SAI table and a mixed SAI + legacy 2i table.
 */
class BulkWriteSAIIndexTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName TABLE_SAI = new QualifiedName(TEST_KEYSPACE, "test_sai");
    // A table with a mix of a SAI index (course) and a legacy 2i index (marks).
    private static final QualifiedName TABLE_MIXED = new QualifiedName(TEST_KEYSPACE, "test_mixed_index");
    // An all-SAI table whose indexes are declared using different (but equivalent) SAI class-name spellings
    // Cassandra accepts: the 'sai' alias and the fully-qualified class name.
    private static final QualifiedName TABLE_SAI_FORMS = new QualifiedName(TEST_KEYSPACE, "test_sai_forms");
    // A table indexed exclusively with legacy 2i indexes (no SAI at all).
    private static final QualifiedName TABLE_LEGACY_ONLY = new QualifiedName(TEST_KEYSPACE, "test_legacy_only_index");

    @Test
    void testBulkWriteAndReadWithMultipleSaiIndexes()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        // 1. Bulk write to table with SAI indexes on both course and marks
        bulkWriterDataFrameWriter(dfWrite, TABLE_SAI).save();

        // 2. Flush to ensure SSTable components (including SAI) are written to disk
        cluster.getFirstRunningInstance().flush(TEST_KEYSPACE);

        assertThat(hasSaiIndexFiles())
            .as("SAI index files should exist on disk after bulk write and flush")
            .isTrue();

        Dataset<Row> dfRead = bulkReaderDataFrame(TABLE_SAI).load();
        checkSmallDataFrameEquality(dfWrite, dfRead);

        // 3. Verify SAI index filtering works on the course column
        Object[][] courseResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE course = 'course0';",
                                                                 TABLE_SAI),
                                                   ConsistencyLevel.ALL);
        assertThat(courseResults).isNotNull();
        assertThat(courseResults.length).isGreaterThan(0);
        for (Object[] row : courseResults)
        {
            // course is the second column (id, course, marks)
            assertThat(row[1]).isEqualTo("course0");
        }

        // 4. Verify SAI index filtering works on the marks column
        Object[][] marksResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE marks = 50;",
                                                                 TABLE_SAI),
                                                   ConsistencyLevel.ALL);
        assertThat(marksResults).isNotNull();
        assertThat(marksResults.length)
            .as("SAI filter on marks=50 should return the matching row")
            .isGreaterThan(0);

        for (Object[] row : marksResults)
        {
            // marks is the third column (id, course, marks)
            assertThat(row[2]).isEqualTo(50);
        }
    }

    /**
     * A table whose SAI indexes are declared with different class-name spellings Cassandra accepts and preserves in
     * the schema - the {@code 'sai'} alias and the fully-qualified {@code org.apache.cassandra.index.sai.StorageAttachedIndex}.
     * The bulk write here does NOT set SKIP_SECONDARY_INDEX_CHECK, so it succeeds only if the analytics classifier
     * recognises every one of these forms as SAI (i.e. the table is treated as all-SAI). If any form were missed, the
     * write would be rejected up-front with a "non-SAI indexes" error.
     */
    @Test
    void testBulkWriteWithSaiIndexesUsingVariedClassNameForms()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        // No SKIP_SECONDARY_INDEX_CHECK: this only saves if both the 'sai' alias and the FQCN are classified as SAI.
        bulkWriterDataFrameWriter(dfWrite, TABLE_SAI_FORMS).save();

        cluster.getFirstRunningInstance().flush(TEST_KEYSPACE);

        assertThat(hasSaiIndexFiles())
            .as("SAI index files should exist after an all-SAI write whose indexes use varied class-name spellings")
            .isTrue();

        Dataset<Row> dfRead = bulkReaderDataFrame(TABLE_SAI_FORMS).load();
        checkSmallDataFrameEquality(dfWrite, dfRead);

        // The SAI-filtered query works on the column indexed via the 'sai' alias.
        Object[][] courseResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE course = 'course0';",
                                                                 TABLE_SAI_FORMS),
                                                   ConsistencyLevel.ALL);
        assertThat(courseResults).isNotNull();
        assertThat(courseResults.length)
            .as("SAI filter on course='course0' should return matching rows")
            .isGreaterThan(0);
    }

    /**
     * A table whose indexes are all legacy 2i and contain no SAI at all must NOT be classified as an SAI write.
     * This is the end-to-end guard against the classifier over-matching: if a non-SAI index were mistaken for SAI,
     * this write would be allowed through (and would silently skip the legacy-2i handling) instead of being rejected.
     */
    @Test
    void testLegacyOnlyIndexWriteIsRejectedAsNonSai()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        assertThatThrownBy(() -> bulkWriterDataFrameWriter(dfWrite, TABLE_LEGACY_ONLY).save())
            .hasStackTraceContaining("non-SAI indexes")
            .hasStackTraceContaining(WriterOptions.SKIP_SECONDARY_INDEX_CHECK.name());
    }

    /**
     * A mixed SAI + legacy 2i table is not an all-SAI write, so without the opt-out it is rejected before any
     * SSTables are written, with a message that points the user at SKIP_SECONDARY_INDEX_CHECK.
     */
    @Test
    void testMixedIndexWriteWithoutSkipCheckIsRejected()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        assertThatThrownBy(() -> bulkWriterDataFrameWriter(dfWrite, TABLE_MIXED).save())
            .hasStackTraceContaining("non-SAI indexes")
            .hasStackTraceContaining(WriterOptions.SKIP_SECONDARY_INDEX_CHECK.name());
    }

    /**
     * With the opt-out, a mixed SAI + legacy 2i table can be bulk written: SAI components are generated inline for
     * the SAI index (so the SAI-filtered query works immediately), while the legacy 2i is rebuilt asynchronously on
     * import.
     */
    @Test
    void testMixedIndexWriteWithSkipCheckSucceeds()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(dfWrite, TABLE_MIXED)
            .option(WriterOptions.SKIP_SECONDARY_INDEX_CHECK.name(), "true")
            .save();

        cluster.getFirstRunningInstance().flush(TEST_KEYSPACE);

        // SAI components for the SAI (course) index landed on disk even though the table also has a legacy 2i.
        assertThat(hasSaiIndexFiles())
            .as("SAI index files should exist on disk after bulk write of a mixed-index table")
            .isTrue();

        // Data round-trips.
        Dataset<Row> dfRead = bulkReaderDataFrame(TABLE_MIXED).load();
        checkSmallDataFrameEquality(dfWrite, dfRead);

        // The SAI-filtered query on the SAI column works immediately: SAI components are inline, so unlike the
        // legacy 2i (rebuilt asynchronously on import) there is no rebuild window to wait for.
        Object[][] courseResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE course = 'course0';",
                                                                 TABLE_MIXED),
                                                   ConsistencyLevel.ALL);
        assertThat(courseResults).isNotNull();
        assertThat(courseResults.length)
            .as("SAI filter on course='course0' should return the matching row")
            .isGreaterThan(0);
        for (Object[] row : courseResults)
        {
            // course is the second column (id, course, marks)
            assertThat(row[1]).isEqualTo("course0");
        }
    }

    /**
     * Checks whether SAI index component files exist on the filesystem for the test keyspace.
     * SAI components are named {@code <sstable descriptor>-SAI+<version>(+<index>)+<component>.db}.
     */
    private boolean hasSaiIndexFiles()
    {
        String[] dataDirs = (String[]) cluster.get(1)
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        String dataDir = dataDirs[0];
        Path keyspacePath = Paths.get(dataDir, TEST_KEYSPACE);

        try (Stream<Path> walkStream = Files.walk(keyspacePath))
        {
            return walkStream
                .filter(Files::isRegularFile)
                .anyMatch(path -> path.getFileName().toString().contains("-SAI+"));
        }
        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion().isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("Storage Attached Index (SAI) is only available in Cassandra 5.0 and above")
        .isTrue();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(TABLE_SAI, CREATE_TEST_TABLE_STATEMENT);
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(course) USING 'StorageAttachedIndex';", TABLE_SAI));
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(marks) USING 'StorageAttachedIndex';", TABLE_SAI));

        // Mixed-index table: SAI on course + legacy 2i on marks.
        createTestTable(TABLE_MIXED, CREATE_TEST_TABLE_STATEMENT);
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(course) USING 'StorageAttachedIndex';", TABLE_MIXED));
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(marks);", TABLE_MIXED));

        // All-SAI table declared with varied (equivalent) SAI class-name spellings: the 'sai' alias on course and
        // the fully-qualified class name on marks.
        createTestTable(TABLE_SAI_FORMS, CREATE_TEST_TABLE_STATEMENT);
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(course) USING 'sai';", TABLE_SAI_FORMS));
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(marks) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                          TABLE_SAI_FORMS));

        // Legacy-2i-only table: no SAI indexes at all, so the write must be rejected as non-SAI.
        createTestTable(TABLE_LEGACY_ONLY, CREATE_TEST_TABLE_STATEMENT);
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(course);", TABLE_LEGACY_ONLY));
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(marks);", TABLE_LEGACY_ONLY));
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }
}
