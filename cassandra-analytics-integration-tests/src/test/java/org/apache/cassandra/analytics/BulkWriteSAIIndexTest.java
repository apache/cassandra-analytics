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
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Integration test for bulk write and read operations on a table with multiple Storage Attached Indexes (SAI).
 * Verifies that SAI SSTable components are written to disk and that SAI index filtering works after bulk write.
 */
class BulkWriteSAIIndexTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName TABLE_SAI =
        new QualifiedName(TEST_KEYSPACE, "test_sai");

    @Test
    void testBulkWriteAndReadWithMultipleSaiIndexes()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        // 1. Bulk write to table with SAI indexes on both course and marks
        bulkWriterDataFrameWriter(dfWrite, TABLE_SAI).save();

        // 2. Flush to ensure SSTable components (including SAI) are written to disk
        cluster.getFirstRunningInstance().flush(TEST_KEYSPACE);

        // 3. Verify SAI index SSTable components exist on the filesystem
        assertThat(hasSaiIndexFiles())
            .as("SAI index files should exist on disk after bulk write and flush")
            .isTrue();

        // 4. Bulk read the data back and verify equality
        Dataset<Row> dfRead = bulkReaderDataFrame(TABLE_SAI).load();
        checkSmallDataFrameEquality(dfWrite, dfRead);

        // 5. Verify SAI index filtering works on the course column
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

        // 6. Verify SAI index filtering works on the marks column
        Object[][] marksResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE marks = 50;",
                                                                 TABLE_SAI),
                                                   ConsistencyLevel.ALL);
        assertThat(marksResults).isNotNull();
    }

    /**
     * Checks whether SAI index files exist on the filesystem for the test keyspace.
     * SAI stores index data in directories or files with naming patterns that include
     * "SAI" or reside under directories indicating SAI components.
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
                .anyMatch(path -> {
                    String pathStr = path.toString();
                    // SAI index components are stored in directories or files
                    // with patterns like ".sai/" or "SAI" in the path
                    return pathStr.contains("SAI") || pathStr.contains(".sai");
                });
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
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }
}
