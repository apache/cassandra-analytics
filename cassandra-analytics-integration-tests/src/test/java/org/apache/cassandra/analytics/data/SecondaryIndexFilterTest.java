/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test to verify that secondary index files are properly filtered out
 * from the bulk reader's listInstance method, ensuring that the Cassandra Analytics
 * library only processes regular SSTable files and ignores secondary index files.
 */
class SecondaryIndexFilterTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName TABLE_WITH_INDEXES = new QualifiedName(TEST_KEYSPACE, "test_secondary_index_filter");
    static final List<String> DATASET = Arrays.asList("alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry");

    @Test
    void testSecondaryIndexFilesAreFilteredFromBulkReader()
    {

        // Verify that secondary index files exist in the filesystem
        boolean hasIndexFiles = hasSecondaryIndexFiles();
        assertThat(hasIndexFiles).isTrue()
            .as("Secondary index files should exist in the filesystem before bulk reading");

        // Use bulk reader to read the table - this will create a snapshot and list SSTable files
        DataFrameReader readDf = bulkReaderDataFrame(TABLE_WITH_INDEXES)
                                .option("snapshotName", "secondaryIndexFilterTest")
                                .option("createSnapshot", "true");

        List<Row> rows = readDf.load().collectAsList();

        // Verify that we can successfully read all the expected data
        // If secondary index files were not properly filtered, the bulk reader might fail
        // or return incorrect results
        assertThat(rows).hasSize(DATASET.size())
            .as("Should be able to read all rows despite presence of secondary index files");

        // Verify the data content is correct
        List<String> actualNames = rows.stream()
            .map(row -> row.getString(3)) // name column (column index 3)
            .sorted()
            .collect(Collectors.toList());

        List<String> expectedNames = DATASET.stream().sorted().collect(Collectors.toList());
        assertThat(actualNames).isEqualTo(expectedNames)
            .as("Data should be read correctly when secondary index files are filtered out");

        // Additional verification: ensure index files still exist after bulk reading
        // (they should not be deleted, just ignored)
        boolean hasIndexFilesAfterRead = hasSecondaryIndexFiles();
        assertThat(hasIndexFilesAfterRead).isTrue()
            .as("Secondary index files should still exist after bulk reading (just filtered out)");
    }

    @Test
    void testBulkReaderWorksWithMultipleSecondaryIndexes()
    {
        // This test ensures that even with multiple secondary indexes creating many index files,
        // the bulk reader still works correctly by filtering them all out

        // Create additional index to generate more index files
        String createAdditionalIndex = String.format("CREATE INDEX IF NOT EXISTS age_idx ON %s (age);",
                                                     TABLE_WITH_INDEXES);
        cluster.get(1).coordinator().execute(createAdditionalIndex, ConsistencyLevel.ALL);

        // Force another flush to create more index files
        cluster.get(1).coordinator().execute(String.format("SELECT * FROM %s WHERE email = 'alice@test.com';",
                                                           TABLE_WITH_INDEXES), ConsistencyLevel.ALL);
        cluster.get(1).flush(TEST_KEYSPACE);

        // Verify multiple types of index files exist
        boolean hasIndexFiles = hasSecondaryIndexFiles();
        assertThat(hasIndexFiles).isTrue()
            .as("Secondary index files should exist");

        // Bulk read should still work correctly
        DataFrameReader readDf = bulkReaderDataFrame(TABLE_WITH_INDEXES)
                                .option("snapshotName", "multipleIndexesTest")
                                .option("createSnapshot", "true");

        List<Row> rows = readDf.load().collectAsList();

        assertThat(rows).hasSize(DATASET.size())
            .as("Should successfully read data even with multiple secondary indexes");
    }

    private void populateTableAndFlush(QualifiedName tableName)
    {
        // Insert test data
        for (int i = 0; i < DATASET.size(); i++)
        {
            String name = DATASET.get(i);
            String email = name + "@test.com";
            int age = 20 + i;
            String status = (i % 2 == 0) ? "active" : "inactive";

            String query = String.format(
                "INSERT INTO %s (id, name, email, age, status) VALUES (%d, '%s', '%s', %d, '%s');",
                tableName, i, name, email, age, status);

            cluster.get(1).coordinator().execute(query, ConsistencyLevel.ALL);
        }

        // Flush to ensure SSTables (including index files) are written to disk
        cluster.get(1).flush(TEST_KEYSPACE);

        // Execute some queries to ensure index files are created and used
        cluster.get(1).coordinator().execute(String.format("SELECT * FROM %s WHERE email = 'alice@test.com';", tableName), ConsistencyLevel.ALL);
        cluster.get(1).coordinator().execute(String.format("SELECT * FROM %s WHERE status = 'active';", tableName), ConsistencyLevel.ALL);

        // Wait a bit for index files to be fully written
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // Force another flush to ensure index SSTables are persisted
        cluster.get(1).flush(TEST_KEYSPACE);
    }

    private boolean hasSecondaryIndexFiles()
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
                    String fileName = path.getFileName().toString();
                    // Look for secondary index file patterns:
                    // - Files containing ".index." in the name
                    // - Files in ".indexes" directories
                    // - Files with index-like naming patterns
                    return fileName.contains(".") &&
                           (fileName.contains("index") ||
                            fileName.contains("idx") ||
                            path.toString().contains(".indexes/") ||
                            path.getParent().getFileName().toString().contains("index"));
                });
        }
        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        // Create table with multiple columns that will have secondary indexes
        String createTableStatement =
            "CREATE TABLE IF NOT EXISTS %s (" +
            "id int PRIMARY KEY, " +
            "name text, " +
            "email text, " +
            "age int, " +
            "status text" +
            ");";

        createTestTable(TABLE_WITH_INDEXES, createTableStatement);

        // Create secondary indexes that will generate index files
        String createEmailIndex = String.format("CREATE INDEX IF NOT EXISTS email_idx ON %s (email);", TABLE_WITH_INDEXES);
        String createStatusIndex = String.format("CREATE INDEX IF NOT EXISTS status_idx ON %s (status);", TABLE_WITH_INDEXES);

        cluster.get(1).coordinator().execute(createEmailIndex, ConsistencyLevel.ALL);
        cluster.get(1).coordinator().execute(createStatusIndex, ConsistencyLevel.ALL);

        // Insert data and flush to ensure SSTables are created (both regular and index files)
        populateTableAndFlush(TABLE_WITH_INDEXES);
    }
}
