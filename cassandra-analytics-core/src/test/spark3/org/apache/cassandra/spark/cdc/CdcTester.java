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

package org.apache.cassandra.spark.cdc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.LocalDataSource;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Helper for writing CommitLogs using the TestSchema
 * and reading back with Spark Streaming to verify matches the expected
 */
public class CdcTester
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTester.class);
    public static final int DEFAULT_NUM_ROWS = 1000;

    // TODO: Use generic CommitLog
    public static CassandraBridge.ICommitLog COMMIT_LOG;  // CHECKSTYLE IGNORE: Constant cannot be made final

    public static void setup(CassandraBridge bridge, TemporaryFolder testFolder)
    {
        bridge.setCommitLogPath(testFolder.getRoot().toPath());
        bridge.setCDC(testFolder.getRoot().toPath());
        COMMIT_LOG = bridge.testCommitLog(testFolder.getRoot());
    }

    public static void tearDown()
    {
        if (COMMIT_LOG != null)
        {
            try
            {
                COMMIT_LOG.stop();
            }
            finally
            {
                COMMIT_LOG.clear();
            }
        }
    }

    public void reset()
    {
        LOGGER.info("Resetting CDC test environment testId={} schema='{}' thread={}",
                    testId, cqlTable.fields(), Thread.currentThread().getName());
        IOUtils.clearDirectory(outputDir, path -> LOGGER.info("Clearing test output path={}", path.toString()));
        CdcTester.tearDown();
        COMMIT_LOG.start();
    }

    final CassandraBridge bridge;
    @Nullable
    final Set<String> requiredColumns;
    final UUID testId;
    final Path testDir;
    final Path outputDir;
    final Path checkpointDir;
    final TestSchema schema;
    final CqlTable cqlTable;
    final int numRows;
    final int expectedNumRows;
    final List<CdcWriter> writers;
    int count = 0;
    final String dataSourceFQCN;
    final boolean addLastModificationTime;
    BiConsumer<Map<String, TestSchema.TestRow>, List<Row>> rowChecker;
    BiConsumer<Map<String, TestSchema.TestRow>, List<TestSchema.TestRow>> checker;

    // CHECKSTYLE IGNORE: Constructor with many parameters
    CdcTester(CassandraBridge bridge,
              TestSchema schema,
              Path testDir,
              List<CdcWriter> writers,
              int numRows,
              int expectedNumRows,
              String dataSourceFQCN,
              boolean addLastModificationTime,
              BiConsumer<Map<String, TestSchema.TestRow>, List<Row>> rowChecker,
              BiConsumer<Map<String, TestSchema.TestRow>, List<TestSchema.TestRow>> checker)
    {
        this.bridge = bridge;
        this.testId = UUID.randomUUID();
        this.testDir = testDir;
        this.writers = writers;
        this.outputDir = testDir.resolve(testId + "_out");
        this.checkpointDir = testDir.resolve(testId + "_checkpoint");
        this.requiredColumns = null;
        this.numRows = numRows;
        this.expectedNumRows = expectedNumRows;
        this.dataSourceFQCN = dataSourceFQCN;
        this.addLastModificationTime = addLastModificationTime;
        this.rowChecker = rowChecker;
        this.checker = checker;
        try
        {
            Files.createDirectory(outputDir);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }

        schema.setCassandraVersion(bridge.getVersion());
        this.schema = schema;
        this.cqlTable = schema.buildTable();
    }

    public interface CdcWriter
    {
        void write(CdcTester tester, Map<String, TestSchema.TestRow> rows, BiConsumer<TestSchema.TestRow, Long> writer);
    }

    public static class Builder
    {
        CassandraBridge bridge;
        TestSchema.Builder schemaBuilder;
        Path testDir;
        int numRows = CdcTester.DEFAULT_NUM_ROWS;
        int expecetedNumRows = numRows;
        List<CdcWriter> writers = new ArrayList<>();
        String dataSourceFQCN = LocalDataSource.class.getName();
        boolean addLastModificationTime = false;
        BiConsumer<Map<String, TestSchema.TestRow>, List<TestSchema.TestRow>> checker;
        BiConsumer<Map<String, TestSchema.TestRow>, List<Row>> rowChecker;

        Builder(CassandraBridge bridge, TestSchema.Builder schemaBuilder, Path testDir)
        {
            this.bridge = bridge;
            this.schemaBuilder = schemaBuilder;
            this.testDir = testDir;

            // Add default writer
            this.writers.add((tester, rows, writer) -> {
                long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                IntStream.range(0, tester.numRows)
                         .forEach(row -> writer.accept(Tester.newUniqueRow(tester.schema, rows), timestampMicros));
            });
        }

        Builder clearWriters()
        {
            writers.clear();
            return this;
        }

        Builder withWriter(CdcWriter writer)
        {
            writers.add(writer);
            return this;
        }

        Builder withNumRows(int numRows)
        {
            this.numRows = numRows;
            return this;
        }

        Builder withExpectedNumRows(int expectedNumRows)
        {
            this.expecetedNumRows = expectedNumRows;
            return this;
        }

        Builder withDataSource(String dataSourceFQCN)
        {
            this.dataSourceFQCN = dataSourceFQCN;
            return this;
        }

        Builder withAddLastModificationTime(boolean addLastModificationTime)
        {
            this.addLastModificationTime = addLastModificationTime;
            return this;
        }

        Builder withRowChecker(Consumer<List<Row>> rowChecker)
        {
            this.rowChecker = (idnored, rows) -> rowChecker.accept(rows);
            return this;
        }

        Builder withSparkRowTestRowsChecker(BiConsumer<Map<String, TestSchema.TestRow>, List<Row>> rowChecker)
        {
            this.rowChecker = rowChecker;
            return this;
        }

        Builder withChecker(BiConsumer<Map<String, TestSchema.TestRow>, List<TestSchema.TestRow>> checker)
        {
            this.checker = checker;
            return this;
        }

        void run()
        {
            new CdcTester(bridge,
                          schemaBuilder.build(),
                          testDir,
                          writers,
                          numRows,
                          expecetedNumRows,
                          dataSourceFQCN,
                          addLastModificationTime,
                          rowChecker,
                          checker).run();
        }
    }

    void logRow(TestSchema.TestRow row, long timestamp)
    {
        bridge.log(cqlTable, COMMIT_LOG, row, timestamp);
        count++;
    }

    void run()
    {
        Map<String, TestSchema.TestRow> rows = new LinkedHashMap<>(numRows);
        List<TestSchema.TestRow> actualRows = Collections.emptyList();
        List<Row> rowsRead = null;

        try
        {
            LOGGER.info("Running CDC test testId={} schema='{}' thread={}",
                        testId, cqlTable.fields(), Thread.currentThread().getName());
            CqlTable cqlTable = bridge.buildSchema(schema.createStatement, schema.keyspace);

            // Write some mutations to CDC CommitLog
            for (CdcWriter writer : writers)
            {
                writer.write(this, rows, (row, timestamp) -> {
                    rows.put(row.getKey(), row);
                    logRow(row, timestamp);
                });
            }
            COMMIT_LOG.sync();
            LOGGER.info("Logged mutations={} testId={}", count, testId);

            // Run streaming query and output to outputDir
            StreamingQuery query = TestUtils.openStreaming(cqlTable.keyspace(),
                                                           cqlTable.createStatement(),
                                                           bridge.getVersion(),
                                                           Partitioner.Murmur3Partitioner,
                                                           testDir.resolve("cdc"),
                                                           outputDir,
                                                           checkpointDir,
                                                           dataSourceFQCN,
                                                           addLastModificationTime);
            // Wait for query to write output parquet files before reading to verify test output matches expected
            int prevNumRows = 0;
            long timeout = System.nanoTime();
            while (actualRows.size() < expectedNumRows)
            {
                rowsRead = readRows();
                actualRows = toTestRows(rowsRead);
                timeout = prevNumRows == actualRows.size() ? timeout : System.nanoTime();
                prevNumRows = actualRows.size();
                long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - timeout);
                if (seconds > 30)
                {
                    // Timeout eventually if no progress
                    LOGGER.warn("Expected {} rows only {} found after {} seconds testId={} ",
                                expectedNumRows, prevNumRows, seconds, testId);
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }

            query.stop();
            query.awaitTermination();
        }
        catch (StreamingQueryException exception)
        {
            if (!exception.getCause().getMessage().startsWith("Job aborted"))
            {
                fail("SparkStreaming job failed with exception: " + exception.getMessage());
            }
        }
        catch (TimeoutException exception)
        {
            fail("Streaming query timed out: " + exception.getMessage());
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Unexpected error in CdcTester", ThrowableUtils.rootCause(throwable));
        }
        finally
        {
            try
            {
                // Read streaming output from outputDir and verify the rows match expected
                LOGGER.info("Finished CDC test, verifying output testId={} schema='{}' thread={} actualRows={}",
                            testId, cqlTable.fields(), Thread.currentThread().getName(), actualRows.size());

                if (rowChecker != null)
                {
                    assertNotNull(rowsRead);
                    rowChecker.accept(rows, rowsRead);
                }

                if (checker == null)
                {
                    int actualRowCount = 0;
                    for (TestSchema.TestRow actualRow : actualRows)
                    {
                        String key = actualRow.getKey();
                        TestSchema.TestRow expectedRow = rows.get(key);
                        assertNotNull(expectedRow);
                        assertEquals("Row read in Spark does not match expected",
                                     expectedRow.withColumns(requiredColumns).nullifyUnsetColumn(), actualRow);
                        actualRowCount++;
                    }
                    assertEquals(String.format("Expected %d rows, but %d read testId=%s",
                                               expectedNumRows, actualRowCount, testId), rows.size(), actualRowCount);
                }
                else
                {
                    checker.accept(rows, actualRows);
                }
            }
            finally
            {
                reset();
            }
        }
    }

    private List<Row> readRows()
    {
        return TestUtils.read(outputDir, TestSchema.toStructType(cqlTable, addLastModificationTime)).collectAsList();
    }

    private List<TestSchema.TestRow> toTestRows(List<Row> rows)
    {
        return rows.stream()
                   .map(row -> schema.toTestRow(row, requiredColumns))
                   .collect(Collectors.toList());
    }

    public static Builder builder(CassandraBridge bridge, TemporaryFolder testDir, TestSchema.Builder schemaBuilder)
    {
        return builder(bridge, testDir.getRoot().toPath(), schemaBuilder);
    }

    public static Builder builder(CassandraBridge bridge, Path testDir, TestSchema.Builder schemaBuilder)
    {
        return new Builder(bridge, schemaBuilder, testDir);
    }
}
