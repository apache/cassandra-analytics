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

package org.apache.cassandra.cdc;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeImplementation;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.cdc.CdcTests.CDC_BRIDGE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class CdcTester
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTester.class);
    public static final int DEFAULT_NUM_ROWS = 1000;

    public static FourZeroCommitLog testCommitLog;

    public static void setup(Path testDirectory)
    {
        setup(testDirectory, 32, false);
    }

    public static void setup(Path testDirectory, int commitLogSegmentSize, boolean enableCompression)
    {
        CdcBridgeImplementation.setup(testDirectory, commitLogSegmentSize, enableCompression);
        testCommitLog = new FourZeroCommitLog(testDirectory);
    }

    public static void tearDown()
    {
        try
        {
            testCommitLog.stop();
        }
        finally
        {
            testCommitLog.clear();
        }
    }

    public void reset()
    {
        LOGGER.info("Resetting CDC test environment testId={} schema='{}' testDir={} thread={}",
                    testId, cqlTable.fields(), testDir, Thread.currentThread().getName());
        CdcTester.tearDown();
        IOUtils.clearDirectory(testDir, path -> LOGGER.info("Clearing test output path={}", path.toString()));
        testCommitLog.start();
    }

    final CassandraBridge bridge;
    @Nullable
    final Set<String> requiredColumns;
    final UUID testId;
    final Path testDir;
    public final TestSchema schema;
    public final CqlTable cqlTable;
    public final int numRows;
    final int expectedNumRows;
    final List<CdcWriter> writers;
    int count = 0;
    final boolean addLastModificationTime;
    BiConsumer<Map<String, TestSchema.TestRow>, List<CdcEvent>> eventsChecker;
    final boolean shouldCdcEventWriterFailOnProcessing;
    Partitioner partitioner = Partitioner.Murmur3Partitioner;
    CdcOptions cdcOptions;
    CassandraSource cassandraSource;

    CdcTester(CassandraBridge bridge,
              TestSchema schema,
              Path testDir,
              List<CdcWriter> writers,
              int numRows,
              int expectedNumRows,
              boolean addLastModificationTime,
              BiConsumer<Map<String, TestSchema.TestRow>, List<CdcEvent>> eventsChecker,
              boolean shouldCdcEventWriterFailOnProcessing,
              CdcOptions cdcOptions,
              CassandraSource cassandraSource)
    {
        this.bridge = bridge;
        this.testId = UUID.randomUUID();
        this.testDir = testDir;
        this.writers = writers;
        this.requiredColumns = null;
        this.numRows = numRows;
        this.expectedNumRows = expectedNumRows;
        this.addLastModificationTime = addLastModificationTime;
        this.eventsChecker = eventsChecker;
        this.shouldCdcEventWriterFailOnProcessing = shouldCdcEventWriterFailOnProcessing;
        this.schema = schema;
        this.cqlTable = schema.buildTable();
        this.cdcOptions = cdcOptions;
        this.cassandraSource = cassandraSource;
    }

    public static Builder builder(CassandraBridge bridge, TestSchema.Builder schemaBuilder, Path testDir)
    {
        return new Builder(bridge, schemaBuilder, testDir);
    }

    public static class Builder
    {
        CassandraBridge bridge;
        TestSchema.Builder schemaBuilder;
        Path testDir;
        int numRows = CdcTester.DEFAULT_NUM_ROWS;
        int expectedNumRows = numRows;
        List<CdcWriter> writers = new ArrayList<>();
        boolean addLastModificationTime = false;
        BiConsumer<Map<String, TestSchema.TestRow>, List<CdcEvent>> eventChecker;
        private boolean shouldCdcEventWriterFailOnProcessing = false;
        private CdcOptions cdcOptions = CdcTests.TEST_OPTIONS;
        private CassandraSource cassandraSource = CassandraSource.DEFAULT;

        Builder(CassandraBridge bridge, TestSchema.Builder schemaBuilder, Path testDir)
        {
            this.bridge = bridge;
            this.schemaBuilder = schemaBuilder.withCdc(true);
            this.testDir = testDir;

            // add default writer
            this.writers.add((tester, rows, writer) -> {
                final long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                IntStream.range(0, tester.numRows)
                         .forEach(i -> writer.accept(newUniqueRow(tester.schema, rows), timestampMicros));
            });
        }

        Builder clearWriters()
        {
            this.writers.clear();
            return this;
        }

        Builder withWriter(CdcWriter writer)
        {
            this.writers.add(writer);
            return this;
        }

        Builder withNumRows(int numRows)
        {
            this.numRows = numRows;
            return this;
        }

        Builder withExpectedNumRows(int expectedNumRows)
        {
            this.expectedNumRows = expectedNumRows;
            return this;
        }

        Builder withAddLastModificationTime(boolean addLastModificationTime)
        {
            this.addLastModificationTime = addLastModificationTime;
            return this;
        }

        Builder withCdcEventChecker(BiConsumer<Map<String, TestSchema.TestRow>, List<CdcEvent>> checker)
        {
            this.eventChecker = checker;
            return this;
        }

        Builder withStatsClass(String statsClass)
        {
            return this;
        }

        Builder shouldCdcEventWriterFailOnProcessing()
        {
            this.shouldCdcEventWriterFailOnProcessing = true;
            return this;
        }

        Builder withCdcOptions(CdcOptions cdcOptions)
        {
            this.cdcOptions = cdcOptions;
            return this;
        }

        Builder withCassandraSource(CassandraSource cassandraSource)
        {
            this.cassandraSource = cassandraSource;
            return this;
        }

        public CdcTester build()
        {
            return new CdcTester(bridge, schemaBuilder.build(), testDir, writers, numRows, expectedNumRows,
                                 addLastModificationTime, eventChecker, shouldCdcEventWriterFailOnProcessing,
                                 cdcOptions, cassandraSource);
        }

        void run()
        {
            build().run();
        }
    }

    public void logRow(CqlTable schema, TestSchema.TestRow row, long timestamp)
    {
        CDC_BRIDGE.log(TimeProvider.DEFAULT, schema, testCommitLog, row, timestamp);
        count++;
    }

    public void sync()
    {
        testCommitLog.sync();
    }

    void run()
    {
        Map<String, TestSchema.TestRow> rows = new LinkedHashMap<>(numRows);
        CassandraVersion version = CassandraVersion.FOURZERO;

        List<CdcEvent> cdcEvents = new ArrayList<>();
        try
        {
            LOGGER.info("Running CDC test testId={} schema='{}' thread={}", testId, cqlTable.fields(), Thread.currentThread().getName());
            Set<String> udtStmts = schema.udts.stream().map(e -> e.createStatement(bridge.cassandraTypes(), schema.keyspace)).collect(Collectors.toSet());
            bridge.buildSchema(schema.createStatement, schema.keyspace, schema.rf, partitioner, udtStmts, null, 0, true);
            schema.setCassandraVersion(version);

            // write some mutations to CDC CommitLog
            for (CdcWriter writer : writers)
            {
                writer.write(this, rows, (row, timestamp) -> {
                    rows.put(row.getPrimaryHexKey(), row);
                    this.logRow(writer.cqlTable(this), row, timestamp);
                });
            }
            sync();
            LOGGER.info("Logged mutations={} testId={}", count, testId);

            long start = System.currentTimeMillis();
            int prevNumRows;
            CdcState state = CdcState.BLANK;
            while (cdcEvents.size() < expectedNumRows)
            {
                try (MicroBatchIterator it = new MicroBatchIterator(CDC_BRIDGE,
                                                                    state,
                                                                    cassandraSource,
                                                                    () -> ImmutableSet.of(schema.keyspace),
                                                                    cdcOptions,
                                                                    CdcTests.ASYNC_EXECUTOR,
                                                                    CdcTests.logProvider(testDir)))
                {
                    while (it.hasNext())
                    {
                        CdcEvent event = it.next();
                        cdcEvents.add(event);
                    }
                    state = it.endState();
                }

                prevNumRows = cdcEvents.size();
                if (cdcEvents.size() == expectedNumRows || maybeTimeout(start, expectedNumRows, prevNumRows, testId.toString()))
                {
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        }
        catch (Throwable t)
        {
            LOGGER.error("Unexpected error in CdcTester", ThrowableUtils.rootCause(t));
            t.printStackTrace();
            fail("Unexpected error in CdcTester");
        }
        finally
        {
            try
            {
                // read streaming output from outputDir and verify the rows match expected
                LOGGER.info("Finished CDC test, verifying output testId={} schema='{}' thread={} actualRows={}",
                            testId, cqlTable.fields(), Thread.currentThread().getName(), cdcEvents.size());

                if (eventsChecker != null)
                {
                    assertNotNull(cdcEvents);
                    eventsChecker.accept(rows, cdcEvents);
                }
            }
            finally
            {
                reset();
            }
        }
    }

    public static boolean maybeTimeout(long startMillis,
                                       int expectedNumRows,
                                       int prevNumRows,
                                       String testId)
    {
        long elapsedSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMillis);
        if (elapsedSecs > 20)
        {
            // timeout eventually if no progress
            LOGGER.warn("Expected {} rows only {} found after {} seconds testId={} ",
                        expectedNumRows, prevNumRows, elapsedSecs, testId);
            return true;
        }
        return false;
    }

    public static Builder testWith(CassandraBridge bridge, Path testDir, TestSchema.Builder schemaBuilder)
    {
        return new Builder(bridge, schemaBuilder, testDir);
    }

    public static TestSchema.TestRow newUniqueRow(TestSchema schema, Map<String, TestSchema.TestRow> rows)
    {
        return newUniqueRow(schema::randomRow, rows);
    }

    public static TestSchema.TestRow newUniquePartitionDeletion(TestSchema schema, Map<String, TestSchema.TestRow> rows)
    {
        return newUniqueRow(schema::randomPartitionDelete, rows);
    }

    private static TestSchema.TestRow newUniqueRow(Supplier<TestSchema.TestRow> rowProvider,
                                                   Map<String, TestSchema.TestRow> rows)
    {
        TestSchema.TestRow testRow;
        do
        {
            testRow = rowProvider.get();
        }
        while (rows.containsKey(testRow.getPrimaryHexKey()));  // Don't write duplicate rows
        return testRow;
    }

    // tl;dr; text and varchar cql types are the same internally in Cassandra
    // TEXT is UTF8 encoded string, as same as varchar. Both are represented as UTF8Type internally.
    private static final Set<String> SAME_TYPE = ImmutableSet.of("text", "varchar");

    public static void assertCqlTypeEquals(String expectedType, String testType)
    {
        if (!expectedType.equals(testType))
        {
            if (!SAME_TYPE.contains(testType) || !SAME_TYPE.contains(expectedType))
            {
                fail(String.format("Expected type: %s; test type: %s", expectedType, testType));
            }
        }
    }
}
