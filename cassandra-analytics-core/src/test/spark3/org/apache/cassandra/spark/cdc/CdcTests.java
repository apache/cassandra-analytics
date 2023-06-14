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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.LocalCommitLog;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.quicktheories.QuickTheory.qt;

@Disabled
public class CdcTests extends VersionRunner
{
    @TempDir
    public static Path DIRECTORY;  // CHECKSTYLE IGNORE: Constant cannot be made final

    public void setup(CassandraBridge bridge)
    {
        CdcTester.setup(bridge, DIRECTORY);
    }

    @AfterEach
    public void tearDown()
    {
        CdcTester.tearDown();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSinglePartitionKey(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", type))
                                  .withRowChecker(sparkRows -> {
                                      for (Row row : sparkRows)
                                      {
                                          byte[] updatedFieldsIndicator = (byte[]) row.get(3);
                                          BitSet actual = BitSet.valueOf(updatedFieldsIndicator);
                                          BitSet expected = new BitSet(3);
                                          expected.set(0, 3);  // Expecting all columns to be set
                                          Assertions.assertEquals(expected, actual);
                                      }
                                  })
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUpdatedFieldsIndicator(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", type))
                                  .clearWriters()
                                  .withAddLastModificationTime(true)
                                  .withWriter((tester, rows, writer) -> {
                                      for (int row = 0; row < tester.numRows; row++)
                                      {
                                          TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                                          testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER);  // Mark c1 as not updated / unset
                                          writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                      }
                                  })
                                  .withRowChecker(sparkRows -> {
                                      for (Row row : sparkRows)
                                      {
                                          byte[] updatedFieldsIndicator = (byte[]) row.get(4);
                                          BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                          BitSet expected = new BitSet(3);
                                          expected.set(0);  // Expecting pk to be set
                                          expected.set(2);  // And c2 to be set
                                          Assertions.assertEquals(expected, bs);
                                          Object actual = row.get(1);
                                          Assertions.assertNull(actual, "c1 should be null");
                                      }
                                  })
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testMultipleWritesToSameKeyInBatch(CassandraBridge bridge)
    {
        setup(bridge);
        // The test writes different groups of mutations.
        // Each group of mutations write to the same key with a different timestamp.
        // For CDC, it only deduplicates and emits the replicated mutations, i.e. they have the same writetime.
        qt()
        .withUnlimitedExamples()
        .withTestingTime(5, TimeUnit.MINUTES)
        .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", type))
                                  .clearWriters()
                                  .withAddLastModificationTime(true)
                                  .withWriter((tester, rows, writer) -> {
                                      // Write initial values
                                      long timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                                      for (int row = 0; row < tester.numRows; row++)
                                      {
                                          writer.accept(Tester.newUniqueRow(tester.schema, rows), timestamp++);
                                      }

                                      // Overwrite with new mutations at later timestamp
                                      for (TestSchema.TestRow row : rows.values())
                                      {
                                          TestSchema.TestRow newUniqueRow = Tester.newUniqueRow(tester.schema, rows);
                                          for (CqlField field : tester.cqlTable.valueColumns())
                                          {
                                              // Update value columns
                                              row = row.copy(field.position(), newUniqueRow.get(field.position()));
                                          }
                                          writer.accept(row, timestamp++);
                                      }
                                  })
                                  .withChecker((testRows, actualRows) -> {
                                      int partitions = testRows.size();
                                      int mutations = actualRows.size();
                                      Assertions.assertEquals(partitions * 2, (Object) mutations, "Each PK should get 2 mutations");
                                  })
                                  .withRowChecker(sparkRows -> {
                                      long timestamp = -1L;
                                      for (Row row : sparkRows)
                                      {
                                          if (timestamp < 0)
                                          {
                                              timestamp = getMicros(row.getTimestamp(3));
                                          }
                                          else
                                          {
                                              long lastTimestamp = timestamp;
                                              timestamp = getMicros(row.getTimestamp(3));
                                              Assertions.assertTrue(lastTimestamp < timestamp, "Writetime should be monotonically increasing");
                                          }
                                      }
                                  })
                                  .run());
    }

    private long getMicros(java.sql.Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        int nanos = timestamp.getNanos();
        return TimeUnit.MILLISECONDS.toMicros(millis) + TimeUnit.NANOSECONDS.toMicros(nanos);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCompactOnlyWithEnoughReplicas(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", type))
                                  .withDataSource(RequireTwoReplicasLocalDataSource.class.getName())
                                  .withNumRows(1000)
                                  .withExpectedNumRows(999)  // Expect 1 less row
                                  .withAddLastModificationTime(true)
                                  .clearWriters()
                                  .withWriter((tester, rows, writer) -> {
                                      // Write initial values
                                      long timestamp = System.currentTimeMillis();
                                      Map<Long, TestSchema.TestRow> genRows = new HashMap<>();
                                      IntStream.range(0, tester.numRows)
                                               .forEach(row -> genRows.put(timestamp + row, Tester.newUniqueRow(tester.schema, rows)));
                                      genRows.forEach((key, value) -> writer.accept(value, TimeUnit.MILLISECONDS.toMicros(key)));

                                      // Write the same values again, with the first value skipped.
                                      // All values except the first one have 2 copies.
                                      // The test is using RequireTwoReplicasCompactionDataSource,
                                      // so the output should not contain the first value.
                                      for (long row = 1; row < tester.numRows; row++)
                                      {
                                          writer.accept(genRows.get(timestamp + row), TimeUnit.MILLISECONDS.toMicros(timestamp + row));
                                      }
                                  })
                                  .withRowChecker(rows -> {
                                      int size = rows.size();
                                      // The timestamp column is added at column 4
                                      int uniqueTsCount = rows.stream().map(r -> r.getTimestamp(3).getTime())
                                                              .collect(Collectors.toSet())
                                                              .size();
                                      Assertions.assertEquals(size, uniqueTsCount, "Output rows should have distinct lastModified timestamps");
                                  })
                                  .withChecker((testRows, actualRows) -> {
                                      Assertions.assertEquals(actualRows.size() + 1, testRows.size(),
                                                              "There should be exact one row less in the output.");
                                      boolean allContains = true;
                                      TestSchema.TestRow unexpectedRow = null;
                                      for (TestSchema.TestRow row : actualRows)
                                      {
                                          if (!testRows.containsValue(row))
                                          {
                                              allContains = false;
                                              unexpectedRow = row;
                                              break;
                                          }
                                      }
                                      if (!allContains && unexpectedRow != null)
                                      {
                                          Assertions.fail("Found an unexpected row from the output: " + unexpectedRow);
                                      }
                                  })
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCompositePartitionKey(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk1", bridge.uuid())
                                                                        .withPartitionKey("pk2", type)
                                                                        .withPartitionKey("pk3", bridge.timestamp())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testClusteringKey(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withPartitionKey("ck", type)
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testMultipleClusteringKeys(CassandraBridge bridge)
    {
        setup(bridge);
        qt().withExamples(50)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2, type3) ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withClusteringKey("ck1", type1)
                                                                        .withClusteringKey("ck2", type2)
                                                                        .withClusteringKey("ck3", type3)
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSet(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.set(type)))
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testList(CassandraBridge bridge)
    {
        setup(bridge);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.list(type)))
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testMap(CassandraBridge bridge)
    {
        setup(bridge);
        // TODO
        qt().withExamples(1)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.bigint())
                                                                        .withColumn("c2", bridge.map(type1, type2)))
                                  .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUpdateFlag(CassandraBridge bridge)
    {
        setup(bridge);
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIRECTORY, TestSchema.builder()
                                                                        .withPartitionKey("pk", bridge.uuid())
                                                                        .withColumn("c1", bridge.aInt())
                                                                        .withColumn("c2", type))
                                  .clearWriters()
                                  .withNumRows(1000)
                                  .withWriter((tester, rows, writer) -> {
                                      int halfway = tester.numRows / 2;
                                      for (int row = 0; row < tester.numRows; row++)
                                      {
                                          TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                                          testRow = testRow.copy("c1", row);
                                          if (row >= halfway)
                                          {
                                              testRow.fromUpdate();
                                          }
                                          writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                      }
                                  })
                                  .withRowChecker(sparkRows -> {
                                      int length = sparkRows.size();
                                      int halfway = length / 2;
                                      for (Row row : sparkRows)
                                      {
                                          int index = row.getInt(1);
                                          boolean isUpdate = row.getBoolean(4);
                                          Assertions.assertEquals(isUpdate, index >= halfway);
                                      }
                                  })
                                  .run());
    }

    // CommitLog Reader

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testReaderWatermarking(CassandraBridge bridge) throws IOException
    {
        setup(bridge);
        TestSchema schema = TestSchema.builder()
                                      .withPartitionKey("pk", bridge.bigint())
                                      .withColumn("c1", bridge.bigint())
                                      .withColumn("c2", bridge.bigint())
                                      .build();
        CqlTable cqlTable = bridge.buildSchema(schema.createStatement, schema.keyspace);
        int numRows = 1000;

        // Write some rows to a CommitLog
        Set<Long> keys = new HashSet<>(numRows);
        for (int index = 0; index < numRows; index++)
        {
            TestSchema.TestRow row = schema.randomRow();
            while (keys.contains(row.getLong("pk")))
            {
                row = schema.randomRow();
            }
            keys.add(row.getLong("pk"));
            bridge.log(cqlTable, CdcTester.COMMIT_LOG, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        CdcTester.COMMIT_LOG.sync();

        AtomicReference<CommitLog.Marker> currentMarker = new AtomicReference<>();
        List<CommitLog.Marker> markers = Collections.synchronizedList(new ArrayList<>());
        Watermarker watermarker = createWatermarker(currentMarker, markers);
        File logFile = Files.list(DIRECTORY.resolve("cdc"))
                            .max((first, second) -> {
                                try
                                {
                                    return Long.compare(Files.size(first), Files.size(second));
                                }
                                catch (IOException exception)
                                {
                                    throw new RuntimeException(exception);
                                }
                            })
                            .orElseThrow(() -> new RuntimeException("No log files found"))
                            .toFile();

        // Read entire CommitLog and verify correct
        Set<Long> allRows = readLog(cqlTable, logFile, watermarker, keys, bridge);
        Assertions.assertEquals(numRows, (Object) allRows.size());

        // Re-read CommitLog from each watermark position and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        List<CommitLog.Marker> allMarkers = new ArrayList<>(markers);
        CommitLog.Marker prevMarker = null;
        for (CommitLog.Marker marker : allMarkers)
        {
            currentMarker.set(marker);
            Set<Long> result = readLog(cqlTable, logFile, watermarker, keys, bridge);
            Assertions.assertTrue(result.size() < foundRows);
            foundRows = result.size();
            if (prevMarker != null)
            {
                Assertions.assertTrue(prevMarker.compareTo(marker) < 0);
                boolean actual = prevMarker.position() < marker.position();
                Assertions.assertTrue(actual);
            }
            prevMarker = marker;

            if (marker.equals(allMarkers.get(allMarkers.size() - 1)))
            {
                // Last marker should return 0 updates and be at the end of the file
                Assertions.assertTrue(result.isEmpty());
            }
            else
            {
                assertFalse(result.isEmpty());
            }
        }
    }

    private Watermarker createWatermarker(AtomicReference<CommitLog.Marker> current, List<CommitLog.Marker> all)
    {
        return new Watermarker()
        {
            @Override
            public Watermarker instance(String jobId)
            {
                return this;
            }

            @Override
            public void recordReplicaCount(IPartitionUpdateWrapper update, int numReplicas)
            {
            }

            @Override
            public int replicaCount(IPartitionUpdateWrapper update)
            {
                return 0;
            }

            @Override
            public void untrackReplicaCount(IPartitionUpdateWrapper update)
            {
            }

            @Override
            public boolean seenBefore(IPartitionUpdateWrapper update)
            {
                return false;
            }

            @Override
            public void updateHighWaterMark(CommitLog.Marker marker)
            {
                all.add(marker);
            }

            @Override
            @Nullable
            public CommitLog.Marker highWaterMark(CassandraInstance instance)
            {
                CommitLog.Marker marker = current.get();
                return marker != null ? marker : instance.zeroMarker();
            }

            @Override
            public void persist(@Nullable Long maxAgeMicros)
            {
            }

            @Override
            public void clear()
            {
                all.clear();
            }
        };
    }

    private Set<Long> readLog(CqlTable table, File logFile, Watermarker watermarker, Set<Long> keys, CassandraBridge bridge)
    {
        try (LocalCommitLog log = new LocalCommitLog(logFile))
        {
            Set<Long> result = bridge.readLog(table, log, watermarker);
            result.forEach(key -> Assertions.assertTrue(keys.contains(key), "Unexpected keys have been read from the commit log"));
            return result;
        }
        catch (Exception exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
