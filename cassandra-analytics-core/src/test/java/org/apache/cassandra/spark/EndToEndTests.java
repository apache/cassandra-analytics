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

package org.apache.cassandra.spark;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import scala.collection.mutable.AbstractSeq;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.mutableSeqAsJavaList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;

/**
 * End-to-end tests that write random data to multiple SSTables,
 * reads the data back into Spark and verifies the rows in Spark match the expected.
 * Uses QuickTheories to test many combinations of field data types and clustering key sort order.
 * Uses custom SSTableTombstoneWriter to write SSTables with tombstones
 * to verify Spark Bulk Reader correctly purges tombstoned data.
 */

public class EndToEndTests
{

    /* Partition Key Tests */
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSinglePartitionKey(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.bigint())
                                 .withColumn("c2", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("c1")
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testOnlyPartitionKeys(CassandraBridge bridge)
    {
        // Special case where schema is only partition keys
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withPartitionKey("b", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testOnlyPartitionClusteringKeys(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.bigint())
                                 .withClusteringKey("c", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultiplePartitionKeys(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withPartitionKey("b", bridge.bigint())
                                 .withColumn("c", bridge.text())
                                 .withColumn("d", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("d")
              .run();
    }

    /* Clustering Key Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBasicSingleClusteringKey(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.bigint())
                                 .withClusteringKey("b", bridge.bigint())
                                 .withColumn("c", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("c")
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSingleClusteringKeyOrderBy(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.sortOrder())
            .checkAssert((clusteringKeyType, sortOrder) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", bridge.bigint())
                                         .withClusteringKey("b", clusteringKeyType)
                                         .withColumn("c", bridge.bigint())
                                         .withSortOrder(sortOrder))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleClusteringKeys(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withClusteringKey("c", bridge.text())
                                 .withColumn("d", bridge.text())
                                 .withColumn("e", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("e")
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testManyClusteringKeys(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.timestamp())
                                 .withClusteringKey("c", bridge.text())
                                 .withClusteringKey("d", bridge.uuid())
                                 .withClusteringKey("e", bridge.aFloat())
                                 .withColumn("f", bridge.text())
                                 .withColumn("g", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("g")
              .run();
    }

    /* Data Type Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testAllDataTypesPartitionKey(CassandraBridge bridge)
    {
        // Test partition key can be read for all data types
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(partitionKeyType -> {
                // Boolean or empty types have limited cardinality
                int numRows = partitionKeyType.cardinality(10);
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", partitionKeyType)
                                         .withColumn("b", bridge.bigint()))
                      .withNumRandomSSTables(1)
                      .withNumRandomRows(numRows)
                      .withExpectedRowCountPerSSTable(numRows)
                      .run();
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testAllDataTypesValueColumn(CassandraBridge bridge)
    {
        // Test value column can be read for all data types
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(valueType ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", bridge.bigint())
                                         .withColumn("b", valueType))
                      .withNumRandomSSTables(1)
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    /* Compaction */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleSSTablesCompaction(CassandraBridge bridge)
    {
        AtomicLong startTotal = new AtomicLong(0);
        AtomicLong newTotal = new AtomicLong(0);
        Map<UUID, Long> column1 = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        Map<UUID, String> column2 = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.bigint())
                                 .withColumn("c2", bridge.text()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < Tester.DEFAULT_NUM_ROWS; row++)
                  {
                      UUID pk = UUID.randomUUID();
                      long c1 = RandomUtils.RANDOM.nextInt(10_000_000);
                      String c2 = UUID.randomUUID().toString();
                      startTotal.addAndGet(c1);
                      column1.put(pk, c1);
                      column2.put(pk, c2);
                      writer.write(pk, c1, c2);
                  }
              })
              // Overwrite c1 with new value greater than previous
              .withSSTableWriter(writer -> {
                  for (UUID pk : column1.keySet())
                  {
                      long newBalance = (long) RandomUtils.RANDOM.nextInt(10_000_000) + column1.get(pk);
                      assertTrue(newBalance > column1.get(pk));
                      newTotal.addAndGet(newBalance);
                      column1.put(pk, newBalance);
                      writer.write(pk, newBalance, column2.get(pk));
                  }
              })
              .withCheck(dataset -> {
                  assertTrue(startTotal.get() < newTotal.get());
                  long sum = 0;
                  int count = 0;
                  for (Row row : dataset.collectAsList())
                  {
                      UUID pk = UUID.fromString(row.getString(0));
                      assertEquals(row.getLong(1), column1.get(pk).longValue());
                      assertEquals(row.getString(2), column2.get(pk));
                      sum += (long) row.get(1);
                      count++;
                  }
                  assertEquals(Tester.DEFAULT_NUM_ROWS, count);
                  assertEquals(newTotal.get(), sum);
              })
              .withReset(() -> {
                  startTotal.set(0);
                  newTotal.set(0);
                  column1.clear();
                  column2.clear();
              });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testCompaction(CassandraBridge bridge)
    {
        int numRowsColumns = 20;
        AtomicInteger total = new AtomicInteger(0);
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withColumn("c", bridge.aInt()))
              // Don't write random data
              .dontWriteRandomData()
              // Write some SSTables deterministically
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRowsColumns; row++)
                  {
                      for (int column = 0; column < numRowsColumns; column++)
                      {
                          writer.write(row, column, 0);
                      }
                  }
              })
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRowsColumns; row++)
                  {
                      for (int column = 0; column < numRowsColumns; column++)
                      {
                          writer.write(row, column, 1);
                      }
                  }
              })
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRowsColumns; row++)
                  {
                      for (int column = 0; column < numRowsColumns; column++)
                      {
                          int num = column * 500;
                          total.addAndGet(num);
                          writer.write(row, column, num);
                      }
                  }
              })
              .withReadListener(row -> {
                  // We should have compacted the SSTables to remove duplicate data and tombstones
                  assert row.getInteger("b") * 500 == row.getInteger("c");
              })
              // Verify sums to correct total
              .withCheck(dataset -> assertEquals(total.get(), dataset.groupBy().sum("c").first().getLong(0)))
              .withCheck(dataset -> assertEquals(numRowsColumns * numRowsColumns,
                                                 dataset.groupBy().count().first().getLong(0)))
              .withReset(() -> total.set(0))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSingleClusteringKey(CassandraBridge bridge)
    {
        AtomicLong total = new AtomicLong(0);
        Map<Integer, MutableLong> testSum = new HashMap<>();
        Set<Integer> clusteringKeys = ImmutableSet.of(0, 1, 2, 3);
        for (int clusteringKey : clusteringKeys)
        {
            testSum.put(clusteringKey, new MutableLong(0));
        }

        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withColumn("c", bridge.bigint())
                                 .withColumn("d", bridge.text()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < Tester.DEFAULT_NUM_ROWS; row++)
                  {
                      for (int clusteringKey : clusteringKeys)
                      {
                          UUID accountId = UUID.randomUUID();
                          long balance = RandomUtils.RANDOM.nextInt(10_000_000);
                          total.addAndGet(balance);
                          String name = UUID.randomUUID().toString().substring(0, 8);
                          testSum.get(clusteringKey).add(balance);
                          writer.write(accountId, clusteringKey, balance, name);
                      }
                  }
              })
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size())
              .withCheck(dataset -> {
                  assertEquals(total.get(), testSum.values().stream().mapToLong(MutableLong::getValue).sum());
                  long sum = 0;
                  int count = 0;
                  for (Row row : dataset.collectAsList())
                  {
                      assertNotNull(row.getString(0));
                      long balance = row.getLong(2);
                      assertNotNull(row.getString(3));
                      sum += balance;
                      count++;
                  }
                  assertEquals(total.get(), sum);
                  assertEquals(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size(), count);
              })
              .withCheck(dataset -> {
                  // Test basic group by matches expected
                  for (Row row : dataset.groupBy("b").sum("c").collectAsList())
                  {
                      assertEquals(testSum.get(row.getInt(0)).getValue().longValue(), row.getLong(1));
                  }
              })
              .withReset(() -> {
                  total.set(0);
                  for (int clusteringKey : clusteringKeys)
                  {
                      testSum.put(clusteringKey, new MutableLong(0));
                  }
              })
              .run();
    }

    /* Static Columns */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testOnlyStaticColumn(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.bigint())
                                 .withStaticColumn("c", bridge.aInt()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    @SuppressWarnings("UnstableApiUsage")  // Use of Guava Uninterruptibles
    public void testStaticColumn(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 20;
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withStaticColumn("c", bridge.aInt())
                                 .withColumn("d", bridge.text()))
              // Don't write random data
              .dontWriteRandomData()
              // Write some SSTables deterministically
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      for (int column = 0; column < numColumns; column++)
                      {
                          // We need to sleep here to prevent timestamp conflicts confusing the static column value
                          if (column == numColumns - 1)
                          {
                              Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
                          }
                          writer.write(row, column, row * column, UUID.randomUUID().toString());
                      }
                  }
              })
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      for (int column = numColumns; column < numColumns * 2; column++)
                      {
                          // We need to sleep here to prevent timestamp conflicts confusing the static column value
                          if (column == numColumns * 2 - 1)
                          {
                              Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
                          }

                          writer.write(row, column, row * column, UUID.randomUUID().toString());
                      }
                  }
              })
              .withReadListener(row -> {
                  // Static column should be the last value written
                  assert row.getInteger("c") == row.getInteger("a") * (numColumns * 2 - 1);
              })
              // Verify row count is correct
              .withCheck(dataset -> assertEquals(numRows * numColumns * 2, dataset.count()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNulledStaticColumns(CassandraBridge bridge)
    {
        int numClusteringKeys = 10;
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withStaticColumn("c", bridge.text())
                                 .withColumn("d", bridge.aInt()))
              .withNumRandomRows(0)
              .dontCheckNumSSTables()
              .withSSTableWriter(writer ->
                  IntStream.range(0, Tester.DEFAULT_NUM_ROWS)
                           .forEach(row -> {
                               UUID pk = UUID.randomUUID();
                               IntStream.range(0, numClusteringKeys)
                                        .forEach(clusteringKey ->
                                                writer.write(pk, clusteringKey, row % 2 == 0 ? null : "Non-null", row));
                           })
              )
              .withReadListener(row -> {
                  String staticCol = row.isNull("c") ? null : row.getString("c");
                  if (row.getInteger("d") % 2 == 0)
                  {
                      assertNull(staticCol);
                  }
                  else
                  {
                      assertEquals("Non-null", staticCol);
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#versions")
    public void testMultipleSSTableCompacted(CassandraVersion version)
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(version);
        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("a", bridge.uuid())
                                                     .withClusteringKey("b", bridge.aInt())
                                                     .withClusteringKey("c", bridge.text())
                                                     .withColumn("d", bridge.text())
                                                     .withColumn("e", bridge.bigint());
        AtomicLong total = new AtomicLong(0);
        Map<UUID, TestSchema.TestRow> rows = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        Tester.builder(schemaBuilder)
              // Don't write random data
              .dontWriteRandomData()
              // Write some SSTables with random data
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < Tester.DEFAULT_NUM_ROWS; row++)
                  {
                      TestSchema schema = schemaBuilder.build();
                      schema.setCassandraVersion(version);
                      TestSchema.TestRow testRow = schema.randomRow();
                      rows.put(testRow.getUUID("a"), testRow);
                      writer.write(testRow.allValues());
                  }
              })
              // Overwrite rows/cells multiple times in different SSTables
              // and ensure compaction compacts together correctly
              .withSSTableWriter(writer -> {
                  for (TestSchema.TestRow testRow : ImmutableSet.copyOf(rows.values()))
                  {
                      // Update rows with new values
                      TestSchema.TestRow newTestRow = testRow.copy("e", RandomUtils.RANDOM.nextLong())
                                                             .copy("d", UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      writer.write(newTestRow.allValues());
                  }
              })
              .withSSTableWriter(writer -> {
                  for (TestSchema.TestRow testRow : ImmutableSet.copyOf(rows.values()))
                  {
                      // Update rows with new values - this should be the final values seen by Spark
                      TestSchema.TestRow newTestRow = testRow.copy("e", RandomUtils.RANDOM.nextLong())
                                                             .copy("d", UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      total.addAndGet(newTestRow.getLong("e"));
                      writer.write(newTestRow.allValues());
                  }
              })
              // Verify rows returned by Spark match expected
              .withReadListener(actualRow -> assertTrue(rows.containsKey(actualRow.getUUID("a"))))
              .withReadListener(actualRow -> assertEquals(rows.get(actualRow.getUUID("a")), actualRow))
              .withReadListener(actualRow -> assertEquals(rows.get(actualRow.getUUID("a")).getLong("e"),
                                                                   actualRow.getLong("e")))
              // Verify Spark aggregations match expected
              .withCheck(dataset -> assertEquals(total.get(), dataset.groupBy().sum("e").first().getLong(0)))
              .withCheck(dataset -> assertEquals(rows.size(), dataset.groupBy().count().first().getLong(0)))
              .withReset(() -> {
                  total.set(0);
                  rows.clear();
              })
              .run();
    }

    /* Tombstone Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testPartitionTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numRows - 2))
            .checkAssert(deleteRangeStart -> {
                assert 0 <= deleteRangeStart && deleteRangeStart < numRows;
                int deleteRangeEnd = deleteRangeStart + RandomUtils.RANDOM.nextInt(numRows - deleteRangeStart - 1) + 1;
                assert deleteRangeStart < deleteRangeEnd && deleteRangeEnd < numRows;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a ="))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = deleteRangeStart; row < deleteRangeEnd; row++)
                          {
                              writer.write(row);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              int value = row.getInt(0);
                              assertTrue(0 <= value && value < numRows);
                              assertTrue(value < deleteRangeStart || value >= deleteRangeEnd);
                              count++;
                          }
                          assertEquals((numRows - (deleteRangeEnd - deleteRangeStart)) * numColumns, count);
                      })
                      .run();
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRowTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numColumns - 1))
            .checkAssert(colNum ->
                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a =", "b ="))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, colNum);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              int value = row.getInt(0);
                              assertTrue(row.getInt(0) >= 0 && value < numRows);
                              assertTrue(row.getInt(1) != colNum);
                              assertEquals(row.get(1), row.get(2));
                              count++;
                          }
                          assertEquals(numRows * (numColumns - 1), count);
                      })
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(integers().between(0, numColumns - 1))
            .checkAssert(startBound -> {
                assertTrue(startBound < numColumns);
                int endBound = startBound + RandomUtils.RANDOM.nextInt(numColumns - startBound);
                assertTrue(endBound >= startBound && endBound <= numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound, endBound);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              int value = row.getInt(1);
                              assertEquals(value, row.getInt(2));
                              assertTrue(value <= numColumns);
                              assertTrue(value < startBound || value >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numColumns - numTombstones), count);
                      })
                      .run();
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneString(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(characters().ascii())
            .checkAssert(startBound -> {
                assertTrue(startBound <= numColumns);
                char endBound = (char) (startBound + RandomUtils.RANDOM.nextInt(numColumns - startBound));
                assertTrue(endBound >= startBound && endBound <= numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", bridge.aInt())
                                         .withClusteringKey("b", bridge.text())
                                         .withColumn("c", bridge.aInt())
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  String value = String.valueOf((char) column);
                                  writer.write(row, value, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound.toString(), Character.toString(endBound));
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              char character = row.getString(1).charAt(0);
                              assertTrue(character <= numColumns);
                              assertTrue(character < startBound || character >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numColumns - numTombstones), count);
                      })
                      .run();
            });
    }

    /* Partial Rows: test reading rows with missing columns */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testPartialRow(CassandraBridge bridge)
    {
        Map<UUID, UUID> rows = new HashMap<>();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.uuid())
                                 .withColumn("d", bridge.aInt())
                                 .withColumn("e", bridge.uuid())
                                 .withColumn("f", bridge.aInt())
                                 .withInsertFields("a", "c", "e"))  // Override insert statement to only insert some columns
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < Tester.DEFAULT_NUM_ROWS; row++)
                  {
                      UUID key = UUID.randomUUID();
                      UUID value = UUID.randomUUID();
                      rows.put(key, value);
                      writer.write(key, value, value);
                  }
              })
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      assertEquals(6, row.size());
                      UUID key = UUID.fromString(row.getString(0));
                      UUID value1 = UUID.fromString(row.getString(2));
                      UUID value2 = UUID.fromString(row.getString(4));
                      assertTrue(rows.containsKey(key));
                      assertEquals(rows.get(key), value1);
                      assertEquals(value2, value1);
                      assertNull(row.get(1));
                      assertNull(row.get(3));
                      assertNull(row.get(5));
                  }
              })
              .withReset(rows::clear)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testPartialRowClusteringKeys(CassandraBridge bridge)
    {
        Map<String, String> rows = new HashMap<>();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.uuid())
                                 .withClusteringKey("c", bridge.uuid())
                                 .withColumn("d", bridge.text())
                                 .withColumn("e", bridge.uuid())
                                 .withColumn("f", bridge.aInt())
                                 .withColumn("g", bridge.uuid())
                                 .withColumn("h", bridge.aInt())
                                 .withInsertFields("a", "b", "c", "e", "g"))  // Override insert statement to only insert some columns
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < Tester.DEFAULT_NUM_ROWS; row++)
                  {
                      UUID a = UUID.randomUUID();
                      UUID b = UUID.randomUUID();
                      UUID c = UUID.randomUUID();
                      UUID e = UUID.randomUUID();
                      UUID g = UUID.randomUUID();
                      String key = a + ":" + b + ":" + c;
                      String value = e + ":" + g;
                      rows.put(key, value);
                      writer.write(a, b, c, e, g);
                  }
              })
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      assertEquals(8, row.size());
                      String a = row.getString(0);
                      String b = row.getString(1);
                      String c = row.getString(2);
                      String e = row.getString(4);
                      String g = row.getString(6);
                      String key = a + ":" + b + ":" + c;
                      String value = e + ":" + g;
                      assertTrue(rows.containsKey(key));
                      assertEquals(rows.get(key), value);
                      assertNull(row.get(3));
                      assertNull(row.get(5));
                      assertNull(row.get(7));
                  }
              })
              .withReset(rows::clear)
              .run();
    }

    /* Collections */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSet(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.set(type)))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testList(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.list(type)))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMap(CassandraBridge bridge)
    {
        qt().withExamples(50)  // Limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((keyType, valueType) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.map(keyType, valueType)))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testClusteringKeySet(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("id", bridge.aInt())
                                 .withColumn("a", bridge.set(bridge.text())))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    /* Frozen Collections */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenSet(CassandraBridge bridge)
    {
        // pk -> a frozen<set<?>>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.set(type).frozen()))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenList(CassandraBridge bridge)
    {
        // pk -> a frozen<list<?>>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.list(type).frozen()))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenMap(CassandraBridge bridge)
    {
        // pk -> a frozen<map<?, ?>>
        qt().withExamples(50)  // Limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((keyType, valueType) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.map(keyType, valueType).frozen()))
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapSet(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<set<text>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(), bridge.set(bridge.text()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapList(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<list<text>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(), bridge.list(bridge.text()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapMap(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<map<bigint, varchar>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(),
                                                  bridge.map(bridge.bigint(), bridge.varchar()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenNestedMapMap(CassandraBridge bridge)
    {
        // pk -> a frozen<map<text, <map<int, timestamp>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(),
                                                             bridge.map(bridge.aInt(), bridge.timestamp())).frozen()))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run();
    }

    /* Filters */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSinglePartitionKeyFilter(CassandraBridge bridge)
    {
        int numRows = 10;
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withColumn("b", bridge.aInt()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      writer.write(row, row + 1);
                  }
              })
              .withFilter("a=1")
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      int a = row.getInt(0);
                      assertEquals(1, a);
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultiplePartitionKeyFilter(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 5;
        Set<String> keys = TestUtils.getKeys(ImmutableList.of(ImmutableList.of("2", "3"),
                                                              ImmutableList.of("2", "3", "4")));
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withPartitionKey("b", bridge.aInt())
                                 .withColumn("c", bridge.aInt()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      for (int column = 0; column < numColumns; column++)
                      {
                          writer.write(row, (row + 1), column);
                      }
                  }
              })
              .withFilter("a in (2, 3) and b in (2, 3, 4)")
              .withCheck(dataset -> {
                  List<Row> rows = dataset.collectAsList();
                  assertEquals(2, rows.size());
                  for (Row row : rows)
                  {
                      int a = row.getInt(0);
                      int b = row.getInt(1);
                      String key = a + ":" + b;
                      assertTrue(keys.contains(key));
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFiltersDoNotMatch(CassandraBridge bridge)
    {
        int numRows = 10;
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withColumn("b", bridge.aInt()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      writer.write(row, row + 1);
                  }
              })
              .withFilter("a=11")
              .withCheck(dataset -> assertTrue(dataset.collectAsList().isEmpty()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFilterWithClusteringKey(CassandraBridge bridge)
    {
        int numRows = 10;
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.aInt())
                                 .withClusteringKey("b", bridge.text())
                                 .withClusteringKey("c", bridge.timestamp()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      writer.write(200, row < 3 ? "abc" : "def", new java.util.Date(10_000L * (row + 1)));
                  }
              })
              .withFilter("a=200 and b='def'")
              .withCheck(dataset -> {
                  List<Row> rows = dataset.collectAsList();
                  assertFalse(rows.isEmpty());
                  assertEquals(7, rows.size());
                  for (Row row : rows)
                  {
                      assertEquals(200, row.getInt(0));
                      assertEquals("def", row.getString(1));
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtNativeTypes(CassandraBridge bridge)
    {
        // pk -> a testudt<b text, c type, d int>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                              .withPartitionKey("pk", bridge.uuid())
                              .withColumn("a", bridge.udt("keyspace", "testudt")
                                                     .withField("b", bridge.text())
                                                     .withField("c", type)
                                                     .withField("d", bridge.aInt())
                                                     .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerSet(CassandraBridge bridge)
    {
        // pk -> a testudt<b text, c frozen<type>, d int>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                .withField("b", bridge.text())
                                                                .withField("c", bridge.set(type).frozen())
                                                                .withField("d", bridge.aInt())
                                                                .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerList(CassandraBridge bridge)
    {
        // pk -> a testudt<b bigint, c frozen<list<type>>, d boolean>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                .withField("b", bridge.bigint())
                                                                .withField("c", bridge.list(type).frozen())
                                                                .withField("d", bridge.bool())
                                                                .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerMap(CassandraBridge bridge)
    {
        // pk -> a testudt<b float, c frozen<set<uuid>>, d frozen<map<type1, type2>>, e boolean>
        qt().withExamples(50)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                .withField("b", bridge.aFloat())
                                                                .withField("c", bridge.set(bridge.uuid()).frozen())
                                                                .withField("d", bridge.map(type1, type2).frozen())
                                                                .withField("e", bridge.bool())
                                                                .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleUdts(CassandraBridge bridge)
    {
        // pk -> col1 udt1<a float, b frozen<set<uuid>>, c frozen<set<type>>, d boolean>,
        //       col2 udt2<a text, b bigint, g varchar>, col3 udt3<int, type, ascii>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("col1", bridge.udt("keyspace", "udt1")
                                                                   .withField("a", bridge.aFloat())
                                                                   .withField("b", bridge.set(bridge.uuid()).frozen())
                                                                   .withField("c", bridge.set(type).frozen())
                                                                   .withField("d", bridge.bool())
                                                                   .build())
                                         .withColumn("col2", bridge.udt("keyspace", "udt2")
                                                                   .withField("a", bridge.text())
                                                                   .withField("b", bridge.bigint())
                                                                   .withField("g", bridge.varchar())
                                                                   .build())
                                         .withColumn("col3", bridge.udt("keyspace", "udt3")
                                                                   .withField("a", bridge.aInt())
                                                                   .withField("b", bridge.list(type).frozen())
                                                                   .withField("c", bridge.ascii())
                                                                   .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedUdt(CassandraBridge bridge)
    {
        // pk -> a test_udt<b float, c frozen<set<uuid>>, d frozen<nested_udt<x int, y type, z int>>, e boolean>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.udt("keyspace", "test_udt")
                                                                .withField("b", bridge.aFloat())
                                                                .withField("c", bridge.set(bridge.uuid()).frozen())
                                                                .withField("d", bridge.udt("keyspace", "nested_udt")
                                                                                      .withField("x", bridge.aInt())
                                                                                      .withField("y", type)
                                                                                      .withField("z", bridge.aInt())
                                                                                      .build().frozen())
                                                                .withField("e", bridge.bool())
                                                                .build()))
                      .run()
            );
    }

    /* Tuples */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBasicTuple(CassandraBridge bridge)
    {
        // pk -> a tuple<int, type1, bigint, type2>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.tuple(bridge.aInt(), type1, bridge.bigint(), type2)))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleWithClusteringKey(CassandraBridge bridge)
    {
        // pk -> col1 type1 -> a tuple<int, type2, bigint>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withClusteringKey("col1", type1)
                                         .withColumn("a", bridge.tuple(bridge.aInt(), type2, bridge.bigint())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedTuples(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, type1, float, varchar, tuple<bigint, boolean, type2>>, timeuuid>
        // Test tuples nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                       bridge.tuple(bridge.aInt(),
                                                                                    type1,
                                                                                    bridge.aFloat(),
                                                                                    bridge.varchar(),
                                                                                    bridge.tuple(bridge.bigint(),
                                                                                                 bridge.bool(),
                                                                                                 type2)),
                                                                       bridge.timeuuid())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleSet(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, set<type>>, timeuuid>
        // Test set nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                       bridge.tuple(bridge.aInt(),
                                                                                    bridge.varchar(),
                                                                                    bridge.aFloat(),
                                                                                    bridge.varchar(),
                                                                                    bridge.set(type)),
                                                                       bridge.timeuuid())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleList(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, list<type>>, timeuuid>
        // Test list nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                       bridge.tuple(bridge.aInt(),
                                                                                    bridge.varchar(),
                                                                                    bridge.aFloat(),
                                                                                    bridge.varchar(),
                                                                                    bridge.list(type)),
                                                                       bridge.timeuuid())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleMap(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, map<type1, type2>>, timeuuid>
        // Test map nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                Tester.builder(TestSchema.builder(bridge)
                                        .withPartitionKey("pk", bridge.uuid())
                                        .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                      bridge.tuple(bridge.aInt(),
                                                                                   bridge.varchar(),
                                                                                   bridge.aFloat(),
                                                                                   bridge.varchar(),
                                                                                   bridge.map(type1, type2)),
                                                                                   bridge.timeuuid())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMapTuple(CassandraBridge bridge)
    {
        // pk -> a map<timeuuid, frozen<tuple<boolean, type, timestamp>>>
        // Test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.map(bridge.timeuuid(),
                                                                     bridge.tuple(bridge.bool(),
                                                                                  type,
                                                                                  bridge.timestamp()).frozen())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSetTuple(CassandraBridge bridge)
    {
        // pk -> a set<frozen<tuple<type, float, text>>>
        // Test tuple nested within set
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.set(bridge.tuple(type,
                                                                                  bridge.aFloat(),
                                                                                  bridge.text()).frozen())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testListTuple(CassandraBridge bridge)
    {
        // pk -> a list<frozen<tuple<int, inet, decimal, type>>>
        // Test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.list(bridge.tuple(bridge.aInt(),
                                                                                   bridge.inet(),
                                                                                   bridge.decimal(),
                                                                                   type).frozen())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleUDT(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, frozen<nested_udt<x int, y type, z int>>, timeuuid>
        // Test tuple with inner UDT
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                       bridge.udt("keyspace", "nested_udt")
                                                                             .withField("x", bridge.aInt())
                                                                             .withField("y", type)
                                                                             .withField("z", bridge.aInt())
                                                                             .build().frozen(),
                                                                       bridge.timeuuid())))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUDTTuple(CassandraBridge bridge)
    {
        // pk -> a nested_udt<x text, y tuple<int, float, type, timestamp>, z ascii>
        // Test UDT with inner tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("a", bridge.udt("keyspace", "nested_udt")
                                                                .withField("x", bridge.text())
                                                                .withField("y", bridge.tuple(bridge.aInt(),
                                                                                             bridge.aFloat(),
                                                                                             type,
                                                                                             bridge.timestamp()))
                                                                .withField("z", bridge.ascii())
                                                                .build()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleClusteringKey(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withClusteringKey("ck", bridge.tuple(bridge.aInt(),
                                                                               bridge.text(),
                                                                               type,
                                                                               bridge.aFloat()))
                                         .withColumn("a", bridge.text())
                                         .withColumn("b", bridge.aInt())
                                         .withColumn("c", bridge.ascii()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtClusteringKey(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withClusteringKey("ck", bridge.udt("keyspace", "udt1")
                                                                        .withField("a", bridge.text())
                                                                        .withField("b", type)
                                                                        .withField("c", bridge.aInt())
                                                                        .build().frozen())
                                         .withColumn("a", bridge.text())
                                         .withColumn("b", bridge.aInt())
                                         .withColumn("c", bridge.ascii()))
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testComplexSchema(CassandraBridge bridge)
    {
        String keyspace = "complex_schema2";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("time", bridge.bigint())
                                     .withField("time_offset_minutes", bridge.aInt())
                                     .build();
        CqlField.CqlUdt udt2 = bridge.udt(keyspace, "udt2")
                                     .withField("version", bridge.text())
                                     .withField("id", bridge.text())
                                     .withField("platform", bridge.text())
                                     .withField("time_range", bridge.text())
                                     .build();
        CqlField.CqlUdt udt3 = bridge.udt(keyspace, "udt3")
                                     .withField("field", bridge.text())
                                     .withField("time_with_zone", udt1)
                                     .build();
        CqlField.CqlUdt udt4 = bridge.udt(keyspace, "udt4")
                                     .withField("first_seen", udt3.frozen())
                                     .withField("last_seen", udt3.frozen())
                                     .withField("first_transaction", udt3.frozen())
                                     .withField("last_transaction", udt3.frozen())
                                     .withField("first_listening", udt3.frozen())
                                     .withField("last_listening", udt3.frozen())
                                     .withField("first_reading", udt3.frozen())
                                     .withField("last_reading", udt3.frozen())
                                     .withField("output_event", bridge.text())
                                     .withField("event_history", bridge.map(bridge.bigint(),
                                                                            bridge.map(bridge.text(),
                                                                                       bridge.bool()).frozen()
                                     ).frozen())
                                     .build();

        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("\"consumerId\"", bridge.text())
                                              .withClusteringKey("dimensions", udt2.frozen())
                                              .withColumn("fields", udt4.frozen())
                                              .withColumn("first_transition_time", udt1.frozen())
                                              .withColumn("last_transition_time", udt1.frozen())
                                              .withColumn("prev_state_id", bridge.text())
                                              .withColumn("state_id", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedFrozenUDT(CassandraBridge bridge)
    {
        // "(a bigint PRIMARY KEY, b <map<int, frozen<testudt>>>)"
        // testudt(a text, b bigint, c int)
        CqlField.CqlUdt testudt = bridge.udt("nested_frozen_udt", "testudt")
                                        .withField("a", bridge.text())
                                        .withField("b", bridge.bigint())
                                        .withField("c", bridge.aInt())
                                        .build();
        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("a", bridge.bigint())
                                             .withColumn("b", bridge.map(bridge.aInt(), testudt.frozen())))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testDeepNestedUDT(CassandraBridge bridge)
    {
        String keyspace = "deep_nested_frozen_udt";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.aInt())
                                     .withField("c", bridge.bigint())
                                     .build();
        CqlField.CqlUdt udt2 = bridge.udt(keyspace, "udt2")
                                     .withField("a", bridge.aInt())
                                     .withField("b", bridge.set(bridge.uuid()))
                                     .build();
        CqlField.CqlUdt udt3 = bridge.udt(keyspace, "udt3")
                                     .withField("a", bridge.aInt())
                                     .withField("b", bridge.set(bridge.uuid()))
                                     .build();
        CqlField.CqlUdt udt4 = bridge.udt(keyspace, "udt4")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.uuid())
                                     .withField("d", bridge.list(bridge.tuple(bridge.text(),
                                                                              bridge.bigint()).frozen()
                                                                             ).frozen())
                                     .build();
        CqlField.CqlUdt udt5 = bridge.udt(keyspace, "udt5")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.bigint())
                                     .withField("d", bridge.set(udt4.frozen()))
                                     .build();
        CqlField.CqlUdt udt6 = bridge.udt(keyspace, "udt6")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.aInt())
                                     .withField("d", bridge.aInt())
                                     .build();
        CqlField.CqlUdt udt7 = bridge.udt(keyspace, "udt7")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.uuid())
                                     .withField("c", bridge.bool())
                                     .withField("d", bridge.bool())
                                     .withField("e", bridge.bool())
                                     .withField("f", bridge.bigint())
                                     .withField("g", bridge.bigint())
                                     .build();

        CqlField.CqlUdt udt8 = bridge.udt(keyspace, "udt8")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.bool())
                                     .withField("c", bridge.bool())
                                     .withField("d", bridge.bool())
                                     .withField("e", bridge.bigint())
                                     .withField("f", bridge.bigint())
                                     .withField("g", bridge.uuid())
                                     .withField("h", bridge.bigint())
                                     .withField("i", bridge.uuid())
                                     .withField("j", bridge.uuid())
                                     .withField("k", bridge.uuid())
                                     .withField("l", bridge.uuid())
                                     .withField("m", bridge.aInt())
                                     .withField("n", bridge.timestamp())
                                     .withField("o", bridge.text())
                                     .build();

        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withClusteringKey("ck", bridge.uuid())
                                              .withColumn("a", udt3.frozen())
                                              .withColumn("b", udt2.frozen())
                                              .withColumn("c", bridge.set(bridge.tuple(udt1,
                                                                                       bridge.text()).frozen()))
                                              .withColumn("d", bridge.set(bridge.tuple(bridge.bigint(),
                                                                                       bridge.text()).frozen()))
                                              .withColumn("e", bridge.set(bridge.tuple(udt2,
                                                                                       bridge.text()).frozen()))
                                              .withColumn("f", bridge.set(udt7.frozen()))
                                              .withColumn("g", bridge.map(bridge.aInt(),
                                                                          bridge.set(bridge.text()).frozen()))
                                              .withColumn("h", bridge.set(bridge.tinyint()))
                                              .withColumn("i", bridge.map(bridge.text(),
                                                                          udt6.frozen()))
                                              .withColumn("j", bridge.map(bridge.text(),
                                                                          bridge.map(bridge.text(),
                                                                                     bridge.text()).frozen()))
                                              .withColumn("k", bridge.list(bridge.tuple(bridge.text(),
                                                                                        bridge.text(),
                                                                                        bridge.text()).frozen()))
                                              .withColumn("l", bridge.list(udt5.frozen()))
                                              .withColumn("m", udt8.frozen())
                                              .withMinCollectionSize(4))
              .withNumRandomRows(50)
              .withNumRandomSSTables(2)
              .run();
    }

    /* BigDecimal/Integer Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBigDecimal(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.decimal())
                                 .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBigInteger(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.varint())
                                 .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtFieldOrdering(CassandraBridge bridge)
    {
        String keyspace = "udt_field_ordering";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("c", bridge.text())
                                     .withField("b", bridge.uuid())
                                     .withField("a", bridge.bool())
                                     .build();
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("a", bridge.set(udt1.frozen())))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    @SuppressWarnings("unchecked")
    public void testUdtTupleInnerNulls(CassandraBridge bridge)
    {
        CqlField.CqlUdt udtType = bridge.udt("udt_inner_nulls", "udt")
                                        .withField("a", bridge.uuid())
                                        .withField("b", bridge.text())
                                        .build();
        CqlField.CqlTuple tupleType = bridge.tuple(bridge.bigint(), bridge.text(), bridge.aInt());

        int numRows = 50;
        int midPoint = numRows / 2;
        Map<UUID, Set<Map<String, Object>>> udtSetValues = new LinkedHashMap<>(numRows);
        Map<UUID, Object[]> tupleValues = new LinkedHashMap<>(numRows);
        for (int tupleIndex = 0; tupleIndex < numRows; tupleIndex++)
        {
            UUID pk = UUID.randomUUID();
            Set<Map<String, Object>> udtSet = new HashSet<>(numRows);
            for (int udtIndex = 0; udtIndex < numRows; udtIndex++)
            {
                Map<String, Object> udt = Maps.newHashMapWithExpectedSize(2);
                udt.put("a", UUID.randomUUID());
                udt.put("b", udtIndex < midPoint ? UUID.randomUUID().toString() : null);
                udtSet.add(udt);
            }
            Object[] tuple = new Object[]{RandomUtils.RANDOM.nextLong(),
                                          tupleIndex < midPoint ? UUID.randomUUID().toString() : null,
                                          RandomUtils.RANDOM.nextInt()};

            udtSetValues.put(pk, udtSet);
            tupleValues.put(pk, tuple);
        }

        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("pk", bridge.uuid())
                                             .withColumn("a", bridge.set(udtType.frozen()))
                                             .withColumn("b", tupleType))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (UUID pk : udtSetValues.keySet())
                  {
                      Set<Object> udtSet = udtSetValues.get(pk).stream()
                                                               .map(map ->  bridge.toUserTypeValue(udtType, map))
                                                               .collect(Collectors.toSet());
                      Object tuple = bridge.toTupleValue(tupleType, tupleValues.get(pk));

                      writer.write(pk, udtSet, tuple);
                  }
              })
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      UUID pk = UUID.fromString(row.getString(0));

                      Set<Map<String, Object>> expectedUdtSet = udtSetValues.get(pk);
                      List<Row> udtSet = mutableSeqAsJavaList((AbstractSeq<Row>) row.get(1));
                      assertEquals(expectedUdtSet.size(), udtSet.size());
                      for (Row udt : udtSet)
                      {
                          Map<String, Object> expectedUdt = Maps.newHashMapWithExpectedSize(2);
                          expectedUdt.put("a", UUID.fromString(udt.getString(0)));
                          expectedUdt.put("b", udt.getString(1));
                          assertTrue(expectedUdtSet.contains(expectedUdt));
                      }

                      Object[] expectedTuple = tupleValues.get(pk);
                      Row tuple = (Row) row.get(2);
                      assertEquals(expectedTuple.length, tuple.length());
                      assertEquals(expectedTuple[0], tuple.getLong(0));
                      assertEquals(expectedTuple[1], tuple.getString(1));
                      assertEquals(expectedTuple[2], tuple.getInt(2));
                  }
              })
              .run();
    }

    /* Complex Clustering Keys */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtsWithNulls(CassandraBridge bridge)
    {
        CqlField.CqlUdt type = bridge.udt("udt_with_nulls", "udt1")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.text())
                                     .build();
        Map<Long, Map<String, Object>> values = new HashMap<>(Tester.DEFAULT_NUM_ROWS);

        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("pk", bridge.bigint())
                                             .withClusteringKey("ck", type.frozen())
                                             .withColumn("col1", bridge.text())
                                             .withColumn("col2", bridge.timestamp())
                                             .withColumn("col3", bridge.aInt()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  int midPoint = Tester.DEFAULT_NUM_ROWS / 2;
                  for (long pk = 0; pk < Tester.DEFAULT_NUM_ROWS; pk++)
                  {
                      Map<String, Object> value = ImmutableMap.of(
                            pk < midPoint ? "a" : "b", RandomUtils.randomValue(bridge.text()).toString(),
                            "c", RandomUtils.randomValue(bridge.text()).toString());
                      values.put(pk, value);
                      writer.write(pk, bridge.toUserTypeValue(type, value),
                                       RandomUtils.randomValue(bridge.text()),
                                       RandomUtils.randomValue(bridge.timestamp()),
                                       RandomUtils.randomValue(bridge.aInt()));
                  }
              })
              .withCheck(dataset -> {
                  Map<Long, Row> rows = dataset.collectAsList().stream()
                                                               .collect(Collectors.toMap(row -> row.getLong(0),
                                                                                         row -> row.getStruct(1)));
                  assertEquals(values.size(), rows.size());
                  for (Map.Entry<Long, Row> pk : rows.entrySet())
                  {
                      assertEquals(values.get(pk.getKey()).get("a"), pk.getValue().getString(0));
                      assertEquals(values.get(pk.getKey()).get("b"), pk.getValue().getString(1));
                      assertEquals(values.get(pk.getKey()).get("c"), pk.getValue().getString(2));
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMapClusteringKey(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.map(bridge.bigint(), bridge.text()).frozen())
                                 .withColumn("c1", bridge.text())
                                 .withColumn("c2", bridge.text())
                                 .withColumn("c3", bridge.text()))
              .withNumRandomRows(5)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testListClusteringKey(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.list(bridge.bigint()).frozen())
                                 .withColumn("c1", bridge.text())
                                 .withColumn("c2", bridge.text())
                                 .withColumn("c3", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSetClusteringKey(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.set(bridge.aFloat()).frozen())
                                 .withColumn("c1", bridge.text())
                                 .withColumn("c2", bridge.text())
                                 .withColumn("c3", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdTClusteringKey(CassandraBridge bridge)
    {
        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("pk", bridge.uuid())
                                             .withClusteringKey("ck", bridge.udt("udt_clustering_key", "udt1")
                                                                            .withField("a", bridge.text())
                                                                            .withField("b", bridge.aFloat())
                                                                            .withField("c", bridge.bigint())
                                                                            .build().frozen())
                                             .withColumn("c1", bridge.text())
                                             .withColumn("c2", bridge.text())
                                             .withColumn("c3", bridge.text()))
              .run();
    }

    /* Column Prune Filters */

    // CHECKSTYLE IGNORE: Despite being static and final, this is a mutable field not to be confused with a constant
    private static final AtomicLong skippedRawBytes = new AtomicLong(0L);
    private static final AtomicLong skippedInputStreamBytes = new AtomicLong(0L);  // CHECKSTYLE IGNORE: Ditto
    private static final AtomicLong skippedRangeBytes = new AtomicLong(0L);        // CHECKSTYLE IGNORE: Ditto

    private static void resetStats()
    {
        skippedRawBytes.set(0L);
        skippedInputStreamBytes.set(0L);
        skippedRangeBytes.set(0L);
    }

    @SuppressWarnings("unused")  // Actually used via reflection in testLargeBlobExclude()
    public static final Stats STATS = new Stats()
    {
        @Override
        public void skippedBytes(long length)
        {
            skippedRawBytes.addAndGet(length);
        }

        @Override
        public void inputStreamBytesSkipped(SSTableSource<? extends SSTable> ssTable,
                                            long bufferedSkipped,
                                            long rangeSkipped)
        {
            skippedInputStreamBytes.addAndGet(bufferedSkipped);
            skippedRangeBytes.addAndGet(rangeSkipped);
        }
    };

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testLargeBlobExclude(CassandraBridge bridge)
    {
        qt().forAll(booleans().all())
            .checkAssert(enableCompression ->
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withClusteringKey("ck", bridge.aInt())
                                         .withColumn("a", bridge.bigint())
                                         .withColumn("b", bridge.text())
                                         .withColumn("c", bridge.blob())
                                         .withBlobSize(400000)  // Override blob size to write large blobs that we can skip
                                         .withCompression(enableCompression))
                      // Test with LZ4 enabled & disabled
                      .withColumns("pk", "ck", "a")  // Partition/clustering keys are always required
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .withStatsClass(EndToEndTests.class.getName() + ".STATS")  // Override stats so we can count bytes skipped
                      .withCheck(dataset -> {
                          EndToEndTests.resetStats();
                          List<Row> rows = dataset.collectAsList();
                          assertFalse(rows.isEmpty());
                          for (Row row : rows)
                          {
                              assertTrue(row.schema().getFieldIndex("pk").isDefined());
                              assertTrue(row.schema().getFieldIndex("ck").isDefined());
                              assertTrue(row.schema().getFieldIndex("a").isDefined());
                              assertFalse(row.schema().getFieldIndex("b").isDefined());
                              assertFalse(row.schema().getFieldIndex("c").isDefined());
                              assertEquals(3, row.length());
                              assertTrue(row.get(0) instanceof String);
                              assertTrue(row.get(1) instanceof Integer);
                              assertTrue(row.get(2) instanceof Long);
                          }
                          assertTrue(skippedRawBytes.get() > 50_000_000);
                          assertTrue(skippedInputStreamBytes.get() > 2_500_000);
                          assertTrue(skippedRangeBytes.get() > 5_000_000);
                      })
                      .withReset(EndToEndTests::resetStats)
                      .run()
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeColumns(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.list(bridge.text()))
                                 .withColumn("e", bridge.map(bridge.bigint(), bridge.text())))
              .withColumns("pk", "ck", "a", "c", "e")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withCheck(dataset -> {
                  List<Row> rows = dataset.collectAsList();
                  assertFalse(rows.isEmpty());
                  for (Row row : rows)
                  {
                      assertTrue(row.schema().getFieldIndex("pk").isDefined());
                      assertTrue(row.schema().getFieldIndex("ck").isDefined());
                      assertTrue(row.schema().getFieldIndex("a").isDefined());
                      assertFalse(row.schema().getFieldIndex("b").isDefined());
                      assertTrue(row.schema().getFieldIndex("c").isDefined());
                      assertFalse(row.schema().getFieldIndex("d").isDefined());
                      assertTrue(row.schema().getFieldIndex("e").isDefined());
                      assertEquals(5, row.length());
                      assertTrue(row.get(0) instanceof String);
                      assertTrue(row.get(1) instanceof Integer);
                      assertTrue(row.get(2) instanceof Long);
                      assertTrue(row.get(3) instanceof String);
                      assertTrue(row.get(4) instanceof scala.collection.immutable.Map);
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUpsertExcludeColumns(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.list(bridge.text()))
                                 .withColumn("e", bridge.map(bridge.bigint(), bridge.text())))
              .withColumns("pk", "ck", "a", "c", "e")
              .withUpsert()
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withCheck(dataset -> {
                  List<Row> rows = dataset.collectAsList();
                  assertFalse(rows.isEmpty());
                  for (Row row : rows)
                  {
                      assertTrue(row.schema().getFieldIndex("pk").isDefined());
                      assertTrue(row.schema().getFieldIndex("ck").isDefined());
                      assertTrue(row.schema().getFieldIndex("a").isDefined());
                      assertFalse(row.schema().getFieldIndex("b").isDefined());
                      assertTrue(row.schema().getFieldIndex("c").isDefined());
                      assertFalse(row.schema().getFieldIndex("d").isDefined());
                      assertTrue(row.schema().getFieldIndex("e").isDefined());
                      assertEquals(5, row.length());
                      assertTrue(row.get(0) instanceof String);
                      assertTrue(row.get(1) instanceof Integer);
                      assertTrue(row.get(2) instanceof Long);
                      assertTrue(row.get(3) instanceof String);
                      assertTrue(row.get(4) instanceof scala.collection.immutable.Map);
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeNoColumns(CassandraBridge bridge)
    {
        // Include all columns
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.bigint())
                                 .withColumn("e", bridge.aFloat())
                                 .withColumn("f", bridge.bool()))
              .withColumns("pk", "ck", "a", "b", "c", "d", "e", "f")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUpsertExcludeNoColumns(CassandraBridge bridge)
    {
        // Include all columns
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.bigint())
                                 .withColumn("e", bridge.aFloat())
                                 .withColumn("f", bridge.bool()))
              .withColumns("pk", "ck", "a", "b", "c", "d", "e", "f")
              .withUpsert()
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeAllColumns(CassandraBridge bridge)
    {
        // Exclude all columns except for partition/clustering keys
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.bigint())
                                 .withColumn("e", bridge.aFloat())
                                 .withColumn("f", bridge.bool()))
              .withColumns("pk", "ck")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUpsertExcludeAllColumns(CassandraBridge bridge)
    {
        // Exclude all columns except for partition/clustering keys
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.bigint())
                                 .withColumn("e", bridge.aFloat())
                                 .withColumn("f", bridge.bool()))
              .withUpsert()
              .withColumns("pk", "ck")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludePartitionOnly(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid()))
              .withColumns("pk")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeKeysOnly(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck1", bridge.text())
                                 .withClusteringKey("ck2", bridge.bigint()))
              .withColumns("pk", "ck1", "ck2")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeKeysStaticColumnOnly(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck1", bridge.text())
                                 .withClusteringKey("ck2", bridge.bigint())
                                 .withStaticColumn("c1", bridge.timestamp()))
              .withColumns("pk", "ck1", "ck2", "c1")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeStaticColumn(CassandraBridge bridge)
    {
        // Exclude static columns
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withStaticColumn("a", bridge.text())
                                 .withStaticColumn("b", bridge.timestamp())
                                 .withColumn("c", bridge.bigint())
                                 .withStaticColumn("d", bridge.uuid()))
              .withColumns("pk", "ck", "c")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUpsertExcludeStaticColumn(CassandraBridge bridge)
    {
        // Exclude static columns
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withStaticColumn("a", bridge.text())
                                 .withStaticColumn("b", bridge.timestamp())
                                 .withColumn("c", bridge.bigint())
                                 .withStaticColumn("d", bridge.uuid()))
              .withColumns("pk", "ck", "c")
              .withUpsert()
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testLastModifiedTimestampAddedWithStaticColumn(CassandraBridge bridge)
    {
        int numRows = 5;
        int numColumns = 5;
        long leastExpectedTimestamp = Timestamp.from(Instant.now()).getTime();
        Set<Pair<Integer, Long>> observedLMT = new HashSet<>();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.aInt())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withStaticColumn("a", bridge.text()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      for (int column = 0; column < numColumns; column++)
                      {
                          // Makes sure the insertion time of each row is unique
                          Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                          writer.write(row, column, "text" + column);
                      }
                  }
              })
              .withLastModifiedTimestampColumn()
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      assertEquals(4, row.length());
                      assertEquals("text4", String.valueOf(row.get(2)));
                      long lmt = row.getTimestamp(3).getTime();
                      assertTrue(lmt > leastExpectedTimestamp);
                      // Due to the static column so the LMT is the same per partition.
                      // Using the pair of ck and lmt for uniqueness check.
                      assertTrue(observedLMT.add(Pair.of(row.getInt(1), lmt)), "Observed a duplicated LMT");
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testLastModifiedTimestampWithExcludeColumns(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge).withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.bigint())
                                 .withColumn("b", bridge.text())
                                 .withColumn("c", bridge.ascii())
                                 .withColumn("d", bridge.list(bridge.text()))
                                 .withColumn("e", bridge.map(bridge.bigint(), bridge.text())))
              .withLastModifiedTimestampColumn()
              .withColumns("pk", "ck", "a", "c", "e", "last_modified_timestamp")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withCheck(dataset -> {
                  List<Row> rows = dataset.collectAsList();
                  assertFalse(rows.isEmpty());
                  for (Row row : rows)
                  {
                      assertTrue(row.schema().getFieldIndex("pk").isDefined());
                      assertTrue(row.schema().getFieldIndex("ck").isDefined());
                      assertTrue(row.schema().getFieldIndex("a").isDefined());
                      assertFalse(row.schema().getFieldIndex("b").isDefined());
                      assertTrue(row.schema().getFieldIndex("c").isDefined());
                      assertFalse(row.schema().getFieldIndex("d").isDefined());
                      assertTrue(row.schema().getFieldIndex("e").isDefined());
                      assertTrue(row.schema().getFieldIndex("last_modified_timestamp").isDefined());
                      assertEquals(6, row.length());
                      assertTrue(row.get(0) instanceof String);
                      assertTrue(row.get(1) instanceof Integer);
                      assertTrue(row.get(2) instanceof Long);
                      assertTrue(row.get(3) instanceof String);
                      assertTrue(row.get(4) instanceof scala.collection.immutable.Map);
                      assertTrue(row.get(5) instanceof java.sql.Timestamp);
                      assertTrue(((java.sql.Timestamp) row.get(5)).getTime() > 0);
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testLastModifiedTimestampAddedWithSimpleColumns(CassandraBridge bridge)
    {
        int numRows = 10;
        long leastExpectedTimestamp = Timestamp.from(Instant.now()).getTime();
        Set<Long> observedLMT = new HashSet<>();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.aInt())
                                 .withColumn("a", bridge.text())
                                 .withColumn("b", bridge.aDouble())
                                 .withColumn("c", bridge.uuid()))
              .withLastModifiedTimestampColumn()
              .dontWriteRandomData()
              .withDelayBetweenSSTablesInSecs(10)
              .withSSTableWriter(writer -> {
                  for (int row = 0; row < numRows; row++)
                  {
                      writer.write(row, "text" + row, Math.random(), UUID.randomUUID());
                  }
              })
              .withSSTableWriter(writer -> {
                  // The second write overrides the first one above
                  for (int row = 0; row < numRows; row++)
                  {
                      // Makes sure the insertion time of each row is unique
                      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                      writer.write(row, "text" + row, Math.random(), UUID.randomUUID());
                  }
              })
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      assertEquals(5, row.length());
                      long lmt = row.getTimestamp(4).getTime();
                      assertTrue(lmt > leastExpectedTimestamp + 10);
                      assertTrue(observedLMT.add(lmt), "Observed a duplicated LMT");
                  }
              })
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testLastModifiedTimestampAddedWithComplexColumns(CassandraBridge bridge)
    {
        long leastExpectedTimestamp = Timestamp.from(Instant.now()).getTime();
        Set<Long> observedLMT = new HashSet<>();
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.timeuuid())
                                 .withClusteringKey("ck", bridge.aInt())
                                 .withColumn("a", bridge.map(bridge.text(),
                                                             bridge.set(bridge.text()).frozen()))
                                 .withColumn("b", bridge.set(bridge.text()))
                                 .withColumn("c", bridge.tuple(bridge.aInt(),
                                                               bridge.tuple(bridge.bigint(),
                                                                            bridge.timeuuid())))
                                 .withColumn("d", bridge.frozen(bridge.list(bridge.aFloat())))
                                 .withColumn("e", bridge.udt("keyspace", "udt")
                                                        .withField("field1", bridge.varchar())
                                                        .withField("field2", bridge.frozen(bridge.set(bridge.text())))
                                                        .build()))
              .withLastModifiedTimestampColumn()
              .withNumRandomRows(10)
              .withNumRandomSSTables(2)
              // Makes sure the insertion time of each row is unique
              .withWriteListener(row -> Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS))
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      assertEquals(8, row.length());
                      long lmt = row.getTimestamp(7).getTime();
                      assertTrue(lmt > leastExpectedTimestamp);
                      assertTrue(observedLMT.add(lmt), "Observed a duplicated LMT");
                  }
              })
              .run();
    }

    /* Identifiers That Need Quoting Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedKeyspaceName(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testReservedWordKeyspaceName(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("keyspace")
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedTableName(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testReservedWordTableName(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("table")
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedPartitionKey(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("Partition_Key_0", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleQuotedPartitionKeys(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("Partition_Key_0", bridge.uuid())
                                              .withPartitionKey("Partition_Key_1", bridge.bigint())
                                              .withColumn("c", bridge.text())
                                              .withColumn("d", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("d")
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedPartitionClusteringKeys(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("a", bridge.uuid())
                                              .withClusteringKey("Clustering_Key_0", bridge.bigint())
                                              .withClusteringKey("Clustering_Key_1", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedColumnNames(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("Partition_Key_0", bridge.uuid())
                                              .withColumn("Column_1", bridge.varint())
                                              .withColumn("Column_2", bridge.text()))
              .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testQuotedColumnNamesWithColumnFilter(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("Quoted_Keyspace_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withTable("Quoted_Table_" + UUID.randomUUID().toString().replaceAll("-", "_"))
                                              .withPartitionKey("Partition_Key_0", bridge.uuid())
                                              .withColumn("Column_1", bridge.varint())
                                              .withColumn("Column_2", bridge.text())
                                              .withQuoteIdentifiers())
              .withColumns("Partition_Key_0", "Column_1") // PK is required for lookup of the inserted data
              .run();
    }
}
