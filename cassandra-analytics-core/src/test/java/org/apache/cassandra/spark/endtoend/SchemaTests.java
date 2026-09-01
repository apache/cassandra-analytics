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

package org.apache.cassandra.spark.endtoend;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

@Tag("Sequential")
public class SchemaTests
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
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testOnlyPartitionKeys(CassandraBridge bridge)
    {
        // Special case where schema is only partition keys
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run(bridge.getVersion());
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withPartitionKey("b", bridge.bigint()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSingleClusteringKeyOrderBy(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.sortOrder())
            .assuming((clusteringKeyType, sortOrder) -> clusteringKeyType.supportedAsPrimaryKeyColumn())
            .checkAssert((clusteringKeyType, sortOrder) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("a", bridge.bigint())
                                                  .withClusteringKey("b", clusteringKeyType)
                                                  .withColumn("c", bridge.bigint())
                                                  .withSortOrder(sortOrder))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .withCheck(dataset -> assertThat(dataset.count()).isEqualTo(numRows * numColumns * 2))
              .run(bridge.getVersion());
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
                      assertThat(staticCol).isNull();
                  }
                  else
                  {
                      assertThat(staticCol).isEqualTo("Non-null");
                  }
              })
              .run(bridge.getVersion());
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
        // AtomicLong is not sufficient here, using BigInteger instead. The aggregation
        // in the withCheck block will overflow and this will throw an exception in Spark 4.
        // In earlier versions of Spark the long value would just overflow and the aggregated
        // value was incorrect.
        AtomicReference<BigInteger> total = new AtomicReference<>(BigInteger.ZERO);
        Random random = new Random();
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
                      TestSchema.TestRow testRow = schema.randomRow(random);
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
                      TestSchema.TestRow newTestRow = testRow.copy("e", random.nextLong())
                                                             .copy("d", UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      writer.write(newTestRow.allValues());
                  }
              })
              .withSSTableWriter(writer -> {
                  for (TestSchema.TestRow testRow : ImmutableSet.copyOf(rows.values()))
                  {
                      // Update rows with new values - this should be the final values seen by Spark
                      TestSchema.TestRow newTestRow = testRow.copy("e", random.nextLong())
                                                             .copy("d", UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      total.updateAndGet(t -> t.add(BigInteger.valueOf(newTestRow.getLong("e"))));
                      writer.write(newTestRow.allValues());
                  }
              })
              // Verify rows returned by Spark match expected
              .withReadListener(actualRow -> assertThat(rows.containsKey(actualRow.getUUID("a"))).isTrue())
              .withReadListener(actualRow -> assertThat(actualRow).isEqualTo(rows.get(actualRow.getUUID("a"))))
              .withReadListener(actualRow -> assertThat(actualRow.getLong("e")).isEqualTo(rows.get(actualRow.getUUID("a")).getLong("e")))
              // Verify Spark aggregations match expected
              .withCheck(dataset -> {
                  BigInteger sum = dataset.agg(functions.sum(
                                          functions.col("e").cast(DataTypes.createDecimalType(38, 0))))
                                          .first()
                                          .getDecimal(0)
                                          .toBigInteger();
                  assertThat(sum).isEqualTo(total.get());
              })
              .withCheck(dataset -> assertThat(dataset.groupBy().count().first().getLong(0)).isEqualTo(rows.size()))
              .withReset(() -> {
                  total.set(BigInteger.ZERO);
                  rows.clear();
              })
              .run(bridge.getVersion());
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
        Random random = new Random();

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
                      pk < midPoint ? "a" : "b", bridge.text().randomValue(random).toString(),
                      "c", bridge.text().randomValue(random).toString());
                      values.put(pk, value);
                      writer.write(pk, bridge.toUserTypeValue(type, value),
                                   bridge.text().randomValue(random),
                                   bridge.timestamp().randomValue(random),
                                   bridge.aInt().randomValue(random));
                  }
              })
              .withCheck(dataset -> {
                  Map<Long, Row> rows = dataset.collectAsList().stream()
                                               .collect(Collectors.toMap(row -> row.getLong(0),
                                                                         row -> row.getStruct(1)));
                  assertThat(rows.size()).isEqualTo(values.size());
                  for (Map.Entry<Long, Row> pk : rows.entrySet())
                  {
                      assertThat(pk.getValue().getString(0)).isEqualTo(values.get(pk.getKey()).get("a"));
                      assertThat(pk.getValue().getString(1)).isEqualTo(values.get(pk.getKey()).get("b"));
                      assertThat(pk.getValue().getString(2)).isEqualTo(values.get(pk.getKey()).get("c"));
                  }
              })
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
    }
}
