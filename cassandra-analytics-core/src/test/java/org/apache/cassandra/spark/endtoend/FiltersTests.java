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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("Sequential")
public class FiltersTests
{
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
                      assertThat(a).isEqualTo(1);
                  }
              })
              .run(bridge.getVersion());
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
                  assertThat(rows.size()).isEqualTo(2);
                  for (Row row : rows)
                  {
                      int a = row.getInt(0);
                      int b = row.getInt(1);
                      String key = a + ":" + b;
                      assertThat(keys.contains(key)).isTrue();
                  }
              })
              .run(bridge.getVersion());
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
              .withCheck(dataset -> assertThat(dataset.collectAsList().isEmpty()).isTrue())
              .run(bridge.getVersion());
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
                  assertThat(rows.isEmpty()).isFalse();
                  assertThat(rows.size()).isEqualTo(7);
                  for (Row row : rows)
                  {
                      assertThat(row.getInt(0)).isEqualTo(200);
                      assertThat(row.getString(1)).isEqualTo("def");
                  }
              })
              .run(bridge.getVersion());
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
                      assertThat(row.length()).isEqualTo(4);
                      assertThat(String.valueOf(row.get(2))).isEqualTo("text4");
                      long lmt = row.getTimestamp(3).getTime();
                      assertThat(lmt > leastExpectedTimestamp).isTrue();
                      // Due to the static column so the LMT is the same per partition.
                      // Using the pair of ck and lmt for uniqueness check.
                      assertThat(observedLMT.add(Pair.of(row.getInt(1), lmt))).as("Observed a duplicated LMT").isTrue();
                  }
              })
              .run(bridge.getVersion());
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
                  assertThat(rows.isEmpty()).isFalse();
                  for (Row row : rows)
                  {
                      assertThat(row.schema().getFieldIndex("pk").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("ck").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("a").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("b").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("c").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("d").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("e").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("last_modified_timestamp").isDefined()).isTrue();
                      assertThat(row.length()).isEqualTo(6);
                      assertThat(row.get(0) instanceof String).isTrue();
                      assertThat(row.get(1) instanceof Integer).isTrue();
                      assertThat(row.get(2) instanceof Long).isTrue();
                      assertThat(row.get(3) instanceof String).isTrue();
                      assertThat(row.get(4) instanceof scala.collection.immutable.Map).isTrue();
                      assertThat(row.get(5) instanceof java.sql.Timestamp).isTrue();
                      assertThat(((java.sql.Timestamp) row.get(5)).getTime() > 0).isTrue();
                  }
              })
              .run(bridge.getVersion());
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
                      assertThat(row.length()).isEqualTo(5);
                      long lmt = row.getTimestamp(4).getTime();
                      assertThat(lmt > leastExpectedTimestamp + 10).isTrue();
                      assertThat(observedLMT.add(lmt)).as("Observed a duplicated LMT").isTrue();
                  }
              })
              .run(bridge.getVersion());
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
                      assertThat(row.length()).isEqualTo(8);
                      long lmt = row.getTimestamp(7).getTime();
                      assertThat(lmt > leastExpectedTimestamp).isTrue();
                      assertThat(observedLMT.add(lmt)).as("Observed a duplicated LMT").isTrue();
                  }
              })
              .run(bridge.getVersion());
    }
}
