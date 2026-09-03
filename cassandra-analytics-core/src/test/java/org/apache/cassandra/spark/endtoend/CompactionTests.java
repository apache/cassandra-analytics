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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("Sequential")
public class CompactionTests
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleSSTablesCompaction(CassandraBridge bridge)
    {
        AtomicLong startTotal = new AtomicLong(0);
        AtomicLong newTotal = new AtomicLong(0);
        Random random = new Random();
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
                      long c1 = random.nextInt(10_000_000);
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
                      long newBalance = (long) random.nextInt(10_000_000) + column1.get(pk);
                      assertThat(newBalance).isGreaterThan(column1.get(pk));
                      newTotal.addAndGet(newBalance);
                      column1.put(pk, newBalance);
                      writer.write(pk, newBalance, column2.get(pk));
                  }
              })
              .withCheck(dataset -> {
                  assertThat(startTotal.get()).isLessThan(newTotal.get());
                  long sum = 0;
                  int count = 0;
                  for (Row row : dataset.collectAsList())
                  {
                      UUID pk = UUID.fromString(row.getString(0));
                      assertThat(row.getLong(1)).isEqualTo(column1.get(pk).longValue());
                      assertThat(row.getString(2)).isEqualTo(column2.get(pk));
                      sum += (long) row.get(1);
                      count++;
                  }
                  assertThat(count).isEqualTo(Tester.DEFAULT_NUM_ROWS);
                  assertThat(sum).isEqualTo(newTotal.get());
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
              .withCheck(dataset -> assertThat(dataset.groupBy().sum("c").first().getLong(0)).isEqualTo(total.get()))
              .withCheck(dataset -> assertThat(dataset.groupBy().count().first().getLong(0)).isEqualTo(numRowsColumns * numRowsColumns))
              .withReset(() -> total.set(0))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSingleClusteringKey(CassandraBridge bridge)
    {
        AtomicLong total = new AtomicLong(0);
        Random random = new Random();
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
                          long balance = random.nextInt(10_000_000);
                          total.addAndGet(balance);
                          String name = UUID.randomUUID().toString().substring(0, 8);
                          testSum.get(clusteringKey).add(balance);
                          writer.write(accountId, clusteringKey, balance, name);
                      }
                  }
              })
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size())
              .withCheck(dataset -> {
                  assertThat(testSum.values().stream().mapToLong(MutableLong::getValue).sum()).isEqualTo(total.get());
                  long sum = 0;
                  int count = 0;
                  for (Row row : dataset.collectAsList())
                  {
                      assertThat(row.getString(0)).isNotNull();
                      long balance = row.getLong(2);
                      assertThat(row.getString(3)).isNotNull();
                      sum += balance;
                      count++;
                  }
                  assertThat(sum).isEqualTo(total.get());
                  assertThat(count).isEqualTo(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size());
              })
              .withCheck(dataset -> {
                  // Test basic group by matches expected
                  for (Row row : dataset.groupBy("b").sum("c").collectAsList())
                  {
                      assertThat(row.getLong(1)).isEqualTo(testSum.get(row.getInt(0)).getValue().longValue());
                  }
              })
              .withReset(() -> {
                  total.set(0);
                  for (int clusteringKey : clusteringKeys)
                  {
                      testSum.put(clusteringKey, new MutableLong(0));
                  }
              })
              .run(bridge.getVersion());
    }
}
