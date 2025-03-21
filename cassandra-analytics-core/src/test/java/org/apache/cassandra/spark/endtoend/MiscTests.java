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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;

/**
 * End-to-end tests that write random data to multiple SSTables,
 * reads the data back into Spark and verifies the rows in Spark match the expected.
 * Uses QuickTheories to test many combinations of field data types and clustering key sort order.
 * Uses custom SSTableTombstoneWriter to write SSTables with tombstones
 * to verify Spark Bulk Reader correctly purges tombstoned data.
 */

@Tag("Sequential")
public class MiscTests
{
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
                      assertThat(row.size()).isEqualTo(6);
                      UUID key = UUID.fromString(row.getString(0));
                      UUID value1 = UUID.fromString(row.getString(2));
                      UUID value2 = UUID.fromString(row.getString(4));
                      assertThat(rows.containsKey(key)).isTrue();
                      assertThat(value1).isEqualTo(rows.get(key));
                      assertThat(value2).isEqualTo(value1);
                      assertThat(row.get(1)).isNull();
                      assertThat(row.get(3)).isNull();
                      assertThat(row.get(5)).isNull();
                  }
              })
              .withReset(rows::clear)
              .run(bridge.getVersion());
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
                      assertThat(row.size()).isEqualTo(8);
                      String a = row.getString(0);
                      String b = row.getString(1);
                      String c = row.getString(2);
                      String e = row.getString(4);
                      String g = row.getString(6);
                      String key = a + ":" + b + ":" + c;
                      String value = e + ":" + g;
                      assertThat(rows.containsKey(key)).isTrue();
                      assertThat(value).isEqualTo(rows.get(key));
                      assertThat(row.get(3)).isNull();
                      assertThat(row.get(5)).isNull();
                      assertThat(row.get(7)).isNull();
                  }
              })
              .withReset(rows::clear)
              .run(bridge.getVersion());
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
                                              .withColumn("c2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testReservedWordKeyspaceName(CassandraBridge bridge)
    {
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace("keyspace")
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("c1", bridge.varint())
                                              .withColumn("c2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
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
                                              .withColumn("c2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
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
                                              .withColumn("c2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
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
                                              .withColumn("c2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
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
                                              .withColumn("d", bridge.bigint())
                                              .withQuotedIdentifiers())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("d")
              .run(bridge.getVersion());
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
                                              .withClusteringKey("Clustering_Key_1", bridge.text())
                                              .withQuotedIdentifiers())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run(bridge.getVersion());
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
                                              .withColumn("Column_2", bridge.text())
                                              .withQuotedIdentifiers())
              .run(bridge.getVersion());
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
                                              .withQuotedIdentifiers())
              .withColumns("Partition_Key_0", "Column_1") // PK is required for lookup of the inserted data
              .run(bridge.getVersion());
    }

    /* NULL values in regular columns */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSinglePartitionKeyWithNullValueColumn(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge).withPartitionKey("a", bridge.bigint())
                                 .withColumn("c", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withNullRegularColumns()
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultiplePartitionKeysWithNullValueColumn(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.bigint())
                                 .withPartitionKey("b", bridge.text())
                                 .withPartitionKey("d", bridge.aDouble())
                                 .withColumn("c", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withNullRegularColumns()
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSinglePartitionAndClusteringKeyWithNullValueColumn(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(clusteringKeyType ->
                         Tester.builder(TestSchema.builder(bridge).withPartitionKey("a", bridge.bigint())
                                                  .withClusteringKey("b", clusteringKeyType)
                                                  .withColumn("c", bridge.text()))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .withNullRegularColumns()
                               .run(bridge.getVersion()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleValueColumnsWithNullValueColumn(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(clusteringKeyType ->
                         Tester.builder(TestSchema.builder(bridge).withPartitionKey("a", bridge.bigint())
                                                  .withClusteringKey("b", clusteringKeyType)
                                                  .withColumn("c", bridge.text())
                                                  .withColumn("d", bridge.aInt())
                                                  .withColumn("e", bridge.ascii())
                                                  .withColumn("f", bridge.blob()))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .withNullRegularColumns()
                               .run(bridge.getVersion()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludeSomeColumnsWithNullValueColumn(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge).withPartitionKey("a", bridge.bigint())
                                 .withClusteringKey("b", bridge.aInt())
                                 .withColumn("c", bridge.text())
                                 .withColumn("d", bridge.aInt())
                                 .withColumn("e", bridge.ascii())
                                 .withColumn("f", bridge.blob()))
              .withColumns("a", "b", "d")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withNullRegularColumns()
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testStaticColumnWithNullValueColumn(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("a", bridge.uuid())
                                 .withClusteringKey("b", bridge.bigint())
                                 .withStaticColumn("c", bridge.aInt())
                                 .withColumn("d", bridge.text()))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withNullRegularColumns()
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNullValueColumnWithPushDownFilter(CassandraBridge bridge)
    {
        int numRows = 10;
        Tester.builder(TestSchema.builder(bridge).withPartitionKey("a", bridge.aInt()).withColumn("b", bridge.aInt()))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      writer.write(i, null);
                  }
              })
              .withFilter("a=1")
              .withCheck((ds) -> {
                  for (Row row : ds.collectAsList())
                  {
                      int a = row.getInt(0);
                      assertThat(a).isEqualTo(1);
                      assertThat(row.get(1)).isNull();
                  }
              })
              .run(bridge.getVersion());
    }

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

        public BufferingInputStreamStats<SSTable> bufferingInputStreamStats()
        {
            return new BufferingInputStreamStats<SSTable>()
            {
                @Override
                public void inputStreamBytesSkipped(CassandraFileSource<SSTable> ssTable,
                                                    long bufferedSkipped,
                                                    long rangeSkipped)
                {
                    skippedInputStreamBytes.addAndGet(bufferedSkipped);
                    skippedRangeBytes.addAndGet(rangeSkipped);
                }
            };
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
                               .withStatsClass(MiscTests.class.getName() + ".STATS")  // Override stats so we can count bytes skipped
                               .withCheck(dataset -> {
                                   MiscTests.resetStats();
                                   List<Row> rows = dataset.collectAsList();
                                   assertThat(rows.isEmpty()).isFalse();
                                   for (Row row : rows)
                                   {
                                       assertThat(row.schema().getFieldIndex("pk").isDefined()).isTrue();
                                       assertThat(row.schema().getFieldIndex("ck").isDefined()).isTrue();
                                       assertThat(row.schema().getFieldIndex("a").isDefined()).isTrue();
                                       assertThat(row.schema().getFieldIndex("b").isDefined()).isFalse();
                                       assertThat(row.schema().getFieldIndex("c").isDefined()).isFalse();
                                       assertThat(row.length()).isEqualTo(3);
                                       assertThat(row.get(0) instanceof String).isTrue();
                                       assertThat(row.get(1) instanceof Integer).isTrue();
                                       assertThat(row.get(2) instanceof Long).isTrue();
                                   }
                                   assertThat(skippedRawBytes.get()).isGreaterThanOrEqualTo(50_000_000);
                                   assertThat(skippedInputStreamBytes.get()).isGreaterThanOrEqualTo(2_500_000);
                                   assertThat(skippedRangeBytes.get()).isGreaterThanOrEqualTo(5_000_000);
                               })
                               .withReset(MiscTests::resetStats)
                               .run(bridge.getVersion())
            );
    }
}
