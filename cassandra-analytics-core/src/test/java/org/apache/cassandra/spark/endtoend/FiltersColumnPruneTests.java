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

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("Sequential")
public class FiltersColumnPruneTests
{
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
                  assertThat(rows).isNotEmpty();
                  for (Row row : rows)
                  {
                      assertThat(row.schema().getFieldIndex("pk").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("ck").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("a").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("b").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("c").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("d").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("e").isDefined()).isTrue();
                      assertThat(row.length()).isEqualTo(5);
                      assertThat(row.get(0)).isInstanceOf(String.class);
                      assertThat(row.get(1)).isInstanceOf(Integer.class);
                      assertThat(row.get(2)).isInstanceOf(Long.class);
                      assertThat(row.get(3)).isInstanceOf(String.class);
                      assertThat(row.get(4)).isInstanceOf(scala.collection.immutable.Map.class);
                  }
              })
              .run(bridge.getVersion());
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
                  assertThat(rows).isNotEmpty();
                  for (Row row : rows)
                  {
                      assertThat(row.schema().getFieldIndex("pk").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("ck").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("a").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("b").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("c").isDefined()).isTrue();
                      assertThat(row.schema().getFieldIndex("d").isDefined()).isFalse();
                      assertThat(row.schema().getFieldIndex("e").isDefined()).isTrue();
                      assertThat(row.length()).isEqualTo(5);
                      assertThat(row.get(0)).isInstanceOf(String.class);
                      assertThat(row.get(1)).isInstanceOf(Integer.class);
                      assertThat(row.get(2)).isInstanceOf(Long.class);
                      assertThat(row.get(3)).isInstanceOf(String.class);
                      assertThat(row.get(4)).isInstanceOf(scala.collection.immutable.Map.class);
                  }
              })
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testExcludePartitionOnly(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid()))
              .withColumns("pk")
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
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
              .run(bridge.getVersion());
    }
}
