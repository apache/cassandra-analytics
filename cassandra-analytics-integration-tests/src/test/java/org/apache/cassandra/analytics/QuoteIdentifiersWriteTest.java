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

package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the bulk writer behavior when requiring quoted identifiers for keyspace, table name, and column names.
 *
 * <p>These tests exercise a full integration test, which includes testing Sidecar behavior when dealing with quoted
 * identifiers.
 */
@ExtendWith(VertxExtension.class)
class QuoteIdentifiersWriteTest extends SparkIntegrationTestBase
{
    static final int ROW_COUNT = 10_000;

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testWriteWithMixedCaseKeyspaceName()
    {
        QualifiedName qualifiedTableName = uniqueTestTableFullName("QuOtEd_KeYsPaCe");
        runWriteTestScenario(qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testWriteWithReservedWordKeyspaceName()
    {
        // keyspace is a reserved word
        QualifiedName qualifiedTableName = uniqueTestTableFullName("keyspace");
        runWriteTestScenario(qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testWriteWithMixedCaseTableName()
    {
        QualifiedName qualifiedTableName = uniqueTestTableFullName(TEST_KEYSPACE, "QuOtEd_TaBlE");
        runWriteTestScenario(qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testWriteWithReservedWordTableName()
    {
        // table is a reserved word
        runWriteTestScenario(new QualifiedName(TEST_KEYSPACE, "table"));
    }

    void runWriteTestScenario(QualifiedName tableName)
    {
        String quotedKeyspace = tableName.maybeQuotedKeyspace();
        createTestKeyspace(quotedKeyspace, ImmutableMap.of("datacenter1", 1));
        createTestTable(String.format("CREATE TABLE IF NOT EXISTS %s (\"IdEnTiFiEr\" bigint, course blob, \"limit\" bigint, PRIMARY KEY(\"IdEnTiFiEr\"));",
                                      tableName));
        waitUntilSidecarPicksUpSchemaChange(tableName.maybeQuotedKeyspace());

        SparkSession spark = getOrCreateSparkSession();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);
        SQLContext sql = spark.sqlContext();

        int parallelism = sc.defaultParallelism();
        JavaRDD<Row> rows = genDataset(javaSparkContext, ROW_COUNT, parallelism);
        Dataset<Row> df = sql.createDataFrame(rows, writeSchema());

        bulkWriterDataFrameWriter(df, tableName).option("quote_identifiers", "true")
                                                .save();
        validateWrites(tableName, rows);
    }

    private void validateWrites(QualifiedName tableName, JavaRDD<Row> rowsWritten)
    {
        // build a set of entries read from Cassandra into a set
        Set<String> actualEntries = Arrays.stream(sidecarTestContext.cassandraTestContext()
                                                                    .cluster()
                                                                    .coordinator(1)
                                                                    .execute(String.format("SELECT * FROM %s;", tableName), ConsistencyLevel.LOCAL_QUORUM))
                                          .map((Object[] columns) -> String.format("%s:%s:%s",
                                                                                   new String(((ByteBuffer) columns[1]).array(), StandardCharsets.UTF_8),
                                                                                   columns[0],
                                                                                   columns[2]))
                                          .collect(Collectors.toSet());

        // Number of entries in Cassandra must match the original datasource
        assertThat(actualEntries.size()).isEqualTo(rowsWritten.count());

        // remove from actual entries to make sure that the data read is the same as the data written
        rowsWritten.collect()
                   .forEach(row -> {
                       String key = String.format("%s:%d:%d",
                                                  new String((byte[]) row.get(0), StandardCharsets.UTF_8),
                                                  row.getLong(1),
                                                  row.getLong(2));
                       assertThat(actualEntries.remove(key)).as(key + " is expected to exist in the actual entries")
                                                            .isTrue();
                   });

        // If this fails, it means there was more data in the database than we expected
        assertThat(actualEntries).as("All entries are expected to be read from database")
                                 .isEmpty();
    }

    static StructType writeSchema()
    {
        return new StructType()
               .add("course", BinaryType, false)
               .add("IdEnTiFiEr", LongType, false) // case-sensitive struct
               .add("limit", LongType, false); // limit is a reserved word in Cassandra
    }

    private static JavaRDD<Row> genDataset(JavaSparkContext sc, int recordCount, Integer parallelism)
    {
        long recordsPerPartition = recordCount / parallelism;
        long remainder = recordCount - (recordsPerPartition * parallelism);
        List<Integer> seq = IntStream.range(0, parallelism).boxed().collect(Collectors.toList());
        return sc.parallelize(seq, parallelism).mapPartitionsWithIndex(
        (Function2<Integer, Iterator<Integer>, Iterator<Row>>) (index, integerIterator) -> {
            long firstRecordNumber = index * recordsPerPartition;
            long recordsToGenerate = index.equals(parallelism) ? remainder : recordsPerPartition;
            return LongStream.range(0, recordsToGenerate).mapToObj(offset -> {
                long limit = firstRecordNumber + offset;
                return RowFactory.create(("course-" + limit).getBytes(), limit, limit);
            }).iterator();
        }, false);
    }
}
