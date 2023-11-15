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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the bulk reader behavior when requiring quoted identifiers for keyspace, table name, and column names.
 *
 * <p>These tests exercise a full integration test, which includes testing Sidecar behavior when dealing with quoted
 * identifiers.
 */
@ExtendWith(VertxExtension.class)
class QuoteIdentifiersReadTest extends SparkIntegrationTestBase
{

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testMixedCaseKeyspace(VertxTestContext context)
    {
        QualifiedName qualifiedTableName = uniqueTestTableFullName("QuOtEd_KeYsPaCe");
        runTestScenario(context, qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testReservedWordKeyspace(VertxTestContext context)
    {
        // keyspace is a reserved word
        QualifiedName qualifiedTableName = uniqueTestTableFullName("keyspace");
        runTestScenario(context, qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testMixedCaseTable(VertxTestContext context)
    {
        QualifiedName qualifiedTableName = uniqueTestTableFullName(TEST_KEYSPACE, "QuOtEd_TaBlE");
        runTestScenario(context, qualifiedTableName);
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testReservedWordTable(VertxTestContext context)
    {
        // table is a reserved word
        runTestScenario(context, new QualifiedName(TEST_KEYSPACE, "table"));
    }

    @CassandraIntegrationTest(nodesPerDc = 1, gossip = true)
    void testReadComplexSchema(VertxTestContext context)
    {
        QualifiedName tableName = uniqueTestTableFullName("QuOtEd_KeYsPaCe", "QuOtEd_TaBlE");

        String quotedKeyspace = tableName.maybeQuotedKeyspace();
        createTestKeyspace(quotedKeyspace, ImmutableMap.of("datacenter1", 1));

        // Create UDT
        String createUdtQuery = "CREATE TYPE " + quotedKeyspace + ".\"UdT1\" (\"TimE\" bigint, \"limit\" int);";
        sidecarTestContext.cassandraTestContext()
                          .cluster()
                          .getFirstRunningInstance()
                          .coordinator()
                          .execute(createUdtQuery, ConsistencyLevel.ALL);

        createTestTable(String.format("CREATE TABLE IF NOT EXISTS %s (" +
                                      "\"IdEnTiFiEr\" text, " +
                                      "IdEnTiFiEr int, " +
                                      "\"User_Defined_Type\" frozen<\"UdT1\">, " +
                                      "PRIMARY KEY(\"IdEnTiFiEr\", IdEnTiFiEr));",
                                      tableName));
        List<String> dataset = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
        populateTableWithUdt(tableName, dataset);
        waitUntilSidecarPicksUpSchemaChange(quotedKeyspace);

        Dataset<Row> data = bulkReaderDataFrame(tableName).option("quote_identifiers", "true")
                                                          .load();
        assertThat(data.count()).isEqualTo(dataset.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getString(0)))
                                .collect(Collectors.toList());
        for (int i = 0; i < dataset.size(); i++)
        {
            Row row = rowList.get(i);
            assertThat(row.getString(0)).isEqualTo(dataset.get(i));
            assertThat(row.getInt(1)).isEqualTo(i);
            assertThat(row.getStruct(2).getLong(0)).isEqualTo(i); // from UdT1 TimE column
            assertThat(row.getStruct(2).getInt(1)).isEqualTo(i); // from UdT1 limit column (limit is a reserved word)
        }
        context.completeNow();
    }

    void runTestScenario(VertxTestContext context, QualifiedName tableName)
    {
        String quotedKeyspace = tableName.maybeQuotedKeyspace();

        createTestKeyspace(quotedKeyspace, ImmutableMap.of("datacenter1", 1));
        createTestTable(String.format("CREATE TABLE IF NOT EXISTS %s (\"IdEnTiFiEr\" text, IdEnTiFiEr int, PRIMARY KEY(\"IdEnTiFiEr\"));",
                                      tableName));
        List<String> dataset = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
        populateTable(tableName, dataset);
        waitUntilSidecarPicksUpSchemaChange(quotedKeyspace);

        Dataset<Row> data = bulkReaderDataFrame(tableName).option("quote_identifiers", "true")
                                                          .load();

        assertThat(data.count()).isEqualTo(dataset.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getString(0)))
                                .collect(Collectors.toList());
        for (int i = 0; i < dataset.size(); i++)
        {
            assertThat(rowList.get(i).getString(0)).isEqualTo(dataset.get(i));
            assertThat(rowList.get(i).getInt(1)).isEqualTo(i);
        }
        context.completeNow();
    }

    void populateTable(QualifiedName tableName, List<String> values)
    {
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            String query = String.format("INSERT INTO %s (\"IdEnTiFiEr\", IdEnTiFiEr) VALUES ('%s', %d);", tableName, value, i);
            sidecarTestContext.cassandraTestContext()
                              .cluster()
                              .getFirstRunningInstance()
                              .coordinator()
                              .execute(query, ConsistencyLevel.ALL);
        }
    }

    void populateTableWithUdt(QualifiedName tableName, List<String> dataset)
    {
        for (int i = 0; i < dataset.size(); i++)
        {
            String value = dataset.get(i);
            String query = String.format("INSERT INTO %s (\"IdEnTiFiEr\", IdEnTiFiEr, \"User_Defined_Type\") " +
                                         "VALUES ('%s', %d, { \"TimE\" : %d, \"limit\" : %d });",
                                         tableName, value, i, i, i);
            sidecarTestContext.cassandraTestContext()
                              .cluster()
                              .getFirstRunningInstance()
                              .coordinator()
                              .execute(query, ConsistencyLevel.ALL);
        }
    }
}
