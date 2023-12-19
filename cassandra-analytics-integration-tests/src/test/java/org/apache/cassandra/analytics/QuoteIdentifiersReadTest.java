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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the bulk reader behavior when requiring quoted identifiers for keyspace, table name, and column names.
 *
 * <p>These tests exercise a full integration test, which includes testing Sidecar behavior when dealing with quoted
 * identifiers.
 */
class QuoteIdentifiersReadTest extends AbstractSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    static final QualifiedName TABLE_NAME_FOR_UDT_TEST = uniqueTestTableFullName("QuOtEd_KeYsPaCe", "QuOtEd_TaBlE");
    static final List<QualifiedName> TABLE_NAMES =
    Arrays.asList(uniqueTestTableFullName("QuOtEd_KeYsPaCe"),
                  uniqueTestTableFullName("keyspace"), // keyspace is a reserved word
                  uniqueTestTableFullName(TEST_KEYSPACE, "QuOtEd_TaBlE"),
                  new QualifiedName(TEST_KEYSPACE, "table"), // table is a reserved word
                  TABLE_NAME_FOR_UDT_TEST);

    @ParameterizedTest(name = "{index} => table={0}")
    @MethodSource("testInputs")
    void testQuoteIdentifiersBulkRead(QualifiedName tableName)
    {
        Dataset<Row> data = bulkReaderDataFrame(tableName).option("quote_identifiers", "true")
                                                          .load();

        assertThat(data.count()).isEqualTo(DATASET.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getString(0)))
                                .collect(Collectors.toList());
        for (int i = 0; i < DATASET.size(); i++)
        {
            assertThat(rowList.get(i).getString(0)).isEqualTo(DATASET.get(i));
            assertThat(rowList.get(i).getInt(1)).isEqualTo(i);
        }
    }

    @Test
    void testReadComplexSchema()
    {
        Dataset<Row> data = bulkReaderDataFrame(TABLE_NAME_FOR_UDT_TEST).option("quote_identifiers", "true")
                                                                        .load();
        assertThat(data.count()).isEqualTo(DATASET.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getString(0)))
                                .collect(Collectors.toList());
        for (int i = 0; i < DATASET.size(); i++)
        {
            Row row = rowList.get(i);
            assertThat(row.getString(0)).isEqualTo(DATASET.get(i));
            assertThat(row.getInt(1)).isEqualTo(i);
            assertThat(row.getStruct(2).getLong(0)).isEqualTo(i); // from UdT1 TimE column
            assertThat(row.getStruct(2).getInt(1)).isEqualTo(i); // from UdT1 limit column (limit is a reserved word)
        }
    }

    static Stream<Arguments> testInputs()
    {
        return TABLE_NAMES.stream().map(Arguments::of);
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(), Semver.SemverType.LOOSE));

        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(1)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDCs(1)
                          .withDataDirCount(1)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(1, clusterBuilder.getTokenCount());
        clusterBuilder.withTokenSupplier(tokenSupplier);
        UpgradeableCluster cluster = clusterBuilder.createWithoutStarting();
        cluster.startup();
        return cluster;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s " +
                                      "(\"IdEnTiFiEr\" text, IdEnTiFiEr int, PRIMARY KEY(\"IdEnTiFiEr\"));";

        TABLE_NAMES.forEach(name -> {
            createTestKeyspace(name, DC1_RF1);

            if (!name.equals(TABLE_NAME_FOR_UDT_TEST))
            {
                createTestTable(name, createTableStatement);
                populateTable(name, DATASET);
            }
        });

        // Create UDT
        String createUdtQuery = "CREATE TYPE " + TABLE_NAME_FOR_UDT_TEST.maybeQuotedKeyspace()
                                + ".\"UdT1\" (\"TimE\" bigint, \"limit\" int);";
        cluster.schemaChangeIgnoringStoppedInstances(createUdtQuery);

        createTestTable(TABLE_NAME_FOR_UDT_TEST, "CREATE TABLE IF NOT EXISTS %s (" +
                                                 "\"IdEnTiFiEr\" text, " +
                                                 "IdEnTiFiEr int, " +
                                                 "\"User_Defined_Type\" frozen<\"UdT1\">, " +
                                                 "PRIMARY KEY(\"IdEnTiFiEr\", IdEnTiFiEr));");
        populateTableWithUdt(TABLE_NAME_FOR_UDT_TEST, DATASET);
    }

    void populateTable(QualifiedName tableName, List<String> values)
    {
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            String query = String.format("INSERT INTO %s (\"IdEnTiFiEr\", IdEnTiFiEr) " +
                                         "VALUES ('%s', %d);", tableName, value, i);
            cluster.getFirstRunningInstance()
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
            cluster.getFirstRunningInstance()
                   .coordinator()
                   .execute(query, ConsistencyLevel.ALL);
        }
    }
}
