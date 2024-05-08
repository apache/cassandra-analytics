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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk reader functionality
 */
class BulkReaderTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);
    QualifiedName table2 = uniqueTestTableFullName(TEST_KEYSPACE);
    QualifiedName tableForNullStaticColumn = uniqueTestTableFullName(TEST_KEYSPACE);

    @Test
    void testReadNullStaticColumn()
    {
        Dataset<Row> data = bulkReaderDataFrame(tableForNullStaticColumn).load();

        List<Row> rows = data.collectAsList().stream()
                             .sorted(Comparator.comparing(row -> row.getString(0)))
                             .collect(Collectors.toList());
        assertThat(rows.size()).isEqualTo(DATASET.size());

        for (int i = 0; i < DATASET.size(); i++)
        {
            Row row = rows.get(i);
            assertThat(row.getString(0)).isEqualTo(DATASET.get(i));
            assertThat(row.getTimestamp(1).getTime()).isEqualTo(1432815430948560L + i);
            if (i % 2 == 0)
            {
                assertThat(row.getTimestamp(2)).as("Row " + (i + 1) + " is expected to have null timestamp").isNull();
            }
            else
            {
                assertThat(row.getTimestamp(2).getTime()).isEqualTo(1432815430948560L + i);
            }
        }
    }

    @Test
    void testReadingFromTwoDifferentTables()
    {
        Dataset<Row> dataForTable1 = bulkReaderDataFrame(table1).load();
        Dataset<Row> dataForTable2 = bulkReaderDataFrame(table2).load();

        assertThat(dataForTable1.count()).isEqualTo(DATASET.size());
        assertThat(dataForTable2.count()).isEqualTo(DATASET.size());

        List<Row> rowList1 = dataForTable1.collectAsList().stream()
                                          .sorted(Comparator.comparing(row -> row.getInt(0)))
                                          .collect(Collectors.toList());

        List<Row> rowList2 = dataForTable2.collectAsList().stream()
                                          .sorted(Comparator.comparing(row -> row.getLong(1)))
                                          .collect(Collectors.toList());

        for (int i = 0; i < DATASET.size(); i++)
        {
            assertThat(rowList1.get(i).getInt(0)).isEqualTo(i);
            assertThat(rowList1.get(i).getString(1)).isEqualTo(DATASET.get(i));
            assertThat(rowList2.get(i).getString(0)).isEqualTo(DATASET.get(i));
            assertThat(rowList2.get(i).getLong(1)).isEqualTo(i);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");
        createTestTable(table2, "CREATE TABLE IF NOT EXISTS %s (name text PRIMARY KEY, value bigint);");
        createTestTable(tableForNullStaticColumn, "CREATE TABLE %s (id text, timestamp timestamp,\n" +
                                                  "   timestamp_static timestamp static, PRIMARY KEY (id, timestamp));");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < DATASET.size(); i++)
        {
            String value = DATASET.get(i);
            String query1 = String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');", table1, i, value);
            String query2 = String.format("INSERT INTO %s (name, value) VALUES ('%s', %d);", table2, value, i);
            String query3 = String.format("INSERT INTO %s (id, timestamp, timestamp_static) VALUES ('%s',%d, %s)",
                                          tableForNullStaticColumn, value, 1432815430948560L + i,
                                          i % 2 == 0 ? "null" : String.valueOf(i + 1432815430948560L));

            firstRunningInstance.coordinator().execute(query1, ConsistencyLevel.ALL);
            firstRunningInstance.coordinator().execute(query2, ConsistencyLevel.ALL);
            firstRunningInstance.coordinator().execute(query3, ConsistencyLevel.ALL);
        }
    }
}
