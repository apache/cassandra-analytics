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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkReaderSaiTypesTest extends BulkReaderSaiTestBase
{
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);

    private static final int MATCH_ID = 9001;
    private static final int BACKGROUND_ROWS = 256;
    private static final String PAYLOAD = payload(16 * 1024);

    static Stream<Arguments> simpleTypeSaiQueries()
    {
        return Stream.of(
        Arguments.of("int", "int_col = 777"),
        Arguments.of("float", "float_col = CAST(77.25 AS FLOAT)"),
        Arguments.of("text", "text_col = 'needle'"),
        Arguments.of("date", "date_col = DATE '2024-02-29'"),
        Arguments.of("inet", "inet_col = '8.8.8.8'"),
        Arguments.of("uuid", "uuid_col = '123e4567-e89b-12d3-a456-426614174000'")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("simpleTypeSaiQueries")
    void testSimpleCqlTypes(String type, String predicate)
    {
        List<Row> actual = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true"))
                           .load()
                           .filter(predicate)
                           .select("id", "payload")
                           .collectAsList();

        long saiReadBytes = bytesRead();

        // verify correctness
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getInt(0)).isEqualTo(MATCH_ID);

        // verify that we have read fewer bytes from data file
        assertThat(saiReadBytes)
        .as("SAI was not effective for %s; fullScanBytes=%s, saiReadBytes=%s", type, fullScanBytes, saiReadBytes)
        .isGreaterThan(0)
        .isLessThan(fullScanBytes / 2);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        String tableStmt = "CREATE TABLE IF NOT EXISTS %s ("
                           + "id int PRIMARY KEY, "
                           + "int_col int, "
                           + "float_col float, "
                           + "text_col text, "
                           + "date_col date, "
                           + "inet_col inet, "
                           + "uuid_col uuid, "
                           + "payload text"
                           + ") WITH compression = {'enabled': 'false'} "
                           + "AND compaction = {"
                           + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                           + "'enabled': 'false'"
                           + "}";

        createTestTable(TABLE, tableStmt);

        ICoordinator coordinator = cluster.get(1).coordinator();

        List<String> indexedColumns = Arrays.asList("int_col", "float_col", "text_col",
                                                    "date_col", "inet_col", "uuid_col");
        for (String column : indexedColumns)
        {
            coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS %s_sai ON %s (%s) USING 'sai'", column, TABLE, column),
                                ConsistencyLevel.ALL);
        }

        // lots of data which does not match search criteria
        for (int i = 0; i < BACKGROUND_ROWS; i++)
        {
            insert(coordinator, i, false);
        }

        // single row matches each of the eight predicates
        insert(coordinator, MATCH_ID, true);

        flushTable();
    }

    private static void insert(ICoordinator coordinator, int id, boolean matching)
    {
        String values;

        if (matching)
        {
            values = "777, 77.25, 'needle', '2024-02-29', '8.8.8.8', 123e4567-e89b-12d3-a456-426614174000";
        }
        else
        {
            values = "1, 1.25, 'haystack', '2023-01-02', '10.0.0.1', 00000000-0000-0000-0000-000000000001";
        }

        coordinator.execute(String.format("INSERT INTO %s (id, int_col, float_col, text_col, date_col, inet_col, uuid_col, payload) VALUES (%d, %s, '%s')",
                                          TABLE, id, values, PAYLOAD),
                            ConsistencyLevel.ALL);
    }

    @Override
    protected QualifiedName testTable()
    {
        return TABLE;
    }

    @Override
    protected int expectedRowCount()
    {
        return BACKGROUND_ROWS + 1;
    }
}
