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
package org.apache.cassandra.analytics.sai;

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

public class BulkReaderSaiCorrectnessTest extends BulkReaderSaiTestBase
{
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);

    private static final int MATCHING_SCORE = 777;
    private static final int KEEP_ID = 1001;
    private static final int DELETE_ID = 1002;
    private static final int CHANGE_ID = 1003;
    private static final int NEW_MATCH_ID = 1004;

    private static final int BACKGROUND_ROWS = 256;
    private static final String PAYLOAD = payload(16 * 1024);

    static Stream<Arguments> queryInputs()
    {
        return Stream.of(
        Arguments.of("eq", TABLE, "score = " + MATCHING_SCORE, 2, 0.18),
        Arguments.of("gt", TABLE, "score > 250", 8, 0.2),
        Arguments.of("gte", TABLE, "score >= 250", 9, 0.2),
        Arguments.of("lt", TABLE, "score < 3", 3, 0.2),
        Arguments.of("lte", TABLE, "score <= 3", 4, 0.2),
        Arguments.of("between", TABLE, "score > 250 AND score < 300", 5, 0.1),
        Arguments.of("or", TABLE, "score <= 2 OR score > 300", 3 + 3, 1), // OR expressions do not use SAI index
        Arguments.of("multi_column", TABLE, "score = 778 AND state = 'changed'", 1, 0.05),
        Arguments.of("overridden", TABLE, "score = 777 AND state = 'change-score'", 0, 0.05) // not most recent value
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queryInputs")
    void testIndexUsage(String view, QualifiedName table, String whereClause, int expectedRows, double maxDataFileReadPercentage)
    {
        // Alternative pushdown filter with Spark SQL context:
        // Dataset<Row> input = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true")).load();
        // input.createOrReplaceTempView("sai_reader_" + view);
        // List<Row> matches = getOrCreateSparkSession().sql("SELECT id, score, state, payload "
        //                                                   + "FROM sai_reader_" + view
        //                                                   + " WHERE " + whereClause)
        //                                              .collectAsList();

        List<Row> matches = bulkReaderDataFrame(table, Map.of("saiFilteringEnabled", "true"))
                            .load()
                            .filter(whereClause)
                            .collectAsList();
        assertThat(matches).hasSize(expectedRows);

        long saiReadBytes = bytesRead();
        // Prove we did not simply fall back to a full scan.
        assertThat(saiReadBytes).as("SAI predicate should substantially reduce Data.db reads; fullScanBytes=%s saiReadBytes=%s", fullScanBytes, saiReadBytes)
                                .isGreaterThan(0)
                                .isLessThanOrEqualTo((int) (fullScanBytes * maxDataFileReadPercentage));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        String tableStmt = "CREATE TABLE IF NOT EXISTS %s ("
                           + "id int PRIMARY KEY, "
                           + "score int, "
                           + "state text, "
                           + "payload text"
                           + ") WITH compression = {'enabled': 'false'} "
                           + "AND compaction = {"
                           + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                           + "'enabled': 'false'"
                           + "};";

        createTestTable(TABLE, tableStmt);

        ICoordinator coordinator = cluster.get(1).coordinator();

        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS score_sai ON %s (score) USING 'sai';", TABLE), ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS state_sai ON %s (state) USING 'sai';", TABLE), ConsistencyLevel.ALL);

        // Lots of data which does not match score=777. The relatively
        // large payload gives us a robust full-scan I/O baseline.
        for (int i = 0; i < BACKGROUND_ROWS; i++)
        {
            insert(coordinator, i, i, "background");
        }

        // All three are SAI matches in SSTable #1.
        insert(coordinator, KEEP_ID, MATCHING_SCORE, "original");
        insert(coordinator, DELETE_ID, MATCHING_SCORE, "delete-me");
        insert(coordinator, CHANGE_ID, MATCHING_SCORE, "change-score");

        flushTable();

        // SSTable #2:
        //
        // KEEP_ID:
        //   Candidate comes from the older SAI index, but the newer
        //   non-indexed column value must win during reconciliation.
        coordinator.execute(String.format("UPDATE %s SET state = 'updated' WHERE id = %d", TABLE, KEEP_ID), ConsistencyLevel.ALL);

        // DELETE_ID:
        // Older SAI has a hit, newer SSTable contains a tombstone.
        coordinator.execute(String.format("DELETE FROM %s WHERE id = %d", TABLE, DELETE_ID), ConsistencyLevel.ALL);

        // CHANGE_ID:
        // Older SAI has score=777. New value is 778.
        // Spark's residual predicate must filter this partition.
        coordinator.execute(String.format("UPDATE %s SET score = 778, state = 'changed' WHERE id = %d", TABLE, CHANGE_ID), ConsistencyLevel.ALL);

        // Match existing only in the second SSTable.
        insert(coordinator, NEW_MATCH_ID, MATCHING_SCORE, "new-match");

        flushTable();
    }

    private static void insert(ICoordinator coordinator, int id, int score, String state)
    {
        coordinator.execute(String.format("INSERT INTO %s (id, score, state, payload) VALUES (%d, %d, '%s', '%s')",
                                          TABLE, id, score, state, PAYLOAD),
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
        return BACKGROUND_ROWS + 3;
    }
}
