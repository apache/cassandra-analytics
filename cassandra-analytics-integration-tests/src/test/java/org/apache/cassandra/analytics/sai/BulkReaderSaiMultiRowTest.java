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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end SAI test for partitions with multiple clustering rows.
 * SAI returns row-level primary keys, while the bulk reader collects partition tokens
 * and must still return every matching row from a selected partition.
 */
public class BulkReaderSaiMultiRowTest extends BulkReaderSaiTestBase
{
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);

    private static final int MATCHING_SCORE = 777;
    private static final int BACKGROUND_PARTITIONS = 256;
    private static final int MIXED_PARTITION = 1001;
    private static final int MATCHING_PARTITION = 1002;
    private static final int NON_MATCHING_PARTITION = 1003;
    private static final int NEW_PARTITION = 1004;

    private static final String PAYLOAD = payload(16 * 1024);

    @Test
    public void testMatchingAndNonMatchingClusteringRowsInSamePartition()
    {
        List<String> expected = expectedMatchingRows();
        List<String> actual = matchingRows("score = " + MATCHING_SCORE);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        assertIndexReducedReads();
    }

    @Test
    public void testMultipleClusteringRowsWithTwoSaiPredicates()
    {
        List<String> expected = expectedMatchingRows();
        // one indexed row has state='other'; it must be rejected by the intersection
        expected.remove(MATCHING_PARTITION + ":5");

        List<String> actual = matchingRows("score = " + MATCHING_SCORE + " AND state = 'selected'");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        assertIndexReducedReads();
    }

    private List<String> matchingRows(String predicate)
    {
        List<Row> rows = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true"))
                         .load()
                         .filter(predicate)
                         .collectAsList();

        return rows.stream()
                   .map(row -> row.getInt(0) + ":" + row.getInt(1))
                   .collect(Collectors.toList());
    }

    private void assertIndexReducedReads()
    {
        long saiReadBytes = bytesRead();
        // In addition to checking the exact rows, make sure the lookup did not
        // silently fall back to scanning every SSTable.
        assertThat(saiReadBytes)
        .as("SAI should avoid a full scan; fullScanBytes=%s saiReadBytes=%s", fullScanBytes, saiReadBytes)
        .isGreaterThan(0)
        .isLessThan(fullScanBytes / 2);
    }

    private static List<String> expectedMatchingRows()
    {
        List<String> expected = new ArrayList<>();
        // The first row in this partition was overwritten with a non-match,
        // and clustering row 1 was updated to become a match in SSTable #2.
        expected.add(MIXED_PARTITION + ":1");
        for (int ck = 2; ck < 24; ck += 2)
        {
            expected.add(MIXED_PARTITION + ":" + ck);
        }

        // Clustering row 0 was deleted in SSTable #2.
        for (int ck = 1; ck < 10; ck++)
        {
            expected.add(MATCHING_PARTITION + ":" + ck);
        }
        expected.add(NEW_PARTITION + ":0");
        return expected;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        String tableStmt = "CREATE TABLE IF NOT EXISTS %s ("
                           + "pk int, "
                           + "ck int, "
                           + "score int, "
                           + "state text, "
                           + "payload text, "
                           + "PRIMARY KEY (pk, ck)"
                           + ") WITH compression = {'enabled': 'false'} "
                           + "AND compaction = {"
                           + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                           + "'enabled': 'false'"
                           + "};";
        createTestTable(TABLE, tableStmt);

        ICoordinator coordinator = cluster.get(1).coordinator();
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS multirow_score_sai ON %s (score) USING 'sai'", TABLE),
                            ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS multirow_state_sai ON %s (state) USING 'sai'", TABLE),
                            ConsistencyLevel.ALL);

        for (int pk = 0; pk < BACKGROUND_PARTITIONS; pk++)
        {
            insert(coordinator, pk, 0, pk, "background");
        }

        // SSTable #1: multiple matching and non-matching rows share a token.
        for (int ck = 0; ck < 24; ck++)
        {
            insert(coordinator, MIXED_PARTITION, ck, ck % 2 == 0 ? MATCHING_SCORE : 1, "selected");
        }
        // Every row matches this partition, except the special state on ck=5
        // used by the two-index intersection test.
        for (int ck = 0; ck < 10; ck++)
        {
            insert(coordinator, MATCHING_PARTITION, ck, MATCHING_SCORE, ck == 5 ? "other" : "selected");
            insert(coordinator, NON_MATCHING_PARTITION, ck, 1, "selected");
        }
        flushTable();

        // SSTable #2: updates and a row deletion must win over older SAI hits.
        coordinator.execute(String.format("UPDATE %s SET score = 42 WHERE pk = %d AND ck = 0", TABLE, MIXED_PARTITION),
                            ConsistencyLevel.ALL);
        coordinator.execute(String.format("UPDATE %s SET score = %d WHERE pk = %d AND ck = 1",
                                          TABLE, MATCHING_SCORE, MIXED_PARTITION), ConsistencyLevel.ALL);
        coordinator.execute(String.format("DELETE FROM %s WHERE pk = %d AND ck = 0", TABLE, MATCHING_PARTITION),
                            ConsistencyLevel.ALL);
        insert(coordinator, NEW_PARTITION, 0, MATCHING_SCORE, "selected");
        insert(coordinator, NEW_PARTITION, 1, 1, "selected");
        flushTable();
    }

    private static void insert(ICoordinator coordinator, int id, int ck, int score, String state)
    {
        coordinator.execute(String.format("INSERT INTO %s (pk, ck, score, state, payload) "
                                          + "VALUES (%d, %d, %d, '%s', '%s')",
                                          TABLE, id, ck, score, state, PAYLOAD), ConsistencyLevel.ALL);
    }

    @Override
    protected QualifiedName testTable()
    {
        return TABLE;
    }

    @Override
    protected int expectedRowCount()
    {
        // one row in MATCHING_PARTITION was deleted in the second SSTable
        return BACKGROUND_PARTITIONS + 24 + 10 + 10 + 2 - 1;
    }
}
