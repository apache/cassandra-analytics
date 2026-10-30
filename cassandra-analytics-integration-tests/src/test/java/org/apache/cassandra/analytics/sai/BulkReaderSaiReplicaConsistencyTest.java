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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for replica-local SAI intersections with disjoint partial updates (CASSANDRA-19018).
 * Two replicas have complementary cells in the same row: neither replica alone satisfies a = 1 AND b = 2,
 * but their reconciliation does.
 */
public class BulkReaderSaiReplicaConsistencyTest extends BulkReaderSaiTestBase
{
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);

    private static final int MATCH_ID = 1001;
    private static final int BACKGROUND_ROWS = 256;

    private static final String PAYLOAD = payload(8 * 1024);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().nodesPerDc(2);
    }

    @Override
    protected DataFrameReader bulkReaderDataFrame(QualifiedName table, Map<String, String> options)
    {
        return super.bulkReaderDataFrame(table, options)
                    .option("consistencyLevel", ConsistencyLevel.ALL.name());
    }

    @Test
    void testConjunctionOfPartialUpdatesOnDifferentReplicas()
    {
        // the full scan must return the reconciled row
        List<Row> expected = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "false"))
                             .load()
                             .filter("a = 1 AND b = 2")
                             .collectAsList();

        assertThat(expected).hasSize(1);
        assertThat(expected.get(0).getInt(0)).isEqualTo(MATCH_ID);
        assertThat(expected.get(0).getInt(1)).isEqualTo(1); // a
        assertThat(expected.get(0).getInt(2)).isEqualTo(2); // b

        long bytesBeforeIndexedRead = bytesRead();

        // repeat with SAI candidate pruning enabled
        List<Row> matches = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true"))
                            .load()
                            .filter("a = 1 AND b = 2")
                            .collectAsList();

        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getInt(0)).isEqualTo(MATCH_ID);
        assertThat(matches.get(0).getInt(1)).isEqualTo(1); // a
        assertThat(matches.get(0).getInt(2)).isEqualTo(2); // b

        // Detect silent fallback to full scanning.
        long saiReadBytes = bytesRead() - bytesBeforeIndexedRead;

        assertThat(saiReadBytes)
        .as("SAI must prune candidates instead of falling back to a full scan; fullScanBytes=%s, saiReadBytes=%s", fullScanBytes, saiReadBytes)
        .isGreaterThan(0)
        .isLessThan(fullScanBytes / 2);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, Map.of("datacenter1", 2));

        createTestTable(TABLE, "CREATE TABLE IF NOT EXISTS %s ("
                               + "id int PRIMARY KEY, "
                               + "a int, "
                               + "b int, "
                               + "payload text) "
                               + "WITH read_repair = 'NONE' "
                               + "AND compression = {'enabled': 'false'} "
                               + "AND compaction = {"
                               + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                               + "'enabled': 'false'};");

        ICoordinator coordinator = cluster.get(1).coordinator();

        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS replica_a_sai ON %s (a) USING 'sai'", TABLE), ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS replica_b_sai ON %s (b) USING 'sai'", TABLE), ConsistencyLevel.ALL);

        for (int id = 0; id < BACKGROUND_ROWS; id++)
        {
            coordinator.execute(String.format("INSERT INTO %s (id, a, b, payload) VALUES (%d, %d, %d, '%s')",
                                              TABLE, id, id + 10, id + 20, PAYLOAD),
            ConsistencyLevel.ALL);
        }

        // Execute local updates, by-passing normal replication.
        // Replica 1: id=1001, a=1,    b=null
        // Replica 2: id=1001, a=null, b=2
        // Neither replica independently matches a=1 AND b=2.
        cluster.get(1).executeInternal(String.format("UPDATE %s SET a = 1 WHERE id = %d", TABLE, MATCH_ID));
        cluster.get(2).executeInternal(String.format("UPDATE %s SET b = 2 WHERE id = %d", TABLE, MATCH_ID));

        // verify divergence before any distributed read
        assertLocalCells(1, 1, null);
        assertLocalCells(2, null, 2);

        // flush both replicas and their SAI index data
        flushTable();
    }

    private void assertLocalCells(int node, Integer expectedA, Integer expectedB)
    {
        Object[][] rows = cluster.get(node).executeInternal(String.format("SELECT a, b FROM %s WHERE id = %d", TABLE, MATCH_ID));
        assertThat(rows).hasNumberOfRows(1);
        assertThat(rows[0][0]).isEqualTo(expectedA);
        assertThat(rows[0][1]).isEqualTo(expectedB);
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
