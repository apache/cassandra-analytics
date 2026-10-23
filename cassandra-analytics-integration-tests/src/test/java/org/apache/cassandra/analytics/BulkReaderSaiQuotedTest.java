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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkReaderSaiQuotedTest extends BulkReaderSaiTestBase
{
    private static final QualifiedName TABLE = new QualifiedName(TEST_KEYSPACE, "TestQuotedTable", false, true);

    private static final int BACKGROUND_ROWS = 256;
    private static final String PAYLOAD = payload(16 * 1024);

    @Test
    void testIndexOnQuotedColumn()
    {
        List<Row> matches = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true"))
                            .load()
                            .filter("Score > 250")
                            .collectAsList();
        assertThat(matches).hasSize(5);

        long saiReadBytes = bytesRead();
        // Prove we did not simply fall back to a full scan.
        assertThat(saiReadBytes).as("SAI predicate should substantially reduce Data.db reads; fullScanBytes=%s saiReadBytes=%s", fullScanBytes, saiReadBytes)
                                .isGreaterThan(0)
                                .isLessThanOrEqualTo((int) (fullScanBytes * 0.4));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        String tableStmt = "CREATE TABLE IF NOT EXISTS %s ("
                           + "id int PRIMARY KEY, "
                           + "\"Score\" int, "
                           + "\"State\" text, "
                           + "payload text"
                           + ") WITH compression = {'enabled': 'false'} "
                           + "AND compaction = {"
                           + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                           + "'enabled': 'false'"
                           + "};";

        createTestTable(TABLE, tableStmt);

        ICoordinator coordinator = cluster.get(1).coordinator();

        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS \"Score_quoted_sai\" ON %s (\"Score\") USING 'sai';",
                                          TABLE), ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS \"State_quoted_sai\" ON %s (\"State\") USING 'sai';",
                                          TABLE), ConsistencyLevel.ALL);

        for (int i = 0; i < BACKGROUND_ROWS; i++)
        {
            insert(coordinator, i, i);
        }

        flushTable();
    }

    private static void insert(ICoordinator coordinator, int id, int score)
    {
        coordinator.execute(String.format("INSERT INTO %s (id, \"Score\", \"State\", payload) VALUES (%d, %d, 'background', '%s')",
                                          TABLE, id, score, PAYLOAD),
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
        return BACKGROUND_ROWS;
    }
}
