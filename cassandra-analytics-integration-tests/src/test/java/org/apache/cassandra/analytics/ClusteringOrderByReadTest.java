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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the bulk reader correctly handles {@code WITH CLUSTERING ORDER BY (b DESC)} and does
 * not return duplicate rows when the same clustering key is written in a later SSTable — the
 * later write must overwrite on the merge path.
 */
class ClusteringOrderByReadTest extends SharedClusterSparkIntegrationTestBase
{
    static final int NUM_ROWS = 5;
    static final int NUM_COLS = 4;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "clust_order");
    Map<String, Long> expected = new HashMap<>();

    @Test
    void testClusteringOrderByNoDuplicates()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo(expected.size());

        for (Row row : data.collectAsList())
        {
            String key = row.getLong(0) + ":" + row.getLong(1);
            assertThat(expected).as("unexpected key %s", key).containsKey(key);
            assertThat(row.getLong(2)).isEqualTo(expected.get(key));
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint, " +
                               "PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC);");
        disableAutoCompaction(table);

        Random random = new Random(0);
        long partitionKey = 0;
        for (int r = 0; r < NUM_ROWS; r++)
        {
            for (long clusteringKey = 0; clusteringKey < NUM_COLS; clusteringKey++)
            {
                long value = random.nextInt(101);
                expected.put(partitionKey + ":" + clusteringKey, value);
                execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, %d, %d);",
                                      table, partitionKey, clusteringKey, value));
            }
            partitionKey++;
        }
        flushKeyspace(table);

        // rewrite smallest clustering key (0, 0) in a separate SSTable — would produce duplicates
        // if WITH CLUSTERING ORDER BY were not honored on the merge path
        long rewriteValue = random.nextInt(101);
        expected.put("0:0", rewriteValue);
        execute(String.format("INSERT INTO %s (a, b, c) VALUES (0, 0, %d);", table, rewriteValue));
        flushKeyspace(table);
    }
}
