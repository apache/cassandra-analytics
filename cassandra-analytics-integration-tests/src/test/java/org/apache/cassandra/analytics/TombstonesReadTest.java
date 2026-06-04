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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the bulk reader correctly filters tombstoned rows in four scenarios — basic
 * (no deletes, baseline), partition tombstones, row tombstones, and range tombstones. Each
 * test owns a distinct table.
 *
 * <p>The basic scenario writes {@code num_rows} inserts at sparse random partition/clustering
 * keys (collisions are statistically negligible at the [0, 1e8] range). The partition/row/range
 * tombstone scenarios use dense sequential keys to keep the delete-by-specific-clustering-key
 * scenarios deterministic.
 */
class TombstonesReadTest extends SharedClusterSparkIntegrationTestBase
{
    static final int NUM_ROWS = 100;
    static final int NUM_COLS = 10;

    QualifiedName basicTable = uniqueTestTableFullName(TEST_KEYSPACE, "tomb_basic");
    QualifiedName partitionTombstoneTable = uniqueTestTableFullName(TEST_KEYSPACE, "tomb_part");
    QualifiedName rowTombstoneTable = uniqueTestTableFullName(TEST_KEYSPACE, "tomb_row");
    QualifiedName rangeTombstoneTable = uniqueTestTableFullName(TEST_KEYSPACE, "tomb_range");

    Map<String, Long> basicValues = new HashMap<>();
    Map<String, Long> partitionValues = new HashMap<>();
    Map<String, Long> rowValues = new HashMap<>();
    Map<String, Long> rangeValues = new HashMap<>();

    @Test
    void testBasic()
    {
        assertRows(basicTable, basicValues);
    }

    @Test
    void testPartitionTombstones()
    {
        assertRows(partitionTombstoneTable, partitionValues);
    }

    @Test
    void testRowTombstones()
    {
        assertRows(rowTombstoneTable, rowValues);
    }

    @Test
    void testRangeTombstones()
    {
        assertRows(rangeTombstoneTable, rangeValues);
    }

    private void assertRows(QualifiedName table, Map<String, Long> expected)
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo(expected.size());
        for (Row row : data.collectAsList())
        {
            String key = row.getLong(0) + ":" + row.getLong(1);
            assertThat(expected).as("unexpected key %s in table %s", key, table).containsKey(key);
            assertThat(row.getLong(2)).isEqualTo(expected.get(key));
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        String ddl = "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint, PRIMARY KEY (a, b));";
        createTestTable(basicTable, ddl);
        createTestTable(partitionTombstoneTable, ddl);
        createTestTable(rowTombstoneTable, ddl);
        createTestTable(rangeTombstoneTable, ddl);
        disableAutoCompaction(basicTable);

        Random random = new Random(0);
        AtomicInteger seedCounter = new AtomicInteger();

        // Basic: sparse random partition+clustering keys in a single loop. NUM_ROWS * NUM_COLS
        // chosen to match the volume of the other scenarios.
        for (int i = 0; i < NUM_ROWS * NUM_COLS; i++)
        {
            long partitionKey = Math.abs(random.nextLong()) % 100_000_000L;
            long clusteringKey = Math.abs(random.nextLong()) % 100_000_000L;
            long value = Math.abs(random.nextLong()) % 100_000_000L;
            execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, %d, %d);",
                                  basicTable, partitionKey, clusteringKey, value));
            basicValues.put(partitionKey + ":" + clusteringKey, value);
        }
        flushKeyspace(basicTable);

        // Partition tombstones: delete partitions [0, 25) — 25 of 100 partitions
        populate(partitionTombstoneTable, partitionValues, new Random(seedCounter.incrementAndGet()));
        flushKeyspace(partitionTombstoneTable);
        int numPartitionDeletes = 25;
        for (long partitionKey = 0; partitionKey < numPartitionDeletes; partitionKey++)
        {
            execute(String.format("DELETE FROM %s WHERE a = %d;", partitionTombstoneTable, partitionKey));
            for (long clusteringKey = 0; clusteringKey < NUM_COLS; clusteringKey++)
            {
                partitionValues.remove(partitionKey + ":" + clusteringKey);
            }
        }
        flushKeyspace(partitionTombstoneTable);

        // Row tombstones: delete b=1 from every partition
        populate(rowTombstoneTable, rowValues, new Random(seedCounter.incrementAndGet()));
        flushKeyspace(rowTombstoneTable);
        for (long partitionKey = 0; partitionKey < NUM_ROWS; partitionKey++)
        {
            execute(String.format("DELETE FROM %s WHERE a = %d AND b = 1;", rowTombstoneTable, partitionKey));
            rowValues.remove(partitionKey + ":1");
        }
        flushKeyspace(rowTombstoneTable);

        // Range tombstones: delete b in [r1, r2) for a deterministic range per partition
        populate(rangeTombstoneTable, rangeValues, new Random(seedCounter.incrementAndGet()));
        flushKeyspace(rangeTombstoneTable);
        Random rangeRand = new Random(99);
        for (long partitionKey = 0; partitionKey < NUM_ROWS; partitionKey++)
        {
            int r1 = rangeRand.nextInt(NUM_COLS);
            int r2 = rangeRand.nextInt(NUM_COLS);
            if (r1 > r2)
            {
                int tmp = r1;
                r1 = r2;
                r2 = tmp;
            }

            if (r1 == r2)
            {
                r2 = Math.min(r1 + 1, NUM_COLS);
            }

            execute(String.format("DELETE FROM %s WHERE a = %d AND b >= %d AND b < %d;",
                                  rangeTombstoneTable, partitionKey, r1, r2));
            for (int clusteringKey = r1; clusteringKey < r2; clusteringKey++)
            {
                rangeValues.remove(partitionKey + ":" + clusteringKey);
            }
        }
        flushKeyspace(rangeTombstoneTable);
    }

    private void populate(QualifiedName table, Map<String, Long> out, Random random)
    {
        for (long partitionKey = 0; partitionKey < NUM_ROWS; partitionKey++)
        {
            for (long clusteringKey = 0; clusteringKey < NUM_COLS; clusteringKey++)
            {
                long value = Math.abs(random.nextLong()) % 100_000_000L;
                execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, %d, %d);",
                                      table, partitionKey, clusteringKey, value));
                out.put(partitionKey + ":" + clusteringKey, value);
            }
        }
    }
}
