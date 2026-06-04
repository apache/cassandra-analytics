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
 * Verifies the bulk reader correctly surfaces the customer-facing STATIC column semantic — a
 * static cell is partition-scoped, so every clustering row of the partition reads back the same
 * static value, and last-write-wins applies across the partition's static cell.
 *
 * <p>This test writes a fresh random static on every insert; the read-side expected value is
 * therefore the <em>last</em> static written for each partition. Cassandra's LWW resolution on
 * the static cell is what surfaces that value on read; if LWW for statics regressed, every
 * clustering row of the partition would still report the same static (because static cells are
 * partition-scoped), but it would no longer equal the last-written value.
 *
 * <p>{@code BulkReaderTest.testReadNullStaticColumn} exercises STATIC schema and null-handling
 * but uses one clustering row per partition, so the shared-across-clustering-rows behaviour
 * isn't exercised there. This test fills that gap by writing multiple clustering rows per
 * partition and asserting all of them read back the partition's last-written static value.
 */
class StaticColumnSharedValueReadTest extends SharedClusterSparkIntegrationTestBase
{
    static final int NUM_PARTITIONS = 10;
    static final int NUM_CLUSTERING_ROWS = 20;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "static_shared");

    /** partition key -> the static value every clustering row of that partition must read back */
    Map<Long, Long> expectedStaticByPartition = new HashMap<>();
    /** (partition, clustering) -> the per-row regular column value */
    Map<String, Long> expectedRegularValue = new HashMap<>();

    @Test
    void testStaticValueSharedAcrossClusteringRows()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo((long) NUM_PARTITIONS * NUM_CLUSTERING_ROWS);

        // Sanity: every clustering row of partition P must report the same static value.
        Map<Long, Long> staticPerPartitionFromRead = new HashMap<>();
        for (Row row : data.collectAsList())
        {
            long partitionKey = row.getLong(0);
            long clusteringKey = row.getLong(1);
            long staticValue = row.getLong(2);
            long regularValue = row.getLong(3);

            assertThat(expectedStaticByPartition).as("unknown partition %d", partitionKey).containsKey(partitionKey);
            assertThat(staticValue)
                .as("partition %d clustering %d: STATIC value mismatch", partitionKey, clusteringKey)
                .isEqualTo(expectedStaticByPartition.get(partitionKey));

            String key = partitionKey + ":" + clusteringKey;
            assertThat(expectedRegularValue).containsKey(key);
            assertThat(regularValue).isEqualTo(expectedRegularValue.get(key));

            Long previouslySeen = staticPerPartitionFromRead.put(partitionKey, staticValue);
            if (previouslySeen != null)
            {
                assertThat(staticValue)
                    .as("partition %d returned divergent static values across clustering rows", partitionKey)
                    .isEqualTo(previouslySeen);
            }
        }

        // Every partition must have been seen at least once on the read side.
        assertThat(staticPerPartitionFromRead.keySet()).isEqualTo(expectedStaticByPartition.keySet());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint static, d bigint, "
                               + "PRIMARY KEY (a, b));");
        disableAutoCompaction(table);

        // Pick a fresh random static on every insert; the partition's expected static is the
        // LAST value written (Cassandra resolves the static cell as LWW).
        // Seeded RNG for reproducibility on failure.
        Random random = new Random(0);
        for (long partitionKey = 0; partitionKey < NUM_PARTITIONS; partitionKey++)
        {
            for (long clusteringKey = 0; clusteringKey < NUM_CLUSTERING_ROWS; clusteringKey++)
            {
                long staticValue = Math.abs(random.nextLong()) % 100_000_000L;
                long regularValue = partitionKey * 1000 + clusteringKey;
                expectedRegularValue.put(partitionKey + ":" + clusteringKey, regularValue);
                expectedStaticByPartition.put(partitionKey, staticValue);
                execute(String.format("INSERT INTO %s (a, b, c, d) VALUES (%d, %d, %d, %d);",
                                      table, partitionKey, clusteringKey, staticValue, regularValue));
            }
        }
        flushKeyspace(table);
    }
}
