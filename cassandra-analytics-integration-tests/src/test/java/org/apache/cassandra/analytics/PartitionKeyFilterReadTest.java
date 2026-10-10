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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies partition-key pushdown on the bulk reader, both single-value ({@code a = N}) and
 * multi-value ({@code a IN (...)}) forms.
 *
 * <p>The bulk reader's {@code CassandraDataSource} accepts partition-key predicates as
 * standard Spark filters; this test uses {@code Dataset#filter} to express them.
 */
class PartitionKeyFilterReadTest extends SharedClusterSparkIntegrationTestBase
{
    static final int NUM_SSTABLES = 5;
    static final int NUM_ROWS = 5;
    static final int NUM_COLS = 4;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "key_filter");
    Map<String, Long> allRows = new HashMap<>();

    @Test
    void testSinglePartitionKeyFilter()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load().filter(functions.col("a").equalTo(1L));
        Map<String, Long> expected = filterByKeys(Set.of(1L));
        assertRowsMatch(data, expected);
    }

    @Test
    void testMultiplePartitionKeyFilter()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load().filter(functions.col("a").isin(1L, 2L, 3L));
        Map<String, Long> expected = filterByKeys(Set.of(1L, 2L, 3L));
        assertRowsMatch(data, expected);
    }

    private Map<String, Long> filterByKeys(Set<Long> keys)
    {
        return allRows.entrySet().stream()
                      .filter(e -> keys.contains(Long.parseLong(e.getKey().split(":")[0])))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void assertRowsMatch(Dataset<Row> data, Map<String, Long> expected)
    {
        List<Row> rows = data.collectAsList();
        assertThat(rows).hasSize(expected.size());
        for (Row row : rows)
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
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint, PRIMARY KEY (a, b));");
        disableAutoCompaction(table);

        Random random = new Random(0);
        long partitionKey = 0;
        for (int s = 0; s < NUM_SSTABLES; s++)
        {
            for (int r = 0; r < NUM_ROWS; r++)
            {
                for (long clusteringKey = 0; clusteringKey < NUM_COLS; clusteringKey++)
                {
                    long value = random.nextInt(101);
                    allRows.put(partitionKey + ":" + clusteringKey, value);
                    execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, %d, %d);",
                                          table, partitionKey, clusteringKey, value));
                }
                partitionKey++;
            }
            flushKeyspace(table);
        }
    }
}
