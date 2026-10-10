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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the bulk reader on a nested-collection column —
 * {@code map<int, frozen<list<bigint>>>}.
 */
class NestedCollectionsReadTest extends SharedClusterSparkIntegrationTestBase
{
    static final int NUM_SSTABLES = 5;
    static final int NUM_ROWS = 50;
    static final int MAP_ENTRIES = 5;
    static final int LIST_ENTRIES = 10;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "nested");
    Map<Long, Map<Integer, List<Long>>> expected = new HashMap<>();

    @Test
    void testNestedRead()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo(expected.size());

        for (Row row : data.collectAsList())
        {
            long key = row.getLong(0);
            Map<Integer, List<Long>> expectedRow = expected.get(key);
            assertThat(expectedRow).as("Unexpected key in Spark output: %s", key).isNotNull();

            Map<Object, Object> actualOuter = JavaConverters.mapAsJavaMapConverter(row.getMap(1)).asJava();
            assertThat(actualOuter).hasSize(expectedRow.size());

            for (Map.Entry<Object, Object> outerEntry : actualOuter.entrySet())
            {
                int outerKey = ((Number) outerEntry.getKey()).intValue();
                assertThat(expectedRow).containsKey(outerKey);

                @SuppressWarnings("unchecked")
                scala.collection.Seq<Object> innerSeq = (scala.collection.Seq<Object>) outerEntry.getValue();
                List<Long> actualInner = JavaConverters.seqAsJavaListConverter(innerSeq).asJava().stream()
                                                       .map(v -> ((Number) v).longValue())
                                                       .collect(Collectors.toList());
                assertThat(actualInner).isEqualTo(expectedRow.get(outerKey));
            }
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b map<int, frozen<list<bigint>>>, "
                               + "PRIMARY KEY (a));");
        disableAutoCompaction(table);

        Random random = new Random(0);
        for (int s = 0; s < NUM_SSTABLES; s++)
        {
            for (long partitionKey = 0; partitionKey < NUM_ROWS; partitionKey++)
            {
                Map<Integer, List<Long>> nested = new LinkedHashMap<>();
                StringBuilder mapCql = new StringBuilder("{");
                Set<Integer> usedOuterKeys = new HashSet<>();
                int mapIdx = 0;
                for (int o = 0; o < MAP_ENTRIES; o++)
                {
                    int outerKey;
                    do
                    {
                        outerKey = random.nextInt(100_000);
                    } while (!usedOuterKeys.add(outerKey));

                    List<Long> inner = new ArrayList<>(LIST_ENTRIES);
                    StringBuilder listCql = new StringBuilder("[");
                    for (int i = 0; i < LIST_ENTRIES; i++)
                    {
                        long value = Math.abs(random.nextLong()) % 100_000_000L;
                        inner.add(value);
                        if (i > 0)
                        {
                            listCql.append(",");
                        }
                        listCql.append(value);
                    }
                    listCql.append("]");

                    nested.put(outerKey, inner);
                    if (mapIdx++ > 0)
                    {
                        mapCql.append(",");
                    }
                    mapCql.append(outerKey).append(":").append(listCql);
                }
                mapCql.append("}");

                execute(String.format("INSERT INTO %s (a, b) VALUES (%d, %s);", table, partitionKey, mapCql));
                expected.put(partitionKey, nested);
            }
            flushKeyspace(table);
        }
    }
}
