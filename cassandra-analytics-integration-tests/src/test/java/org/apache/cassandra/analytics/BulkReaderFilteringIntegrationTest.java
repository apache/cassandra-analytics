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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.spark.data.ClientConfig.SSTABLE_END_TIMESTAMP_MICROS;
import static org.apache.cassandra.spark.data.ClientConfig.SSTABLE_START_TIMESTAMP_MICROS;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for various filters used during bulk reading.
 */
class BulkReaderFilteringIntegrationTest extends SharedClusterSparkIntegrationTestBase
{
    static final int DATA_SIZE = 1000;

    QualifiedName twcsTable = uniqueTestTableFullName(TEST_KEYSPACE);
    QualifiedName lcsTable = uniqueTestTableFullName(TEST_KEYSPACE);

    // Use base timestamp that's 10 minutes in the past
    static final long BASE_TIMESTAMP_MILLIS = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);

    // Separate each batch by 2 minutes to ensure they go into different TWCS windows (1 minute window size)
    static final long EARLY_TIMESTAMP_MICROS = TimeUnit.MILLISECONDS.toMicros(BASE_TIMESTAMP_MILLIS);
    static final long MIDDLE_TIMESTAMP_MICROS = TimeUnit.MILLISECONDS.toMicros(BASE_TIMESTAMP_MILLIS + TimeUnit.MINUTES.toMillis(2));
    static final long LATE_TIMESTAMP_MICROS = TimeUnit.MILLISECONDS.toMicros(BASE_TIMESTAMP_MILLIS + TimeUnit.MINUTES.toMillis(4));

    @Test
    void testReadAllDataWithoutTimeRangeFilter()
    {
        // Read all data without any time range filter
        Map<String, String> timeRangeOptions = Map.of();
        int expectedDataSize = DATA_SIZE * 3; // all 3 SSTables read
        Set<Long> expectedSSTableTimestamps = Set.of(EARLY_TIMESTAMP_MICROS, MIDDLE_TIMESTAMP_MICROS, LATE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, expectedDataSize, expectedSSTableTimestamps);
    }

    @Test
    void testTimeRangeFilterWithStartBoundInclusive()
    {
        // Read data starting MIDDLE_TIMESTAMP
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_START_TIMESTAMP_MICROS, Long.valueOf(MIDDLE_TIMESTAMP_MICROS).toString());
        int expectedDataSize = DATA_SIZE * 2; // 2 SSTables read
        Set<Long> expectedSSTableTimestamps = Set.of(MIDDLE_TIMESTAMP_MICROS, LATE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, expectedDataSize, expectedSSTableTimestamps);
    }

    @Test
    void testTimeRangeFilterWithStartBoundExclusive()
    {
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_START_TIMESTAMP_MICROS, Long.valueOf(LATE_TIMESTAMP_MICROS + 1).toString());
        Set<Long> expectedSSTableTimestamps = Set.of(LATE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, DATA_SIZE, expectedSSTableTimestamps); // 1 SSTables read
    }

    @Test
    void testTimeRangeFilterWithEndBoundInclusive()
    {
        // Read data ending with MIDDLE_TIMESTAMP inclusive
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(MIDDLE_TIMESTAMP_MICROS).toString());
        int expectedDataSize = DATA_SIZE * 2; // 2 SSTables read
        Set<Long> expectedSSTableTimestamps = Set.of(EARLY_TIMESTAMP_MICROS, MIDDLE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, expectedDataSize, expectedSSTableTimestamps);
    }

    @Test
    void testTimeRangeFilterWithEndBoundExclusive()
    {
        // Read data ending with MIDDLE_TIMESTAMP exclusive
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(MIDDLE_TIMESTAMP_MICROS - 1).toString());
        Set<Long> expectedSSTableTimestamps = Set.of(EARLY_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, DATA_SIZE, expectedSSTableTimestamps); // 1 SSTables read
    }

    @Test
    void testTimeRangeFilterWithStartAndEndBound()
    {
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_START_TIMESTAMP_MICROS, Long.valueOf(MIDDLE_TIMESTAMP_MICROS).toString(),
                                                      SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(LATE_TIMESTAMP_MICROS - 1).toString());
        Set<Long> expectedSSTableTimestamps = Set.of(MIDDLE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, DATA_SIZE, expectedSSTableTimestamps); // 1 SSTables read
    }

    @Test
    void testTimeRangeFilterWithStartAndEndBoundExclusive()
    {
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_START_TIMESTAMP_MICROS, Long.valueOf(EARLY_TIMESTAMP_MICROS + 1).toString(),
                                                      SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(LATE_TIMESTAMP_MICROS - 1).toString());
        int expectedDataSize = DATA_SIZE * 2; // 2 SSTables read
        Set<Long> expectedSSTableTimestamps = Set.of(EARLY_TIMESTAMP_MICROS, MIDDLE_TIMESTAMP_MICROS);
        runTimeRangeFilterTest(timeRangeOptions, expectedDataSize, expectedSSTableTimestamps);
    }

    @Test
    void testTimeRangeFilterNonOverlappingBound()
    {
        Map<String, String> timeRangeOptions = Map.of(SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(EARLY_TIMESTAMP_MICROS - 1).toString());
        Dataset<Row> data = bulkReaderDataFrame(twcsTable, timeRangeOptions).load();

        List<Row> rows = data.collectAsList();
        assertThat(rows.size()).isEqualTo(0); // no data read
    }

    @Test
    void testTimeRangeFilterWithoutTWCS()
    {
        // Attempt to use time range filter with non-TWCS table should throw exception
        Map<String, String> timeRangeOptions = Map.of(
            SSTABLE_START_TIMESTAMP_MICROS, Long.valueOf(EARLY_TIMESTAMP_MICROS).toString(),
            SSTABLE_END_TIMESTAMP_MICROS, Long.valueOf(LATE_TIMESTAMP_MICROS).toString()
        );

        assertThatThrownBy(() -> {
            Dataset<Row> data = bulkReaderDataFrame(lcsTable, timeRangeOptions).load();
            data.collectAsList();
        })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("SSTableTimeRangeFilter is only supported with TimeWindowCompactionStrategy. " +
                    "Current compaction strategy is: org.apache.cassandra.db.compaction.LeveledCompactionStrategy");
    }

    private void runTimeRangeFilterTest(Map<String, String> timeRangeOptions,
                                        int expectedDataSize,
                                        Set<Long> expectedTimestamps)
    {
        Dataset<Row> data = bulkReaderDataFrame(twcsTable, timeRangeOptions).load();

        List<Row> rows = data.collectAsList();
        assertThat(rows.size()).isEqualTo(expectedDataSize);

        Set<Long> allTimestamps = rows.stream()
                                      .map(row -> row.getLong(2))
                                      .collect(Collectors.toSet());

        assertThat(expectedTimestamps.size()).isEqualTo(allTimestamps.size());
        assertThat(expectedTimestamps).containsAll(allTimestamps);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        IInstance instance = cluster.getFirstRunningInstance();
        ICoordinator coordinator = instance.coordinator();

        // Initialize schema for SSTable time range filtering

        // Create table with TWCS compaction strategy with compaction window 1 minute
        createTestTable(twcsTable, "CREATE TABLE IF NOT EXISTS %s (" +
                                   "    id text PRIMARY KEY," +
                                   "    data text," +
                                   "    timestamp bigint" +
                                   ") WITH compaction = {" +
                                   "    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'," +
                                   "    'compaction_window_size': '1'," +
                                   "    'compaction_window_unit': 'MINUTES'" +
                                   "};");

        createTestTable(lcsTable, "CREATE TABLE IF NOT EXISTS %s (" +
                                  "    id text PRIMARY KEY," +
                                  "    data text" +
                                  ") WITH compaction = {" +
                                  "    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'" +
                                  "};");
        for (int i = 0; i < 10; i++)
        {
            String query = String.format("INSERT INTO %s (id, data) VALUES ('%s', 'data_%s')", lcsTable, i, "data" + i);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }
        instance.nodetool("flush", TEST_KEYSPACE, lcsTable.table());

        // create 3 SSTables in 3 time windows, each SSTable created 2 mins apart
        // Insert early data with early timestamps
        for (int i = 0; i < DATA_SIZE; i++)
        {
            long timestamp = EARLY_TIMESTAMP_MICROS + i;
            String query = String.format("INSERT INTO %s (id, data, timestamp) VALUES ('%s', 'data_%s', %d) USING TIMESTAMP %d",
                                         twcsTable, i, "data" + i, EARLY_TIMESTAMP_MICROS, timestamp);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }

        // Flush to create first SSTable
        instance.nodetool("flush", TEST_KEYSPACE, twcsTable.table());

        // Insert middle data with middle timestamps
        for (int i = 0; i < DATA_SIZE; i++)
        {
            int id = DATA_SIZE + i;
            long timestamp = MIDDLE_TIMESTAMP_MICROS + i;
            String query = String.format("INSERT INTO %s (id, data, timestamp) VALUES ('%s', 'data_%s', %d) USING TIMESTAMP %d",
                                         twcsTable, id, "data" + id, MIDDLE_TIMESTAMP_MICROS, timestamp);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }

        // Flush to create second SSTable
        instance.nodetool("flush", TEST_KEYSPACE, twcsTable.table());

        // Insert late data with late timestamps
        for (int i = 0; i < DATA_SIZE; i++)
        {
            int id = DATA_SIZE * 2 + i;
            long timestamp = LATE_TIMESTAMP_MICROS + i;
            String query = String.format("INSERT INTO %s (id, data, timestamp) VALUES ('%s', 'data_%s', %d) USING TIMESTAMP %d",
                                         twcsTable, id, "data" + id, LATE_TIMESTAMP_MICROS, timestamp);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }

        // Flush to create third SSTable
        instance.nodetool("flush", TEST_KEYSPACE, twcsTable.table());
    }
}
