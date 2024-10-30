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

package org.apache.cassandra.db.commitlog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.CdcTester;
import org.apache.cassandra.cdc.CdcTests;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaBuilder;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.cdc.CdcTests.BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.CDC_BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.directory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BufferingCommitLogReaderTests
{
    static
    {
        CdcTests.setup();
    }

    @Test
    public void testReaderSeek()
    {
        TestSchema schema = TestSchema.builder(BRIDGE)
                                      .withPartitionKey("pk", BRIDGE.bigint())
                                      .withColumn("c1", BRIDGE.bigint())
                                      .withColumn("c2", BRIDGE.bigint())
                                      .withCdc(true)
                                      .build();
        CqlTable cqlTable = schema.buildTable();
        new SchemaBuilder(cqlTable, Partitioner.Murmur3Partitioner, true); // init Schema instance
        int numRows = 1000;

        // write some rows to a CommitLog
        Set<Long> keys = new HashSet<>(numRows);
        for (int i = 0; i < numRows; i++)
        {
            TestSchema.TestRow row = schema.randomRow();
            while (keys.contains(row.getLong("pk")))
            {
                row = schema.randomRow();
            }
            keys.add(row.getLong("pk"));
            CDC_BRIDGE.log(TimeProvider.DEFAULT, cqlTable, CdcTester.testCommitLog, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        CdcTester.testCommitLog.sync();

        List<Marker> markers = Collections.synchronizedList(new ArrayList<>());
        CommitLog firstLog = CdcTests.logProvider(directory)
                                     .logs()
                                     .min(CommitLog::compareTo)
                                     .orElseThrow(() -> new RuntimeException("Commit log file not found"));

        // read entire commit log and verify correct
        Consumer<Marker> listener = markers::add;
        Set<Long> allRows = readLog(null, keys, firstLog, listener);
        assertEquals(numRows, allRows.size());

        // re-read commit log from each watermark position
        // and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        List<Marker> allMarkers = new ArrayList<>(markers);
        Marker prevMarker = null;
        assertFalse(allMarkers.isEmpty());
        for (Marker marker : allMarkers)
        {
            Set<Long> result = readLog(marker, keys, firstLog, null);
            assertTrue(result.size() < foundRows);
            foundRows = result.size();
            if (prevMarker != null)
            {
                assertTrue(prevMarker.compareTo(marker) < 0);
                assertTrue(prevMarker.position < marker.position);
            }
            prevMarker = marker;

            if (marker.equals(allMarkers.get(allMarkers.size() - 1)))
            {
                // last marker should return 0 updates
                // and be at the end of the file
                assertTrue(result.isEmpty());
            }
            else
            {
                assertFalse(result.isEmpty());
            }
        }
    }

    private Set<Long> readLog(@Nullable Marker highWaterMark,
                              Set<Long> keys,
                              CommitLog logFile,
                              @Nullable Consumer<Marker> listener)
    {
        Set<Long> keysRead = new HashSet<>();

        try (BufferingCommitLogReader reader = new BufferingCommitLogReader(logFile,
                                                                            highWaterMark,
                                                                            CdcStats.STUB,
                                                                            listener))
        {
            BufferingCommitLogReader.Result result = reader.result();
            for (PartitionUpdateWrapper update : result.updates())
            {
                long key = Objects.requireNonNull(update.partitionKey()).getLong();
                assertFalse(keysRead.contains(key));
                keysRead.add(key);
                assertTrue(keys.contains(key));
            }

            return keysRead;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
