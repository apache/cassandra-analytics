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

import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.CdcTests;
import org.apache.cassandra.cdc.LocalCommitLog;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.cdc.test.TestCdcBridgeProvider;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferingCommitLogReaderTests
{
    @BeforeAll
    static void beforeAll()
    {
        TestCdcBridgeProvider.setup();
    }

    @AfterAll
    static void afterAll()
    {
        TestCdcBridgeProvider.tearDown();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testReaderSeek(CassandraVersion version)
    {
        CassandraBridge bridge = TestCdcBridgeProvider.getCassandraBridge(version);
        CdcBridge cdcBridge = TestCdcBridgeProvider.getTestCdcBridge(version);
        Path directory = TestCdcBridgeProvider.getCommitLogDir(version);
        CommitLogInstance commitLog = cdcBridge.createCommitLogInstance(directory);
        TestSchema schema = TestSchema.builder(bridge)
                                      .withPartitionKey("pk", bridge.bigint())
                                      .withColumn("c1", bridge.bigint())
                                      .withColumn("c2", bridge.bigint())
                                      .withCdc(true)
                                      .build();
        CqlTable cqlTable = schema.buildTable();
        bridge.buildSchema(cqlTable.createStatement(),
                           cqlTable.keyspace(),
                           ReplicationFactor.simpleStrategy(1),
                           Partitioner.Murmur3Partitioner,
                           Collections.emptySet(),
                           null, 0, true);
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
            cdcBridge.log(TimeProvider.DEFAULT, cqlTable, commitLog, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        commitLog.sync();

        List<Marker> markers = Collections.synchronizedList(new ArrayList<>());
        CommitLog firstLog = new LocalCommitLog(Paths.get(CdcTests.logProvider(directory)
                                                                              .logs()
                                                                              .min(CommitLog::compareTo)
                                                                              .orElseThrow(() -> new RuntimeException("Commit log file not found"))
                                                                              .path()))
        {
            @Override
            public boolean completed()
            {
                return true;
            }
        };

        // read entire commit log and verify correct
        Consumer<Marker> listener = markers::add;
        Set<Long> allRows = readLog(cdcBridge, null, keys, firstLog, listener);
        assertThat(allRows).hasSize(numRows);

        // re-read commit log from each watermark position
        // and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        List<Marker> allMarkers = new ArrayList<>(markers);
        Marker prevMarker = null;
        assertThat(allMarkers).isNotEmpty();
        for (Marker marker : allMarkers)
        {
            Set<Long> result = readLog(cdcBridge, marker, keys, firstLog, null);
            assertThat(result.size()).isLessThan(foundRows);
            foundRows = result.size();
            if (prevMarker != null)
            {
                assertThat(prevMarker).isLessThan(marker);
                assertThat(prevMarker.position).isLessThan(marker.position);
            }
            prevMarker = marker;

            if (marker.equals(allMarkers.get(allMarkers.size() - 1)))
            {
                // last marker should return 0 updates
                // and be at the end of the file
                assertThat(result).isEmpty();
            }
            else
            {
                assertThat(result).isNotEmpty();
            }
        }
    }

    private Set<Long> readLog(CdcBridge cdcBridge,
                              @Nullable Marker highWaterMark,
                              Set<Long> keys,
                              CommitLog logFile,
                              @Nullable Consumer<Marker> listener)
    {
        Set<Long> keysRead = new HashSet<>();

        BufferingCommitLogReader.Result result = cdcBridge.readLog(logFile,
                                                                   null,
                                                                   CommitLogMarkers.of(highWaterMark),
                                                                   0,
                                                                   CdcStats.STUB,
                                                                   null,
                                                                   listener,
                                                                   null,
                                                                   false);
        for (PartitionUpdateWrapper update : result.updates())
        {
            long key = Objects.requireNonNull(update.partitionKey()).getLong();
            assertThat(keysRead).doesNotContain(key);
            keysRead.add(key);
            assertThat(keys).contains(key);
        }

        // Verify the position fix: after reading (from any start offset), position must
        // reach maxOffset and isFullyRead() must return true.
        // TODO(lantoniak): Re-enable assertion.
        // assertThat(result.isFullyRead()).isTrue();

        return keysRead;
    }
}
