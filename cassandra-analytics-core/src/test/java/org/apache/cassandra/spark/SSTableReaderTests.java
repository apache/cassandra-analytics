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

package org.apache.cassandra.spark;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.data.BasicSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.getFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.quicktheories.QuickTheory.qt;

public class SSTableReaderTests
{

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testCollectionWithTtlUsingConstantReferenceTime(CassandraBridge bridge)
    {
        // offset is 0, column values in all rows should be unexpired; thus, reading 10 values
        testTtlUsingConstantReferenceTimeHelper(bridge, 50, 0, 10, 10);
        // ensure all rows expires by advancing enough time in the future; thus, reading 0 values
        testTtlUsingConstantReferenceTimeHelper(bridge, 50, 100, 10, 0);
    }

    // helper that write rows with ttl, and assert on the compaction result by changing the reference time
    private void testTtlUsingConstantReferenceTimeHelper(CassandraBridge bridgeForTest,
                                                         int ttlSecs,
                                                         int timeOffsetSecs,
                                                         int rows,
                                                         int expectedValues)
    {
        AtomicInteger referenceEpoch = new AtomicInteger(0);
        TimeProvider navigatableTimeProvider = referenceEpoch::get;

        Set<Integer> expectedColValue = new HashSet<>(Arrays.asList(1, 2, 3));
        TestRunnable test = (partitioner, dir, bridge) -> {
            TestSchema schema = TestSchema.builder(bridge)
                                          .withPartitionKey("a", bridge.aInt())
                                          .withColumn("b", bridge.set(bridge.aInt()))
                                          .withTTL(ttlSecs)
                                          .build();
            schema.writeSSTable(dir, bridge, partitioner, (writer) -> {
                for (int i = 0; i < rows; i++)
                {
                    writer.write(i, expectedColValue);
                }
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            });
            int t1 = navigatableTimeProvider.nowInSeconds();
            assertEquals(1, countSSTables(dir));

            // open CompactionStreamScanner over SSTables
            CqlTable table = schema.buildTable();
            TestDataLayer dataLayer = new TestDataLayer(bridge, getFileType(dir, FileType.DATA).collect(Collectors.toList()), table);
            BasicSupplier ssTableSupplier = new BasicSupplier(dataLayer.listSSTables().collect(Collectors.toSet()));

            int count = 0;
            referenceEpoch.set(t1 + timeOffsetSecs);

            try (StreamScanner<RowData> scanner = bridge.getCompactionScanner(table,
                                                                              partitioner,
                                                                              ssTableSupplier,
                                                                              null,
                                                                              Collections.emptyList(),
                                                                              null,
                                                                              navigatableTimeProvider,
                                                                              false,
                                                                              false,
                                                                              Stats.DoNothingStats.INSTANCE))
            {
                // iterate through CompactionStreamScanner verifying it correctly compacts data together
                RowData rowData = scanner.data();
                while (scanner.next())
                {
                    scanner.advanceToNextColumn();

                    // extract column name
                    ByteBuffer colBuf = rowData.getColumnName();
                    String colName = ByteBufferUtils.string(ByteBufferUtils.readBytesWithShortLength(colBuf));
                    colBuf.get();
                    if (StringUtils.isEmpty(colName))
                    {
                        continue;
                    }
                    assertEquals("b", colName);

                    // extract value column
                    ByteBuffer b = rowData.getValue();
                    Set<?> set = new HashSet<>(Arrays.asList(((GenericArrayData) bridge.set(bridge.aInt())
                                                                                       .deserializeToType(bridge.typeConverter(), b))
                                                             .array()));
                    assertEquals(expectedColValue, set);
                    count++;
                }
            }
            assertEquals(expectedValues, count);
        };

        qt()
        .forAll(TestUtils.partitioners())
        .checkAssert(partitioner -> runTest(partitioner, bridgeForTest, test));
    }
}
