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

package org.apache.cassandra.cdc;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.newUniqueRow;
import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests verifying Y2038 time handling in the CDC pipeline.
 * Validates that TTL, expiration time, and deletion time values flow correctly
 * through the commit log writing and reading pipeline for both Cassandra 4.0 and 5.0.
 */
public class Y2038TimeTests extends CdcTestBase
{
    private static final int TTL_SECONDS = 3600; // 1 hour

    /**
     * Verifies that expirationTimeInSec is a valid long value and approximately equals nowInSeconds + ttl.
     * This ensures the int-to-long widening in the CDC pipeline preserves time values correctly.
     */
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testTtlExpirationTimeIsLong(CassandraVersion version)
    {
        long beforeTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                            .withPartitionKey("pk", bridge.uuid())
                                                            .withColumn("c1", bridge.aInt()))
        .clearWriters()
        .withNumRows(5)
        .withWriter((tester, rows, writer) -> {
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = newUniqueRow(tester.schema, rows);
                testRow.setTTL(TTL_SECONDS);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            for (CdcEvent event : events)
            {
                assertThat(event.getTtl()).isNotNull();
                assertThat(event.getTtl().ttlInSec).isEqualTo(TTL_SECONDS);
                // expirationTimeInSec should be approximately nowInSeconds + TTL
                long expirationTime = event.getTtl().expirationTimeInSec;
                assertThat(expirationTime)
                    .as("expirationTimeInSec should be between beforeTest+TTL and afterTest+TTL")
                    .isBetween(beforeTestEpochSec + TTL_SECONDS, afterTestEpochSec + TTL_SECONDS);
            }
        })
        .run();
    }

    /**
     * Tests with maximum supported TTL value (max int seconds ~ 68 years).
     * In Cassandra 5.0, the expiration time (nowInSeconds + ttl) can exceed Integer.MAX_VALUE.
     * In Cassandra 4.0, the expiration time is limited to int range.
     */
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMaxSupportedTtl(CassandraVersion version)
    {
        // Use a large TTL value. The max TTL Cassandra supports is MAX_DELETION_TIME - nowInSeconds.
        // We use 20 years which is safe for both 4.0 and 5.0.
        int largeTtl = 20 * 365 * 24 * 3600; // ~20 years in seconds

        testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                            .withPartitionKey("pk", bridge.uuid())
                                                            .withColumn("c1", bridge.aInt()))
        .clearWriters()
        .withNumRows(3)
        .withWriter((tester, rows, writer) -> {
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = newUniqueRow(tester.schema, rows);
                testRow.setTTL(largeTtl);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long nowEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            for (CdcEvent event : events)
            {
                assertThat(event.getTtl()).isNotNull();
                assertThat(event.getTtl().ttlInSec).isEqualTo(largeTtl);
                long expirationTime = event.getTtl().expirationTimeInSec;
                // The expiration time should be positive and in the future
                assertThat(expirationTime)
                    .as("expirationTimeInSec should be positive and in the future")
                    .isGreaterThan(nowEpochSec);
            }
        })
        .run();
    }

    /**
     * Tests that deletion time from partition deletions is handled correctly as a long value.
     */
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testDeletionTimeIsLong(CassandraVersion version)
    {
        long beforeTestMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                            .withPartitionKey("pk", bridge.uuid())
                                                            .withColumn("c1", bridge.aInt()))
        .clearWriters()
        .withNumRows(5)
        .withWriter((tester, rows, writer) -> {
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = CdcTester.newUniquePartitionDeletion(tester.schema, rows);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            for (CdcEvent event : events)
            {
                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.PARTITION_DELETE);
                // Deletion timestamp should be within the test window
                long eventTimestampMicros = event.getTimestamp(TimeUnit.MICROSECONDS);
                assertThat(eventTimestampMicros)
                    .as("deletion timestamp should be within the test time window")
                    .isBetween(beforeTestMicros, afterTestMicros);
            }
        })
        .run();
    }

    /**
     * Tests that TTL and non-TTL rows can coexist correctly,
     * verifying time handling does not corrupt non-TTL data.
     */
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMixedTtlAndNonTtlRows(CassandraVersion version)
    {
        testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                            .withPartitionKey("pk", bridge.uuid())
                                                            .withColumn("c1", bridge.aInt()))
        .clearWriters()
        .withNumRows(10)
        .withWriter((tester, rows, writer) -> {
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = newUniqueRow(tester.schema, rows);
                // Set TTL on even rows only
                if (i % 2 == 0)
                {
                    testRow.setTTL(TTL_SECONDS);
                }
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            int withTtl = 0;
            int withoutTtl = 0;
            for (CdcEvent event : events)
            {
                if (event.getTtl() != null)
                {
                    withTtl++;
                    assertThat(event.getTtl().ttlInSec).isEqualTo(TTL_SECONDS);
                    assertThat(event.getTtl().expirationTimeInSec).isGreaterThan(0L);
                }
                else
                {
                    withoutTtl++;
                }
            }
            assertThat(withTtl).isEqualTo(5);
            assertThat(withoutTtl).isEqualTo(5);
        })
        .run();
    }
}
