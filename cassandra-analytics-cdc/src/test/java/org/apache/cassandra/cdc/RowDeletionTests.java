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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.CdcTester.testWith;
import static org.apache.cassandra.cdc.CdcTests.BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.MESSAGE_CONVERTER;
import static org.apache.cassandra.cdc.CdcTests.directory;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class RowDeletionTests
{
    @Test
    public void testRowDeletionWithClusteringKeyAndStatic()
    {
        testRowDeletion(true, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder(BRIDGE)
                                          .withPartitionKey("pk", BRIDGE.uuid())
                                          .withClusteringKey("ck", BRIDGE.bigint())
                                          .withStaticColumn("sc", BRIDGE.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", BRIDGE.bigint()));
    }

    @Test
    public void testRowDeletionWithClusteringKeyNoStatic()
    {
        testRowDeletion(false, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder(BRIDGE)
                                          .withPartitionKey("pk", BRIDGE.uuid())
                                          .withClusteringKey("ck", BRIDGE.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", BRIDGE.bigint()));
    }

    @Test
    public void testRowDeletionSimpleSchema()
    {
        testRowDeletion(false, // has static
                        false, // has clustering key?
                        type -> TestSchema.builder(BRIDGE)
                                          .withPartitionKey("pk", BRIDGE.uuid())
                                          .withColumn("c1", type)
                                          .withColumn("c2", BRIDGE.bigint()));
    }

    private void testRowDeletion(boolean hasStatic,
                                 boolean hasClustering,
                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write row-level tombstones
        // The expected output should include the values of all primary keys but all other columns should be null,
        // i.e. [pk.., ck.., null..]. The bitset should indicate that only the primary keys are present.
        // This kind of output means the entire row is deleted
        final Set<UUID> rowDeletionIndices = new HashSet<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            type -> testWith(BRIDGE, directory, schemaBuilder.apply(type))
                    .withAddLastModificationTime(true)
                    .clearWriters()
                    .withNumRows(numRows)
                    .withWriter((tester, rows, writer) -> {
                        rowDeletionIndices.clear();
                        long timestamp = minTimestamp;
                        for (int i = 0; i < tester.numRows; i++)
                        {
                            TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows);
                            if (rnd.nextDouble() < 0.5)
                            {
                                testRow.delete();
                                rowDeletionIndices.add(testRow.getUUID("pk"));
                            }
                            timestamp += 1;
                            writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                        }
                    })
                    .withCdcEventChecker((testRows, events) -> {
                        for (int i = 0; i < events.size(); i++)
                        {
                            CdcEvent event = events.get(i);
                            long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                            UUID pk = (UUID) MESSAGE_CONVERTER.toCdcMessage(event.getPartitionKeys().get(0)).value();
                            assertTrue(lmtInMillis >= minTimestamp, "Last modification time should have a lower bound of " + minTimestamp);
                            assertEquals(1, event.getPartitionKeys().size(), "Regardless of being row deletion or not, the partition key must present");
                            if (hasClustering) // and ck to be set.
                            {
                                assertEquals(1, event.getClusteringKeys().size());
                            }
                            else
                            {
                                assertNull(event.getClusteringKeys());
                            }

                            if (rowDeletionIndices.contains(pk)) // verify row deletion
                            {
                                assertNull(event.getStaticColumns(), "None primary key columns should be null");
                                assertNull(event.getValueColumns(), "None primary key columns should be null");
                                assertEquals(CdcEvent.Kind.ROW_DELETE, event.getKind());
                            }
                            else // verify update
                            {
                                if (hasStatic)
                                {
                                    assertNotNull(event.getStaticColumns());
                                }
                                else
                                {
                                    assertNull(event.getStaticColumns());
                                }
                                assertNotNull(event.getValueColumns());
                                assertEquals(CdcEvent.Kind.INSERT, event.getKind());
                            }
                        }
                    })
                    .run());
    }
}
