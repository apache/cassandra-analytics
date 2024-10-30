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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CollectionElement;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.CdcTester.testWith;
import static org.apache.cassandra.cdc.CdcTests.BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.directory;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class CollectionDeletionTests
{
    @Test
    public void testElementDeletionInMap()
    {
        final String name = "m";
        testElementDeletionInCollection(1, 2, /* numOfColumns */
                                        ImmutableList.of(name),
                                        type -> TestSchema.builder(BRIDGE)
                                                          .withPartitionKey("pk", BRIDGE.uuid())
                                                          .withColumn(name, BRIDGE.map(type, type)));
    }

    @Test
    public void testElementDeletionInSet()
    {
        final String name = "s";
        testElementDeletionInCollection(1, 2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder(BRIDGE)
                                                          .withPartitionKey("pk", BRIDGE.uuid())
                                                          .withColumn(name, BRIDGE.set(type)));
    }

    @Test
    public void testElementDeletionsInMultipleColumns()
    {
        testElementDeletionInCollection(1, 4, /* numOfColumns */
                                        Arrays.asList("c1", "c2", "c3"),
                                        type -> TestSchema.builder(BRIDGE)
                                                          .withPartitionKey("pk", BRIDGE.uuid())
                                                          .withColumn("c1", BRIDGE.set(type))
                                                          .withColumn("c2", BRIDGE.set(type))
                                                          .withColumn("c3", BRIDGE.set(type)));
    }

    // validate that cell deletions in a complex data can be correctly encoded.
    private void testElementDeletionInCollection(int numOfPKs,
                                                 int numOfColumns,
                                                 List<String> collectionColumnNames,
                                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, byte[]> elementDeletionIndices = new HashMap<>();
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
                        elementDeletionIndices.clear();
                        long timestamp = minTimestamp;
                        for (int i = 0; i < tester.numRows; i++)
                        {
                            int ignoredSize = 10;
                            TestSchema.TestRow testRow;
                            if (rnd.nextDouble() < 0.5)
                            {
                                // NOTE: it is a little hacky. For simplicity, all collections in the row
                                // has the SAME entry being deleted.
                                ByteBuffer key = type.serialize(type.randomValue(ignoredSize));
                                testRow = CdcTester.newUniqueRow(tester.schema, rows);
                                for (String name : collectionColumnNames)
                                {
                                    testRow = testRow.copy(name, CollectionElement.deleted(CellPath.create(key)));
                                }
                                elementDeletionIndices.put(i, key.array());
                            }
                            else
                            {
                                testRow = CdcTester.newUniqueRow(tester.schema, rows);
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
                            assertTrue(lmtInMillis >= minTimestamp, "Last modification time should have a lower bound of " + minTimestamp);
                            assertEquals(numOfPKs, event.getPartitionKeys().size(), "Regardless of being row deletion or not, the partition key must present");
                            assertNull(event.getClusteringKeys());
                            assertNull(event.getStaticColumns());

                            if (elementDeletionIndices.containsKey(i)) // verify deletion
                            {
                                assertEquals(CdcEvent.Kind.COMPLEX_ELEMENT_DELETE, event.getKind());
                                Map<String, List<ByteBuffer>> cellTombstonesPerCol = event.getTombstonedCellsInComplex();
                                assertNotNull(cellTombstonesPerCol);
                                Map<String, Value> valueColMap = event.getValueColumns()
                                                                      .stream()
                                                                      .collect(Collectors.toMap(v -> v.columnName, Function.identity()));
                                for (String name : collectionColumnNames)
                                {
                                    assertNull(valueColMap.get(name).getValue(), "Collection column's value should be null since only deletion applies");
                                    assertNotNull(cellTombstonesPerCol.get(name));
                                    List<ByteBuffer> deletedCellKeys = cellTombstonesPerCol.get(name);
                                    assertEquals(1, deletedCellKeys.size());
                                    assert deletedCellKeys.get(0).hasArray();
                                    byte[] keyBytesRead = deletedCellKeys.get(0).array();
                                    assertArrayEquals(elementDeletionIndices.get(i), keyBytesRead, "The key encoded should be the same");
                                }
                            }
                            else // verify update
                            {
                                assertEquals(CdcEvent.Kind.INSERT, event.getKind());
                                assertNotNull(event.getValueColumns());
                            }
                        }
                    })
                    .run());
    }
}
