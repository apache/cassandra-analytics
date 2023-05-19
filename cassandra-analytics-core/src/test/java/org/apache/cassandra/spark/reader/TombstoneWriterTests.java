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

package org.apache.cassandra.spark.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

/**
 * Test we can write out partition, row and range tombstones to SSTables using the SSTableTombstoneWriter
 */
public class TombstoneWriterTests
{
    private static final int NUM_ROWS = 50;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testPartitionTombstone()
    {
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert(version -> TestUtils.runTest(version, (partitioner, directory, bridge) -> {
                // Write tombstone SSTable
                TestSchema schema = TestSchema.basicBuilder(bridge)
                                              .withDeleteFields("a =")
                                              .build();
                schema.writeTombstoneSSTable(directory, bridge, partitioner, writer -> {
                    for (int index = 0; index < NUM_ROWS; index++)
                    {
                        writer.write(index);
                    }
                });

                // Convert SSTable to JSON
                Path dataDbFile = TestUtils.getFirstFileType(directory, FileType.DATA);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                bridge.sstableToJson(dataDbFile, out);
                JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }

                // Verify SSTable contains partition tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertTrue(0 <= key && key < NUM_ROWS);
                    assertTrue(node.get(index).has("rows"));
                    assertTrue(partition.has("deletion_info"));
                    assertTrue(partition.get("deletion_info").has("marked_deleted"));
                    assertTrue(partition.get("deletion_info").has("local_delete_time"));
                }
            }));
    }

    @Test
    public void testRowTombstone()
    {
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert(version -> TestUtils.runTest(version, (partitioner, directory, bridge) -> {
                // Write tombstone SSTable
                TestSchema schema = TestSchema.basicBuilder(bridge)
                                              .withDeleteFields("a =", "b =")
                                              .build();
                schema.writeTombstoneSSTable(directory, bridge, partitioner, writer -> {
                    for (int index = 0; index < NUM_ROWS; index++)
                    {
                        writer.write(index, index);
                    }
                });

                // Convert SSTable to JSON
                Path dataDbFile = TestUtils.getFirstFileType(directory, FileType.DATA);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                bridge.sstableToJson(dataDbFile, out);
                JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }

                // Verify SSTable contains row tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertTrue(0 <= key && key < NUM_ROWS);
                    assertFalse(partition.has("deletion_info"));

                    assertTrue(node.get(index).has("rows"));
                    JsonNode row = node.get(index).get("rows").get(0);
                    assertEquals("row", row.get("type").asText());
                    assertEquals(key, row.get("clustering").get(0).asInt());
                    assertTrue(row.has("deletion_info"));
                    assertTrue(row.get("deletion_info").has("marked_deleted"));
                    assertTrue(row.get("deletion_info").has("local_delete_time"));
                }
            }));
    }

    @Test
    public void testRangeTombstone()
    {
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert(version -> TestUtils.runTest(version, (partitioner, directory, bridge) -> {
                // Write tombstone SSTable
                TestSchema schema = TestSchema.basicBuilder(bridge)
                                              .withDeleteFields("a =", "b >=", "b <")
                                              .build();
                schema.writeTombstoneSSTable(directory, bridge, partitioner, writer -> {
                    for (int index = 0; index < NUM_ROWS; index++)
                    {
                        writer.write(index, 50, 999);
                    }
                });

                // Convert SSTable to JSON
                Path dataDbFile = TestUtils.getFirstFileType(directory, FileType.DATA);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                bridge.sstableToJson(dataDbFile, out);
                JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }

                // Verify SSTable contains range tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertTrue(0 <= key && key < NUM_ROWS);
                    assertFalse(partition.has("deletion_info"));

                    assertTrue(node.get(index).has("rows"));
                    assertEquals(2, node.get(index).get("rows").size());

                    JsonNode row1 = node.get(index).get("rows").get(0);
                    assertEquals("range_tombstone_bound", row1.get("type").asText());
                    JsonNode start = row1.get("start");
                    assertEquals("inclusive", start.get("type").asText());
                    assertEquals(50, start.get("clustering").get(0).asInt());
                    assertTrue(start.has("deletion_info"));
                    assertTrue(start.get("deletion_info").has("marked_deleted"));
                    assertTrue(start.get("deletion_info").has("local_delete_time"));

                    JsonNode row2 = node.get(index).get("rows").get(1);
                    assertEquals("range_tombstone_bound", row2.get("type").asText());
                    JsonNode end = row2.get("end");
                    assertEquals("exclusive", end.get("type").asText());
                    assertEquals(999, end.get("clustering").get(0).asInt());
                    assertTrue(end.has("deletion_info"));
                    assertTrue(end.get("deletion_info").has("marked_deleted"));
                    assertTrue(end.get("deletion_info").has("local_delete_time"));
                }
            }));
    }
}
