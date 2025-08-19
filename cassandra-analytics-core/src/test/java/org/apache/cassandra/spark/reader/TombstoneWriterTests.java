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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.assertj.core.api.Assertions.assertThat;
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
                assertThat(node).hasSize(NUM_ROWS);
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertThat(key).isBetween(0, NUM_ROWS - 1);
                    assertThat(node.get(index).has("rows")).isTrue();
                    assertThat(partition.has("deletion_info")).isTrue();
                    assertThat(partition.get("deletion_info").has("marked_deleted")).isTrue();
                    assertThat(partition.get("deletion_info").has("local_delete_time")).isTrue();
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
                assertThat(node).hasSize(NUM_ROWS);
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertThat(key).isBetween(0, NUM_ROWS - 1);
                    assertThat(partition.has("deletion_info")).isFalse();

                    assertThat(node.get(index).has("rows")).isTrue();
                    JsonNode row = node.get(index).get("rows").get(0);
                    assertThat(row.get("type").asText()).isEqualTo("row");
                    assertThat(row.get("clustering").get(0).asInt()).isEqualTo(key);
                    assertThat(row.has("deletion_info")).isTrue();
                    assertThat(row.get("deletion_info").has("marked_deleted")).isTrue();
                    assertThat(row.get("deletion_info").has("local_delete_time")).isTrue();
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
                assertThat(node).hasSize(NUM_ROWS);
                for (int index = 0; index < NUM_ROWS; index++)
                {
                    JsonNode partition = node.get(index).get("partition");
                    int key = partition.get("key").get(0).asInt();
                    assertThat(key).isBetween(0, NUM_ROWS - 1);
                    assertThat(partition.has("deletion_info")).isFalse();

                    assertThat(node.get(index).has("rows")).isTrue();
                    assertThat(node.get(index).get("rows")).hasSize(2);

                    JsonNode row1 = node.get(index).get("rows").get(0);
                    assertThat(row1.get("type").asText()).isEqualTo("range_tombstone_bound");
                    JsonNode start = row1.get("start");
                    assertThat(start.get("type").asText()).isEqualTo("inclusive");
                    assertThat(start.get("clustering").get(0).asInt()).isEqualTo(50);
                    assertThat(start.has("deletion_info")).isTrue();
                    assertThat(start.get("deletion_info").has("marked_deleted")).isTrue();
                    assertThat(start.get("deletion_info").has("local_delete_time")).isTrue();

                    JsonNode row2 = node.get(index).get("rows").get(1);
                    assertThat(row2.get("type").asText()).isEqualTo("range_tombstone_bound");
                    JsonNode end = row2.get("end");
                    assertThat(end.get("type").asText()).isEqualTo("exclusive");
                    assertThat(end.get("clustering").get(0).asInt()).isEqualTo(999);
                    assertThat(end.has("deletion_info")).isTrue();
                    assertThat(end.get("deletion_info").has("marked_deleted")).isTrue();
                    assertThat(end.get("deletion_info").has("local_delete_time")).isTrue();
                }
            }));
    }
}
