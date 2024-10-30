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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CollectionElement;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.RangeTombstoneData;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.msg.jdk.CdcMessage;
import org.apache.cassandra.cdc.msg.jdk.Column;
import org.apache.cassandra.cdc.msg.jdk.RangeTombstoneMsg;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaBuilder;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.quicktheories.api.Pair;

import static org.apache.cassandra.cdc.CdcTester.DEFAULT_NUM_ROWS;
import static org.apache.cassandra.cdc.CdcTester.testCommitLog;
import static org.apache.cassandra.cdc.CdcTester.testWith;
import static org.apache.cassandra.cdc.CdcTests.ASYNC_EXECUTOR;
import static org.apache.cassandra.cdc.CdcTests.BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.CDC_BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.MESSAGE_CONVERTER;
import static org.apache.cassandra.cdc.CdcTests.directory;
import static org.apache.cassandra.cdc.CdcTests.logProvider;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class MicroBatchIteratorTests
{
    @Test
    public void testSetDeletion()
    {
        Map<String, String> deletedValues = new HashMap<>(DEFAULT_NUM_ROWS);
        runTest(
        TestSchema.builder(BRIDGE)
                  .withPartitionKey("a", BRIDGE.uuid())
                  .withColumn("b", BRIDGE.set(BRIDGE.text())),
        (schema, i, rows) -> {
            TestSchema.TestRow testRow = CdcTester.newUniqueRow(schema, rows);
            String deletedValue = (String) BRIDGE.text().randomValue(4);
            ByteBuffer key = BRIDGE.text().serialize(deletedValue);
            testRow = testRow.copy("b", CollectionElement.deleted(CellPath.create(key)));
            deletedValues.put(testRow.get(0).toString(), deletedValue);
            return testRow;
        },
        (event, rows, nowMicros) -> {
            CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
            assertEquals(msg.operationType(), CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
            String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
            assertNotNull(msg.getComplexCellDeletion());
            assertEquals(expected, msg.getComplexCellDeletion().get("b").get(0).toString());
        }
        );
    }

    @Test
    public void testMapDeletion()
    {
        Map<String, String> deletedValues = new HashMap<>(DEFAULT_NUM_ROWS);
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.uuid())
                          .withColumn("b", BRIDGE.map(BRIDGE.text(), BRIDGE.aInt())),
                (schema, i, rows) -> {
                    TestSchema.TestRow testRow = CdcTester.newUniqueRow(schema, rows);
                    String deletedValue = (String) BRIDGE.text().randomValue(4);
                    ByteBuffer key = BRIDGE.text().serialize(deletedValue);
                    testRow = testRow.copy("b", CollectionElement.deleted(CellPath.create(key)));
                    deletedValues.put(testRow.get(0).toString(), deletedValue);
                    return testRow;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
                    String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
                    assertNotNull(msg.getComplexCellDeletion());
                    assertEquals(expected, msg.getComplexCellDeletion().get("b").get(0).toString());
                }
        );
    }

    @Test
    public void testRangeTombstone()
    {
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.uuid())
                          .withClusteringKey("b", BRIDGE.aInt())
                          .withClusteringKey("c", BRIDGE.aInt())
                          .withColumn("d", BRIDGE.text()),
                (schema, i, rows) -> {
                    TestSchema.TestRow testRow = CdcTester.newUniqueRow(schema, rows);
                    int start = RandomUtils.randomPositiveInt(1024);
                    int end = start + RandomUtils.randomPositiveInt(100000);
                    testRow.setRangeTombstones(ImmutableList.of(
                                               new RangeTombstoneData(
                                               new RangeTombstoneData.Bound(new Integer[]{start, start + RandomUtils.randomPositiveInt(100)}, true),
                                               new RangeTombstoneData.Bound(new Integer[]{end, end + RandomUtils.randomPositiveInt(100)}, true)))
                    );
                    rows.put(testRow.get(0).toString(), testRow);
                    return testRow;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.RANGE_DELETE);
                    List<RangeTombstoneMsg> tombstones = msg.rangeTombstones();
                    TestSchema.TestRow row = rows.get(msg.column("a").value().toString());
                    assertEquals(1, tombstones.size());
                    RangeTombstoneMsg tombstone = tombstones.get(0);
                    assertTrue(tombstone.startInclusive);
                    assertTrue(tombstone.endInclusive);
                    assertEquals(2, tombstone.startBound().size());
                    assertEquals(2, tombstone.endBound().size());
                    assertEquals("b", tombstone.startBound().get(0).name());
                    assertEquals("c", tombstone.startBound().get(1).name());
                    assertEquals("b", tombstone.endBound().get(0).name());
                    assertEquals("c", tombstone.endBound().get(1).name());
                    RangeTombstoneData expected = row.rangeTombstones().get(0);
                    assertEquals(expected.open.values[0], tombstone.startBound().get(0).value());
                    assertEquals(expected.open.values[1], tombstone.startBound().get(1).value());
                    assertEquals(expected.close.values[0], tombstone.endBound().get(0).value());
                    assertEquals(expected.close.values[1], tombstone.endBound().get(1).value());
                    assertEquals(expected.open.inclusive, tombstone.startInclusive);
                    assertEquals(expected.close.inclusive, tombstone.endInclusive);
                }
        );
    }

    @Test
    public void testRowDelete()
    {
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.timeuuid())
                          .withPartitionKey("b", BRIDGE.aInt())
                          .withClusteringKey("c", BRIDGE.bigint())
                          .withColumn("d", BRIDGE.text()),
                (schema, i, rows) -> {
                    TestSchema.TestRow row = schema.randomRow();
                    row.delete();
                    rows.put(row.getPrimaryHexKey(), row);
                    return row;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.ROW_DELETE);
                    assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                    String key = event.getHexKey();
                    assertTrue(rows.containsKey(key));
                    assertEquals(2, msg.partitionKeys().size());
                    assertEquals(1, msg.clusteringKeys().size());
                    assertEquals(0, msg.staticColumns().size());
                    assertEquals(0, msg.valueColumns().size());
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInserts()
    {
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.timeuuid())
                          .withPartitionKey("b", BRIDGE.text())
                          .withClusteringKey("c", BRIDGE.timestamp())
                          .withColumn("d", BRIDGE.map(BRIDGE.text(), BRIDGE.aInt())),
                (schema, i, rows) -> {
                    TestSchema.TestRow row = schema.randomRow();
                    rows.put(row.getPrimaryHexKey(), row);
                    return row;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.INSERT);
                    assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                    String key = event.getHexKey();
                    assertTrue(rows.containsKey(key));
                    TestSchema.TestRow testRow = rows.get(key);
                    Map<String, Integer> expected = (Map<String, Integer>) testRow.get(3);
                    Column col = msg.valueColumns().get(0);
                    assertEquals("d", col.name());
                    assertEquals("map<text, int>", col.type().cqlName());
                    Map<String, Integer> actual = (Map<String, Integer>) col.value();
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void testPartitionDelete()
    {
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.timeuuid())
                          .withPartitionKey("b", BRIDGE.aInt())
                          .withClusteringKey("c", BRIDGE.bigint())
                          .withColumn("d", BRIDGE.text()),
                (schema, i, rows) -> {
                    TestSchema.TestRow row = schema.randomPartitionDelete();
                    rows.put(row.getPartitionHexKey(), row); // partition delete so just the partition keys
                    return row;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.PARTITION_DELETE);
                    assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                    String key = event.getHexKey();
                    assertTrue(rows.containsKey(key));
                    assertEquals(2, msg.partitionKeys().size());
                    assertEquals(0, msg.clusteringKeys().size());
                    assertEquals(0, msg.staticColumns().size());
                    assertEquals(0, msg.valueColumns().size());
                });
    }

    @Test
    public void testUpdateStaticColumnAndValueColumns()
    {
        qt().forAll(cql3Type(BRIDGE).zip(arbitrary().enumValues(OperationType.class), Pair::of))
            .checkAssert(cql3TypeAndInsertFlag -> {
                CqlField.NativeType cqlType = cql3TypeAndInsertFlag._1;
                OperationType insertOrUpdate = cql3TypeAndInsertFlag._2;
                testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                      .withPartitionKey("pk", BRIDGE.uuid())
                                                      .withClusteringKey("ck", BRIDGE.uuid())
                                                      .withStaticColumn("sc", cqlType)
                                                      .withColumn("c1", cqlType))
                .clearWriters()
                .withWriter(((tester, rows, writer) -> {
                    long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    IntStream.range(0, tester.numRows)
                             .forEach(i -> {
                                 TestSchema.TestRow row = CdcTester.newUniqueRow(tester.schema, rows);
                                 insertOrUpdate.accept(row);
                                 writer.accept(row, timestampMicros);
                             });
                }))
                .withCdcEventChecker((testRows, events) -> {
                    assertFalse(events.isEmpty());
                    for (CdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        Value pk = event.getPartitionKeys().get(0);
                        assertEquals("pk", pk.columnName);
                        assertEquals(1, event.getClusteringKeys().size());
                        Value ck = event.getClusteringKeys().get(0);
                        assertEquals("ck", ck.columnName);
                        assertEquals(1, event.getValueColumns().size());
                        Value c1 = event.getValueColumns().get(0);
                        assertEquals("c1", c1.columnName);
                        assertEquals(1, event.getStaticColumns().size());
                        Value sc = event.getStaticColumns().get(0);
                        assertEquals("sc", sc.columnName);
                        TestSchema.TestRow testRow = testRows.get(event.getHexKey());
                        assertEquals(testRow.get(2), // static column matches
                                     cqlType.deserializeToJavaType(sc.getValue()));
                        assertEquals(testRow.get(3), // value column matches
                                     cqlType.deserializeToJavaType(c1.getValue()));
                    }
                })
                .run();
            });
    }

    @Test
    public void testUpdate()
    {
        runTest(TestSchema.builder(BRIDGE)
                          .withPartitionKey("a", BRIDGE.timeuuid())
                          .withPartitionKey("b", BRIDGE.aInt())
                          .withClusteringKey("c", BRIDGE.bigint())
                          .withColumn("d", BRIDGE.text()),
                (schema, i, rows) -> {
                    TestSchema.TestRow row = schema.randomRow();
                    row.fromUpdate();
                    rows.put(row.getPrimaryHexKey(), row);
                    return row;
                },
                (event, rows, nowMicros) -> {
                    CdcMessage msg = MESSAGE_CONVERTER.toCdcMessage(event);
                    assertEquals(msg.operationType(), CdcEvent.Kind.UPDATE);
                    assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                    String key = event.getHexKey();
                    assertTrue(rows.containsKey(key));
                    assertEquals(2, msg.partitionKeys().size());
                    assertEquals(1, msg.clusteringKeys().size());
                    assertEquals(0, msg.staticColumns().size());
                    assertEquals(1, msg.valueColumns().size());
                    TestSchema.TestRow row = rows.get(key);
                    String expected = (String) row.get(3);
                    assertEquals(expected, msg.valueColumns().get(0).value().toString());
                }
        );
    }

    private enum OperationType implements Consumer<TestSchema.TestRow>
    {
        INSERT(TestSchema.TestRow::fromInsert),
        UPDATE(TestSchema.TestRow::fromUpdate);

        private final Consumer<TestSchema.TestRow> testRowConsumer;

        OperationType(Consumer<TestSchema.TestRow> testRowConsumer)
        {
            this.testRowConsumer = testRowConsumer;
        }

        public void accept(TestSchema.TestRow row)
        {
            testRowConsumer.accept(row);
        }
    }

    public interface RowGenerator
    {
        TestSchema.TestRow newRow(TestSchema schema, int i, Map<String, TestSchema.TestRow> rows);
    }

    public interface TestVerifier
    {
        void verify(CdcEvent event, Map<String, TestSchema.TestRow> rows, long nowMicros);
    }

    private static void runTest(TestSchema.Builder schemaBuilder,
                                RowGenerator rowGenerator,
                                TestVerifier verify)
    {
        String jobId = UUID.randomUUID().toString();
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        int numRows = DEFAULT_NUM_ROWS;
        TestSchema schema = schemaBuilder
                            .withCdc(true)
                            .build();
        CqlTable cqlTable = schema.buildTable();
        new SchemaBuilder(cqlTable, Partitioner.Murmur3Partitioner, schema.withCdc);
        schema.setCassandraVersion(CassandraVersion.FOURZERO);

        try
        {
            Map<String, TestSchema.TestRow> rows = new HashMap<>(numRows);
            for (int i = 0; i < numRows; i++)
            {
                TestSchema.TestRow row = rowGenerator.newRow(schema, i, rows);
                CDC_BRIDGE.log(TimeProvider.DEFAULT, cqlTable, testCommitLog, row, nowMicros);
            }
            testCommitLog.sync();

            int count = 0;
            long start = System.currentTimeMillis();
            CdcState state = CdcState.BLANK;
            try (MicroBatchIterator it = new MicroBatchIterator(CDC_BRIDGE,
                                                                state,
                                                                CassandraSource.DEFAULT,
                                                                () -> ImmutableSet.of(schema.keyspace),
                                                                CdcTests.TEST_OPTIONS,
                                                                ASYNC_EXECUTOR,
                                                                logProvider(directory)))
            {
                while (count < numRows && it.hasNext())
                {
                    CdcEvent event = it.next();
                    verify.verify(event, rows, nowMicros);
                    count++;
                    if (CdcTester.maybeTimeout(start, numRows, count, jobId))
                    {
                        break;
                    }
                }
                assertEquals(numRows, count);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        finally
        {
            resetTest();
        }
    }

    private static void resetTest()
    {
        CdcTester.tearDown();
        testCommitLog.start();
    }
}
