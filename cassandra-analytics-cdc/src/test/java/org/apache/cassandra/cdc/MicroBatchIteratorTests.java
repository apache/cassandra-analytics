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
import static org.assertj.core.api.Assertions.assertThat;
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
            assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
            String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
            assertThat(msg.getComplexCellDeletion()).isNotNull();
            assertThat(msg.getComplexCellDeletion().get("b").get(0).toString()).isEqualTo(expected);
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
                    String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
                    assertThat(msg.getComplexCellDeletion()).isNotNull();
                    assertThat(msg.getComplexCellDeletion().get("b").get(0).toString()).isEqualTo(expected);
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.RANGE_DELETE);
                    List<RangeTombstoneMsg> tombstones = msg.rangeTombstones();
                    TestSchema.TestRow row = rows.get(msg.column("a").value().toString());
                    assertThat(tombstones).hasSize(1);
                    RangeTombstoneMsg tombstone = tombstones.get(0);
                    assertThat(tombstone.startInclusive).isTrue();
                    assertThat(tombstone.endInclusive).isTrue();
                    assertThat(tombstone.startBound()).hasSize(2);
                    assertThat(tombstone.endBound()).hasSize(2);
                    assertThat(tombstone.startBound().get(0).name()).isEqualTo("b");
                    assertThat(tombstone.startBound().get(1).name()).isEqualTo("c");
                    assertThat(tombstone.endBound().get(0).name()).isEqualTo("b");
                    assertThat(tombstone.endBound().get(1).name()).isEqualTo("c");
                    RangeTombstoneData expected = row.rangeTombstones().get(0);
                    assertThat(tombstone.startBound().get(0).value()).isEqualTo(expected.open.values[0]);
                    assertThat(tombstone.startBound().get(1).value()).isEqualTo(expected.open.values[1]);
                    assertThat(tombstone.endBound().get(0).value()).isEqualTo(expected.close.values[0]);
                    assertThat(tombstone.endBound().get(1).value()).isEqualTo(expected.close.values[1]);
                    assertThat(tombstone.startInclusive).isEqualTo(expected.open.inclusive);
                    assertThat(tombstone.endInclusive).isEqualTo(expected.close.inclusive);
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.ROW_DELETE);
                    assertThat(msg.lastModifiedTimeMicros()).isEqualTo(nowMicros);
                    String key = event.getHexKey();
                    assertThat(rows.containsKey(key)).isTrue();
                    assertThat(msg.partitionKeys()).hasSize(2);
                    assertThat(msg.clusteringKeys()).hasSize(1);
                    assertThat(msg.staticColumns()).hasSize(0);
                    assertThat(msg.valueColumns()).hasSize(0);
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.INSERT);
                    assertThat(msg.lastModifiedTimeMicros()).isEqualTo(nowMicros);
                    String key = event.getHexKey();
                    assertThat(rows.containsKey(key)).isTrue();
                    TestSchema.TestRow testRow = rows.get(key);
                    Map<String, Integer> expected = (Map<String, Integer>) testRow.get(3);
                    Column col = msg.valueColumns().get(0);
                    assertThat(col.name()).isEqualTo("d");
                    assertThat(col.type().cqlName()).isEqualTo("map<text, int>");
                    Map<String, Integer> actual = (Map<String, Integer>) col.value();
                    assertThat(actual).isEqualTo(expected);
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.PARTITION_DELETE);
                    assertThat(msg.lastModifiedTimeMicros()).isEqualTo(nowMicros);
                    String key = event.getHexKey();
                    assertThat(rows.containsKey(key)).isTrue();
                    assertThat(msg.partitionKeys()).hasSize(2);
                    assertThat(msg.clusteringKeys()).hasSize(0);
                    assertThat(msg.staticColumns()).hasSize(0);
                    assertThat(msg.valueColumns()).hasSize(0);
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
                    assertThat(events).isNotEmpty();
                    for (CdcEvent event : events)
                    {
                        assertThat(event.getPartitionKeys()).hasSize(1);
                        Value pk = event.getPartitionKeys().get(0);
                        assertThat(pk.columnName).isEqualTo("pk");
                        assertThat(event.getClusteringKeys()).hasSize(1);
                        Value ck = event.getClusteringKeys().get(0);
                        assertThat(ck.columnName).isEqualTo("ck");
                        assertThat(event.getValueColumns()).hasSize(1);
                        Value c1 = event.getValueColumns().get(0);
                        assertThat(c1.columnName).isEqualTo("c1");
                        assertThat(event.getStaticColumns()).hasSize(1);
                        Value sc = event.getStaticColumns().get(0);
                        assertThat(sc.columnName).isEqualTo("sc");
                        TestSchema.TestRow testRow = testRows.get(event.getHexKey());
                        assertThat(cqlType.deserializeToJavaType(sc.getValue()))
                            .isEqualTo(testRow.get(2)); // static column matches
                        assertThat(cqlType.deserializeToJavaType(c1.getValue()))
                            .isEqualTo(testRow.get(3)); // value column matches
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
                    assertThat(msg.operationType()).isEqualTo(CdcEvent.Kind.UPDATE);
                    assertThat(msg.lastModifiedTimeMicros()).isEqualTo(nowMicros);
                    String key = event.getHexKey();
                    assertThat(rows.containsKey(key)).isTrue();
                    assertThat(msg.partitionKeys()).hasSize(2);
                    assertThat(msg.clusteringKeys()).hasSize(1);
                    assertThat(msg.staticColumns()).hasSize(0);
                    assertThat(msg.valueColumns()).hasSize(1);
                    TestSchema.TestRow row = rows.get(key);
                    String expected = (String) row.get(3);
                    assertThat(msg.valueColumns().get(0).value().toString()).isEqualTo(expected);
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
                assertThat(count).isEqualTo(numRows);
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
