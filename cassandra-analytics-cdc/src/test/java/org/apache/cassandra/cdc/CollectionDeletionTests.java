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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.cdc.test.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class CollectionDeletionTests extends CdcTestBase
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testElementDeletionInMap(CassandraVersion version)
    {
        final String name = "m";
        testElementDeletionInCollection(bridge, cdcBridge, commitLogDir, 1, 2, /* numOfColumns */
                                        ImmutableList.of(name),
                                        type -> TestSchema.builder(bridge)
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.map(type, type)));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testElementDeletionInSet(CassandraVersion version)
    {
        final String name = "s";
        testElementDeletionInCollection(bridge, cdcBridge, commitLogDir, 1, 2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder(bridge)
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.set(type)));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testElementDeletionsInMultipleColumns(CassandraVersion version)
    {
        testElementDeletionInCollection(bridge, cdcBridge, commitLogDir, 1, 4, /* numOfColumns */
                                        Arrays.asList("c1", "c2", "c3"),
                                        type -> TestSchema.builder(bridge)
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn("c1", bridge.set(type))
                                                          .withColumn("c2", bridge.set(type))
                                                          .withColumn("c3", bridge.set(type)));
    }

    // validate that cell deletions in a complex data can be correctly encoded.
    private void testElementDeletionInCollection(CassandraBridge bridge,
                                                 CdcBridge cdcBridge,
                                                 Path directory,
                                                 int numOfPKs,
                                                 int numOfColumns,
                                                 List<String> collectionColumnNames,
                                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, byte[]> elementDeletionIndices = new HashMap<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsMapKey)
            .checkAssert(
            type -> testWith(bridge, cdcBridge, directory, schemaBuilder.apply(type))
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
                                    Object value = TestUtils.collectionDeleteMutation(bridge.getVersion(), key);
                                    testRow = testRow.copy(name, value);
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
                            assertThat(lmtInMillis)
                                .as("Last modification time should have a lower bound of " + minTimestamp)
                                .isGreaterThanOrEqualTo(minTimestamp);
                            assertThat(event.getPartitionKeys())
                                .as("Regardless of being row deletion or not, the partition key must present")
                                .hasSize(numOfPKs);
                            assertThat(event.getClusteringKeys()).isNull();
                            assertThat(event.getStaticColumns()).isNull();

                            if (elementDeletionIndices.containsKey(i)) // verify deletion
                            {
                                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
                                Map<String, List<ByteBuffer>> cellTombstonesPerCol = event.getTombstonedCellsInComplex();
                                assertThat(cellTombstonesPerCol).isNotNull();
                                Map<String, Value> valueColMap = event.getValueColumns()
                                                                      .stream()
                                                                      .collect(Collectors.toMap(value -> value.columnName, Function.identity()));
                                for (String name : collectionColumnNames)
                                {
                                    assertThat(valueColMap.get(name).getValue())
                                        .as("Collection column's value should be null since only deletion applies")
                                        .isNull();
                                    assertThat(cellTombstonesPerCol.get(name)).isNotNull();
                                    List<ByteBuffer> deletedCellKeys = cellTombstonesPerCol.get(name);
                                    assertThat(deletedCellKeys).hasSize(1);
                                    assert deletedCellKeys.get(0).hasArray();
                                    byte[] keyBytesRead = deletedCellKeys.get(0).array();
                                    assertThat(keyBytesRead)
                                        .as("The key encoded should be the same")
                                        .isEqualTo(elementDeletionIndices.get(i));
                                }
                            }
                            else // verify update
                            {
                                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
                                assertThat(event.getValueColumns()).isNotNull();
                            }
                        }
                    })
                    .run());
    }
}
