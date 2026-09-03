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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.apache.cassandra.spark.CommonTestUtils.qtRandom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class RowDeletionTests extends CdcTestBase
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRowDeletionWithClusteringKeyAndStatic(CassandraVersion version)
    {
        testRowDeletion(bridge, cdcBridge,
                        true, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder(bridge)
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withStaticColumn("sc", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRowDeletionWithClusteringKeyNoStatic(CassandraVersion version)
    {
        testRowDeletion(bridge, cdcBridge,
                        false, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder(bridge)
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRowDeletionSimpleSchema(CassandraVersion version)
    {
        testRowDeletion(bridge, cdcBridge,
                        false, // has static
                        false, // has clustering key?
                        type -> TestSchema.builder(bridge)
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    private void testRowDeletion(CassandraBridge bridge,
                                 CdcBridge cdcBridge,
                                 boolean hasStatic,
                                 boolean hasClustering,
                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write row-level tombstones
        // The expected output should include the values of all primary keys but all other columns should be null,
        // i.e. [pk.., ck.., null..]. The bitset should indicate that only the primary keys are present.
        // This kind of output means the entire row is deleted
        final Set<UUID> rowDeletionIndices = new HashSet<>();
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert(
            (type, random) -> testWith(bridge, cdcBridge, commitLogDir, schemaBuilder.apply(type))
                    .withAddLastModificationTime(true)
                    .withRandom(random)
                    .clearWriters()
                    .withNumRows(numRows)
                    .withWriter((tester, rows, writer) -> {
                        rowDeletionIndices.clear();
                        long timestamp = minTimestamp;
                        for (int i = 0; i < tester.numRows; i++)
                        {
                            TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                            if (random.nextDouble() < 0.5)
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
                            UUID pk = (UUID) messageConverter.toCdcMessage(event.getPartitionKeys().get(0)).value();
                            assertThat(lmtInMillis)
                            .as("Last modification time should have a lower bound of " + minTimestamp)
                            .isGreaterThanOrEqualTo(minTimestamp);
                            assertThat(event.getPartitionKeys().size())
                            .as("Regardless of being row deletion or not, the partition key must present")
                            .isEqualTo(1);
                            if (hasClustering) // and ck to be set.
                            {
                                assertThat(event.getClusteringKeys().size()).isEqualTo(1);
                            }
                            else
                            {
                                assertThat(event.getClusteringKeys()).isNull();
                            }

                            if (rowDeletionIndices.contains(pk)) // verify row deletion
                            {
                                assertThat(event.getStaticColumns()).as("None primary key columns should be null").isNull();
                                assertThat(event.getValueColumns()).as("None primary key columns should be null").isNull();
                                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.ROW_DELETE);
                            }
                            else // verify update
                            {
                                if (hasStatic)
                                {
                                    assertThat(event.getStaticColumns()).isNotNull();
                                }
                                else
                                {
                                    assertThat(event.getStaticColumns()).isNull();
                                }
                                assertThat(event.getValueColumns()).isNotNull();
                                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
                            }
                        }
                    })
                    .run());
    }
}
