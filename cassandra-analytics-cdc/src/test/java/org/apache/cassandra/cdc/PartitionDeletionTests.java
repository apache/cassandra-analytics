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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ComparisonUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class PartitionDeletionTests extends CdcTestBase
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testPartitionDeletionWithStaticColumn(CassandraVersion version)
    {
        testPartitionDeletion(bridge, cdcBridge,
                              true, // has static columns
                              true, // has clustering key
                              1, // partition key columns
                              type -> TestSchema.builder(bridge)
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withStaticColumn("sc", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testPartitionDeletionWithoutCK(CassandraVersion version)
    {
        testPartitionDeletion(bridge, cdcBridge,
                              false, // has static columns
                              false, // has clustering key
                              3, // partition key columns
                              type -> TestSchema.builder(bridge)
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withPartitionKey("pk3", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testPartitionDeletionWithCompositePK(CassandraVersion version)
    {
        testPartitionDeletion(bridge, cdcBridge,
                              false, // has static columns
                              true, // has clustering key
                              2, // partition key columns
                              type -> TestSchema.builder(bridge)
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    // At most can have 1 clustering key when `hasClustering` is true.
    private void testPartitionDeletion(CassandraBridge bridge,
                                       CdcBridge cdcBridge,
                                       boolean hasStatic,
                                       boolean hasClustering,
                                       int partitionKeys,
                                       Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write partition-level tombstones
        // The expected output should include the values of all partition keys but all other columns should be null,
        // i.e. [pk.., null..]. The bitset should indicate that only the partition keys are present.
        // This kind of output means the entire partition is deleted
        final Set<Integer> partitionDeletionIndices = new HashSet<>();
        final List<List<Object>> validationPk = new ArrayList<>(); // pk of the partition deletions
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(type -> {
                testWith(bridge, cdcBridge, commitLogDir, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    partitionDeletionIndices.clear();
                    validationPk.clear();
                    long timestamp = minTimestamp;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow;
                        if (rnd.nextDouble() < 0.5)
                        {
                            testRow = CdcTester.newUniquePartitionDeletion(tester.schema, rows);
                            List<Object> pk = new ArrayList<>(partitionKeys);
                            for (int j = 0; j < partitionKeys; j++)
                            {
                                pk.add(testRow.get(j));
                            }
                            validationPk.add(pk);
                            partitionDeletionIndices.add(i);
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
                    for (int i = 0, pkValidationIdx = 0; i < events.size(); i++)
                    {
                        CdcEvent event = events.get(i);
                        long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                        assertThat(lmtInMillis)
                            .as("Last modification time should have a lower bound of " + minTimestamp)
                            .isGreaterThanOrEqualTo(minTimestamp);
                        assertThat(event.getPartitionKeys())
                            .as("Regardless of being row deletion or not, the partition key must present")
                            .hasSize(partitionKeys);

                        if (partitionDeletionIndices.contains(i)) // verify partition deletion
                        {
                            assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.PARTITION_DELETE);
                            assertThat(event.getClusteringKeys())
                                .as("Partition deletion has no clustering keys")
                                .isNull();

                            assertThat(event.getStaticColumns()).isNull();
                            assertThat(event.getValueColumns()).isNull();

                            List<Object> testPKs = event.getPartitionKeys().stream()
                                                        .map(v -> {
                                                            CqlField.CqlType cqlType = v.getCqlType(bridge::parseType);
                                                            return cqlType.deserializeToJavaType(v.getValue());
                                                        })
                                                        .collect(Collectors.toList());

                            List<Object> expectedPK = validationPk.get(pkValidationIdx++);
                            assertThat(ComparisonUtils.equals(expectedPK.toArray(), testPKs.toArray()))
                                .as("Partition deletion should indicate the correct partition at row" + i +
                                    ". Expected: " + expectedPK + ", actual: " + testPKs)
                                .isTrue();
                        }
                        else // verify update
                        {
                            assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
                            if (hasClustering)
                            {
                                assertThat(event.getClusteringKeys()).isNotNull();
                            }
                            else
                            {
                                assertThat(event.getClusteringKeys()).isNull();
                            }
                            if (hasStatic)
                            {
                                assertThat(event.getStaticColumns()).isNotNull();
                            }
                            else
                            {
                                assertThat(event.getStaticColumns()).isNull();
                            }
                            assertThat(event.getValueColumns()).isNotNull();
                        }
                    }
                })
                .run();
            });
    }
}
