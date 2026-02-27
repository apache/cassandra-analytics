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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.api.RangeTombstoneData;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.RangeTombstone;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ComparisonUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class RangeDeletionTests extends CdcTestBase
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRangeDeletions(CassandraVersion version)
    {
        testRangeDeletions(bridge, cdcBridge,
                           false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder(bridge)
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
        testRangeDeletions(bridge, cdcBridge,
                           false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder(bridge)
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRangeDeletionsWithStatic(CassandraVersion version)
    {
        testRangeDeletions(bridge, cdcBridge,
                           true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder(bridge)
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
        testRangeDeletions(bridge, cdcBridge,
                           true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder(bridge)
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
    }

    // validate that range deletions can be correctly encoded.
    private void testRangeDeletions(CassandraBridge bridge,
                                    CdcBridge cdcBridge,
                                    boolean hasStatic,
                                    int numOfPartitionKeys,
                                    int numOfClusteringKeys,
                                    boolean withOpenEnd,
                                    Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        Preconditions.checkArgument(numOfClusteringKeys > 0, "Range deletion test won't run without having clustering keys!");
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        Map<Integer, TestSchema.TestRow> rangeTombstones = new HashMap<>();
        long minTimestamp = System.currentTimeMillis();
        int numRows = 1000;
        qt().forAll(cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(
            type ->
            testWith(bridge, cdcBridge, commitLogDir, schemaBuilder.apply(type))
            .withAddLastModificationTime(true)
            .clearWriters()
            .withNumRows(numRows)
            .withWriter(rangeDeletionWriter(rangeTombstones, numOfPartitionKeys, numOfClusteringKeys, withOpenEnd, minTimestamp))
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
                        .hasSize(numOfPartitionKeys);

                    if (rangeTombstones.containsKey(i)) // verify deletion
                    {
                        assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.RANGE_DELETE);
                        // the bounds are added in its dedicated column.
                        assertThat(event.getClusteringKeys())
                            .as("Clustering keys should be absent for range deletion")
                            .isNull();
                        assertThat(event.getStaticColumns()).isNull();
                        List<RangeTombstone> rangeTombstoneList = event.getRangeTombstoneList();
                        assertThat(rangeTombstoneList).isNotNull();
                        assertThat(rangeTombstoneList)
                            .as("There should be 1 range tombstone")
                            .hasSize(1);
                        TestSchema.TestRow sourceRow = rangeTombstones.get(i);
                        RangeTombstoneData expectedRT = sourceRow.rangeTombstones().get(0);
                        RangeTombstone rt = rangeTombstoneList.get(0);
                        assertThat(rt.startInclusive).isEqualTo(expectedRT.open.inclusive);
                        assertThat(rt.endInclusive).isEqualTo(expectedRT.close.inclusive);
                        assertThat(rt.getStartBound()).hasSize(numOfClusteringKeys);
                        assertThat(rt.getEndBound()).hasSize(withOpenEnd ? numOfClusteringKeys - 1 : numOfClusteringKeys);
                        Object[] startBoundVals = rt.getStartBound().stream()
                                                    .map(v -> v.getCqlType(bridge::parseType)
                                                               .deserializeToJavaType(v.getValue()))
                                                    .toArray();
                        assertComparisonEquals(expectedRT.open.values, startBoundVals);

                        Object[] endBoundVals = rt.getEndBound().stream()
                                                  .map(v -> v.getCqlType(bridge::parseType)
                                                             .deserializeToJavaType(v.getValue()))
                                                  .toArray();
                        // The range bound in mutation does not encode the null value.
                        // We need to get rid of the null in the test value array
                        Object[] expectedCloseVals = withOpenEnd
                                                     ? new Object[numOfClusteringKeys - 1]
                                                     : expectedRT.close.values;
                        System.arraycopy(expectedRT.close.values, 0,
                                         expectedCloseVals, 0, expectedCloseVals.length);
                        assertComparisonEquals(expectedCloseVals, endBoundVals);
                    }
                    else // verify update
                    {
                        assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
                        assertThat(event.getClusteringKeys()).isNotNull();
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
            .run());
    }

    public static CdcWriter rangeDeletionWriter(Map<Integer, TestSchema.TestRow> rangeTombstones,
                                                int numOfPartitionKeys,
                                                int numOfClusteringKeys,
                                                boolean withOpenEnd,
                                                long minTimestamp)
    {
        return (tester, rows, writer) -> {
            long timestamp = minTimestamp;
            rangeTombstones.clear();
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow;
                if (ThreadLocalRandom.current().nextDouble() < 0.5)
                {
                    testRow = CdcTester.newUniqueRow(tester.schema, rows);
                    Object[] baseBound = testRow.rawValues(numOfPartitionKeys, numOfPartitionKeys + numOfClusteringKeys);
                    // create a new bound that has the last CK value different from the base bound
                    Object[] newBound = new Object[baseBound.length];
                    System.arraycopy(baseBound, 0, newBound, 0, baseBound.length);
                    TestSchema.TestRow newRow = CdcTester.newUniqueRow(tester.schema, rows);
                    int lastCK = newBound.length - 1;
                    newBound[lastCK] = newRow.get(numOfPartitionKeys + numOfClusteringKeys - 1);
                    Object[] open;
                    Object[] close;
                    // the field's corresponding java type should be comparable... (ugly :()
                    if (((Comparable<Object>) baseBound[lastCK]).compareTo(newBound[lastCK]) < 0) // for queries like WHERE ck > 1 AND ck < 2
                    {
                        open = baseBound;
                        close = newBound;
                    }
                    else
                    {
                        open = newBound;
                        close = baseBound;
                    }
                    if (withOpenEnd) // for queries like WHERE ck > 1
                    {
                        close[lastCK] = null;
                    }
                    testRow.setRangeTombstones(Arrays.asList(
                    new RangeTombstoneData(new RangeTombstoneData.Bound(open, true), new RangeTombstoneData.Bound(close, true))));
                    rangeTombstones.put(i, testRow);
                }
                else
                {
                    testRow = CdcTester.newUniqueRow(tester.schema, rows);
                }
                timestamp += 1;
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
            }
        };
    }

    public static void assertComparisonEquals(Object expected, Object actual)
    {
        assertThat(ComparisonUtils.equals(expected, actual))
            .as("Expect %s to equal to %s, but not.", expected, actual)
            .isTrue();
    }
}
