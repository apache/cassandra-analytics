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
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.RangeTombstoneData;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.RangeTombstone;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ComparisonUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.CdcTester.testWith;
import static org.apache.cassandra.cdc.CdcTests.BRIDGE;
import static org.apache.cassandra.cdc.CdcTests.directory;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class RangeDeletionTests
{
    @Test
    public void testRangeDeletions()
    {
        testRangeDeletions(false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder(BRIDGE)
                                             .withPartitionKey("pk1", BRIDGE.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", BRIDGE.bigint())
                                             .withColumn("c1", type));
        testRangeDeletions(false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder(BRIDGE)
                                             .withPartitionKey("pk1", BRIDGE.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", BRIDGE.bigint())
                                             .withColumn("c1", type));
    }

    @Test
    public void testRangeDeletionsWithStatic()
    {
        testRangeDeletions(true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder(BRIDGE)
                                             .withPartitionKey("pk1", BRIDGE.uuid())
                                             .withClusteringKey("ck1", BRIDGE.ascii())
                                             .withClusteringKey("ck2", BRIDGE.bigint())
                                             .withStaticColumn("s1", BRIDGE.uuid())
                                             .withColumn("c1", type));
        testRangeDeletions(true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder(BRIDGE)
                                             .withPartitionKey("pk1", BRIDGE.uuid())
                                             .withClusteringKey("ck1", BRIDGE.ascii())
                                             .withClusteringKey("ck2", BRIDGE.bigint())
                                             .withStaticColumn("s1", BRIDGE.uuid())
                                             .withColumn("c1", type));
    }

    // validate that range deletions can be correctly encoded.
    private void testRangeDeletions(boolean hasStatic,
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
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            type ->
            testWith(BRIDGE, directory, schemaBuilder.apply(type))
            .withAddLastModificationTime(true)
            .clearWriters()
            .withNumRows(numRows)
            .withWriter(rangeDeletionWriter(rangeTombstones, numOfPartitionKeys, numOfClusteringKeys, withOpenEnd, minTimestamp))
            .withCdcEventChecker((testRows, events) -> {
                for (int i = 0; i < events.size(); i++)
                {
                    CdcEvent event = events.get(i);
                    long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                    assertTrue(lmtInMillis >= minTimestamp,
                               "Last modification time should have a lower bound of " + minTimestamp);
                    assertEquals(numOfPartitionKeys, event.getPartitionKeys().size(),
                                 "Regardless of being row deletion or not, the partition key must present");

                    if (rangeTombstones.containsKey(i)) // verify deletion
                    {
                        assertEquals(CdcEvent.Kind.RANGE_DELETE, event.getKind());
                        // the bounds are added in its dedicated column.
                        assertNull(event.getClusteringKeys(), "Clustering keys should be absent for range deletion");
                        assertNull(event.getStaticColumns());
                        List<RangeTombstone> rangeTombstoneList = event.getRangeTombstoneList();
                        assertNotNull(rangeTombstoneList);
                        assertEquals(1, rangeTombstoneList.size(), "There should be 1 range tombstone");
                        TestSchema.TestRow sourceRow = rangeTombstones.get(i);
                        RangeTombstoneData expectedRT = sourceRow.rangeTombstones().get(0);
                        RangeTombstone rt = rangeTombstoneList.get(0);
                        assertEquals(expectedRT.open.inclusive, rt.startInclusive);
                        assertEquals(expectedRT.close.inclusive, rt.endInclusive);
                        assertEquals(numOfClusteringKeys, rt.getStartBound().size());
                        assertEquals(withOpenEnd ? numOfClusteringKeys - 1 : numOfClusteringKeys,
                                     rt.getEndBound().size());
                        Object[] startBoundVals = rt.getStartBound().stream()
                                                    .map(v -> v.getCqlType(BRIDGE::parseType)
                                                               .deserializeToJavaType(v.getValue()))
                                                    .toArray();
                        assertComparisonEquals(expectedRT.open.values, startBoundVals);

                        Object[] endBoundVals = rt.getEndBound().stream()
                                                  .map(v -> v.getCqlType(BRIDGE::parseType)
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
                        assertEquals(CdcEvent.Kind.INSERT, event.getKind());
                        assertNotNull(event.getClusteringKeys());
                        if (hasStatic)
                        {
                            assertNotNull(event.getStaticColumns());
                        }
                        else
                        {
                            assertNull(event.getStaticColumns());
                        }
                        assertNotNull(event.getValueColumns());
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
        assertTrue(ComparisonUtils.equals(expected, actual),
                   String.format("Expect %s to equal to %s, but not.", expected, actual));
    }
}
