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

package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.RangeTombstone;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.utils.ComparisonUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import scala.collection.mutable.AbstractSeq;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.mutableSeqAsJavaList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;

@Disabled
public class CdcTombstoneTests extends VersionRunner
{
    @TempDir
    public static Path DIRECTORY;  // CHECKSTYLE IGNORE: Constant cannot be made final
    private CassandraBridge bridge;

    @BeforeEach
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void setup(CassandraBridge bridge)
    {
        this.bridge = bridge;
        CdcTester.setup(bridge, DIRECTORY);
    }

    @AfterEach
    public void tearDown()
    {
        CdcTester.tearDown();
    }

    private static void assertEqualsWithMessage(Object expected, Object actual)
    {
        boolean actual1 = ComparisonUtils.equals(expected, actual);
        assertTrue(actual1, String.format("Expect %s to equal to %s, but not.", expected, actual));
    }

    @Test
    public void testCellDeletion()
    {
        // The test write cell-level tombstones, i.e. deleting one or more columns in a row, for CDC job to aggregate
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester
                .builder(bridge, DIRECTORY, TestSchema.builder()
                                                      .withPartitionKey("pk", bridge.uuid())
                                                      .withColumn("c1", bridge.bigint())
                                                      .withColumn("c2", type)
                                                      .withColumn("c3", bridge.list(type)))
                .clearWriters()
                .withWriter((tester, rows, writer) -> {
                    for (int row = 0; row < tester.numRows; row++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER);  // Mark c1 as not updated / unset
                        testRow = testRow.copy("c2", null);                          // Delete c2
                        testRow = testRow.copy("c3", null);                          // Delete c3
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                    }
                })
                .withRowChecker(sparkRows -> {
                    for (Row row : sparkRows)
                    {
                        byte[] updatedFieldsIndicator = (byte[]) row.get(4);
                        BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                        BitSet expected = new BitSet(4);
                        expected.set(0);  // Expecting pk to be set...
                        expected.set(2);  // ... and c2 to be set
                        expected.set(3);  // ... and c3 to be set
                        assertEquals(expected, bs);
                        // pk should be set
                        Object actual = row.get(0);
                        assertNotNull(actual, "pk should not be null");
                        // null due to unset
                        Object actual3 = row.get(1);
                        assertNull(actual3, "c1 should be null");
                        // null due to deletion
                        Object actual2 = row.get(2);
                        assertNull(actual2, "c2 should be null");
                        // null due to deletion
                        Object actual1 = row.get(3);
                        assertNull(actual1, "c3 should be null");
                    }
                })
                .run());
    }

    @Test
    public void testRowDeletionWithClusteringKeyAndStatic()
    {
        testRowDeletion(5,     // Number of columns
                        true,  // Has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withStaticColumn("sc", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testRowDeletionWithClusteringKeyNoStatic()
    {
        testRowDeletion(4,     // Number of columns
                        true,  // Has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testRowDeletionSimpleSchema()
    {
        testRowDeletion(3,      // Number of columns
                        false,  // Has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    private void testRowDeletion(int numOfColumns,
                                 boolean hasClustering,
                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write row-level tombstones.
        // The expected output should include the values of all primary keys but all other columns should be null,
        // i.e. [pk.., ck.., null..]. The bitset should indicate that only the primary keys are present.
        // This kind of output means the entire row is deleted.
        Set<Integer> rowDeletionIndices = new HashSet<>();
        Random random = new Random(1);
        long minTimestamp = System.currentTimeMillis();
        int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester
                .builder(bridge, DIRECTORY, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    rowDeletionIndices.clear();
                    long timestamp = minTimestamp;
                    for (int row = 0; row < tester.numRows; row++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        if (random.nextDouble() < 0.5)
                        {
                            testRow.delete();
                            rowDeletionIndices.add(row);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                // Disable checker on the test row. The check is done in the row checker below.
                .withChecker((testRows, actualRows) -> { })
                .withRowChecker(sparkRows -> {
                    assertEquals(numRows, (Object) sparkRows.size(), "Unexpected number of rows in output");
                    for (int index = 0; index < sparkRows.size(); index++)
                    {
                        Row row = sparkRows.get(index);
                        long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                        assertTrue(lmtInMillis >= minTimestamp, "Last modification time should have a lower bound of " + minTimestamp);
                        byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1);  // Indicator column
                        BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                        BitSet expected = new BitSet(numOfColumns);
                        if (rowDeletionIndices.contains(index))  // Verify row deletion
                        {
                            expected.set(0);    // Expecting pk...
                            if (hasClustering)  // ... and ck to be set
                            {
                                expected.set(1);
                            }
                            assertEquals(expected, bs, "row" + index + " should only have the primary keys to be flagged.");
                            // pk should be set
                            Object actual1 = row.get(0);
                            assertNotNull(actual1, "pk should not be null");
                            if (hasClustering)
                            {
                                // ck should be set
                                Object actual = row.get(1);
                                assertNotNull(actual, "ck should not be null");
                            }
                            for (int colIndex = hasClustering ? 2 : 1; colIndex < numOfColumns; colIndex++)
                            {
                                // null due to row deletion
                                Object actual = row.get(colIndex);
                                assertNull(actual, "None primary key columns should be null");
                            }
                        }
                        else
                        {
                            // Verify update
                            for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                            {
                                expected.set(colIndex);
                                Object actual = row.get(colIndex);
                                assertNotNull(actual, "All column values should exist for full row update");
                            }
                            assertEquals(expected, bs, "row" + index + " should have all columns set");
                        }
                    }
                })
                .run());
    }

    @Test
    public void testPartitionDeletionWithStaticColumn()
    {
        testPartitionDeletion(5,     // Number of columns
                              true,  // Has clustering key
                              1,     // Partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withStaticColumn("sc", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testPartitionDeletionWithCompositePK()
    {
        testPartitionDeletion(5,     // Number of columns
                              true,  // Has clustering key
                              2,     // Partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testPartitionDeletionWithoutCK()
    {
        testPartitionDeletion(5,      // Number of columns
                              false,  // Has clustering key
                              3,      // Partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withPartitionKey("pk3", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    // At most can have 1 clustering key when `hasClustering` is true
    @SuppressWarnings("SameParameterValue")
    private void testPartitionDeletion(int numOfColumns,
                                       boolean hasClustering,
                                       int partitionKeys,
                                       Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write partition-level tombstones.
        // The expected output should include the values of all partition keys but all other columns should be null,
        // i.e. [pk.., null..]. The bitset should indicate that only the partition keys are present.
        // This kind of output means the entire partition is deleted.
        Set<Integer> partitionDeletionIndices = new HashSet<>();
        List<List<Object>> validationPk = new ArrayList<>();  // pk of the partition deletions
        Random random = new Random(1);
        long minTimestamp = System.currentTimeMillis();
        int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester
                .builder(bridge, DIRECTORY, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    partitionDeletionIndices.clear();
                    validationPk.clear();
                    long timestamp = minTimestamp;
                    for (int row = 0; row < tester.numRows; row++)
                    {
                        TestSchema.TestRow testRow;
                        if (random.nextDouble() < 0.5)
                        {
                            testRow = Tester.newUniquePartitionDeletion(tester.schema, rows);
                            List<Object> pk = new ArrayList<>(partitionKeys);
                            for (int key = 0; key < partitionKeys; key++)
                            {
                                pk.add(testRow.get(key));
                            }
                            validationPk.add(pk);
                            partitionDeletionIndices.add(row);
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                // Disable checker on the test row. The check is done in the row checker below.
                .withChecker((testRows, actualRows) -> { })
                .withRowChecker(sparkRows -> {
                    assertEquals(numRows, (Object) sparkRows.size(), "Unexpected number of rows in output");
                    for (int index = 0, pkValidationIdx = 0; index < sparkRows.size(); index++)
                    {
                        Row row = sparkRows.get(index);
                        long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                        assertTrue(lmtInMillis >= minTimestamp, "Last modification time should have a lower bound of " + minTimestamp);
                        byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1);  // Indicator column
                        BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                        BitSet expected = new BitSet(numOfColumns);
                        if (partitionDeletionIndices.contains(index))
                        {
                            // Verify partition deletion
                            List<Object> pk = new ArrayList<>(partitionKeys);
                            // Expecting partition keys
                            for (int key = 0; key < partitionKeys; key++)
                            {
                                expected.set(key);
                                Object actual = row.get(key);
                                assertNotNull(actual, "partition key should not be null");
                                pk.add(row.get(key));
                            }
                            assertEquals(expected, bs, "row" + index + " should only have only the partition keys to be flagged.");
                            List<Object> expectedPK = validationPk.get(pkValidationIdx++);
                            boolean actual1 = ComparisonUtils.equals(expectedPK.toArray(), pk.toArray());
                            assertTrue(actual1, "Partition deletion should indicate the correct partition at row" + index + ". "
                                                           + "Expected: " + expectedPK + ", actual: " + pk);
                            if (hasClustering)
                            {
                                // ck should be set
                                Object actual = row.get(partitionKeys);
                                assertNull(actual, "ck should be null at row" + index);
                            }
                            for (int colIndex = partitionKeys; colIndex < numOfColumns; colIndex++)
                            {
                                // null due to partition deletion
                                Object actual = row.get(colIndex);
                                assertNull(actual, "None partition key columns should be null at row" + 1);
                            }
                        }
                        else
                        {
                            // Verify update
                            for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                            {
                                expected.set(colIndex);
                                Object actual = row.get(colIndex);
                                assertNotNull(actual, "All column values should exist for full row update");
                            }
                            assertEquals(expected, bs, "row" + index + " should have all columns set");
                        }
                    }
                })
                .run());
    }

    @Test
    public void testElementDeletionInMap()
    {
        String name = "m";
        testElementDeletionInCollection(2,  // Number of columns
                                        ImmutableList.of(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.map(type, type)));
    }

    @Test
    public void testElementDeletionInSet()
    {
        String name = "s";
        testElementDeletionInCollection(2,  // Number of columns
                                        ImmutableList.of(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.set(type)));
    }

    @Test
    public void testElementDeletionsInMultipleColumns()
    {
        testElementDeletionInCollection(4,  // Number of columns
                                        ImmutableList.of("c1", "c2", "c3"),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn("c1", bridge.set(type))
                                                          .withColumn("c2", bridge.set(type))
                                                          .withColumn("c3", bridge.set(type)));
    }

    // Validate that cell deletions in a complex data can be correctly encoded
    private void testElementDeletionInCollection(int numOfColumns,
                                                 List<String> collectionColumnNames,
                                                 Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // Key: row# that has deletion; value: the deleted cell key/path in the collection
        Map<Integer, byte[]> elementDeletionIndices = new HashMap<>();
        Random random = new Random(1);
        long minTimestamp = System.currentTimeMillis();
        int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester
                .builder(bridge, DIRECTORY, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    elementDeletionIndices.clear();
                    long timestamp = minTimestamp;
                    for (int row = 0; row < tester.numRows; row++)
                    {
                        int ignoredSize = 10;
                        TestSchema.TestRow testRow;
                        if (random.nextDouble() < 0.5)
                        {
                            // NOTE: This is a little hacky: for simplicity,
                            //       all collections in the row has the SAME entry being deleted
                            ByteBuffer key = type.serialize(type.randomValue(ignoredSize));
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                            for (String name : collectionColumnNames)
                            {
                                testRow = testRow.copy(name, bridge.deletedCollectionElement(key));
                            }
                            elementDeletionIndices.put(row, key.array());
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                // Disable checker on the test row. The check is done in the sparkrow checker below.
                .withChecker((testRows, actualRows) -> { })
                .withRowChecker(sparkRows -> {
                    assertEquals(numRows, (Object) sparkRows.size(), "Unexpected number of rows in output");
                    for (int rowIndex = 0; rowIndex < sparkRows.size(); rowIndex++)
                    {
                        Row row = sparkRows.get(rowIndex);
                        long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                        assertTrue(lmtInMillis >= minTimestamp, "Last modification time should have a lower bound of " + minTimestamp);
                        byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1);  // Indicator column
                        BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                        BitSet expected = new BitSet(numOfColumns);
                        for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++)
                        {
                            expected.set(columnIndex);
                        }
                        if (elementDeletionIndices.containsKey(rowIndex))
                        {
                            // Verify deletion
                            Map<Object, Object> cellTombstonesPerColumn = row.getJavaMap(numOfColumns + 3);  // Cell deletion in complex
                            assertNotNull(cellTombstonesPerColumn);
                            for (String name : collectionColumnNames)
                            {
                                Object actual1 = row.get(row.fieldIndex(name));
                                assertNull(actual1, "Collection column should be null after deletion");

                                Object actual = cellTombstonesPerColumn.get(name);
                                assertNotNull(actual);
                                List<?> deletedCellKeys =
                                        mutableSeqAsJavaList((AbstractSeq<?>) cellTombstonesPerColumn.get(name));
                                assertEquals(1, (Object) deletedCellKeys.size());
                                byte[] keyBytesRead = (byte[]) deletedCellKeys.get(0);
                                assertArrayEquals(elementDeletionIndices.get(rowIndex), keyBytesRead,
                                                  "The key encoded should be the same");
                            }
                        }
                        else
                        {
                            // Verify update
                            for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                            {
                                Object actual = row.get(colIndex);
                                assertNotNull(actual, "All column values should exist for full row update");
                            }
                            Object actual = row.get(numOfColumns + 3);
                            assertNull(actual, "the cell deletion map should be absent");
                        }
                        assertEquals(expected, bs, "row" + rowIndex + " should have all columns set");
                    }
                })
                .run());
    }

    @Test
    public void testRangeDeletions()
    {
        testRangeDeletions(4,     // Number of columns
                           1,     // Number of partition key columns
                           2,     // Number of clustering key columns
                           true,  // Open end
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
        testRangeDeletions(4,      // Number of columns
                           1,      // Number of partition key columns
                           2,      // Number of clustering key columns
                           false,  // Open end
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
    }

    @Test
    public void testRangeDeletionsWithStatic()
    {
        testRangeDeletions(5,     // Number of columns
                           1,     // Number of partition key columns
                           2,     // Number of clustering key columns
                           true,  // Open end
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
        testRangeDeletions(5,      // Number of columns
                           1,      // Number of partition key columns
                           2,      // Number of clustering key columns
                           false,  // Open end
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
    }

    // Validate that range deletions can be correctly encoded
    // CHECKSTYLE IGNORE: Long method
    @SuppressWarnings({"unchecked", "SameParameterValue"})
    private void testRangeDeletions(int numOfColumns,
                                    int numOfPartitionKeys,
                                    int numOfClusteringKeys,
                                    boolean withOpenEnd,
                                    Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        Preconditions.checkArgument(0 < numOfClusteringKeys,
                                    "Range deletion test won't run without having clustering keys!");
        // Key: row# that has deletion; value: the deleted cell key/path in the collection
        Map<Integer, TestSchema.TestRow> rangeTombestones = new HashMap<>();
        Random random = new Random(1);
        long minTimestamp = System.currentTimeMillis();
        int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester
                .builder(bridge, DIRECTORY, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    long timestamp = minTimestamp;
                    rangeTombestones.clear();
                    for (int row = 0; row < tester.numRows; row++)
                    {
                        TestSchema.TestRow testRow;
                        if (random.nextDouble() < 0.5)
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                            Object[] baseBound =
                                    testRow.rawValues(numOfPartitionKeys, numOfPartitionKeys + numOfClusteringKeys);
                            // Create a new bound that has the last ck value different from the base bound
                            Object[] newBound = new Object[baseBound.length];
                            System.arraycopy(baseBound, 0, newBound, 0, baseBound.length);
                            TestSchema.TestRow newRow = Tester.newUniqueRow(tester.schema, rows);
                            int lastCK = newBound.length - 1;
                            newBound[lastCK] = newRow.get(numOfPartitionKeys + numOfClusteringKeys - 1);
                            Object[] open;
                            Object[] close;
                            // The field's corresponding java type should be comparable...
                            if (((Comparable<Object>) baseBound[lastCK]).compareTo(newBound[lastCK]) < 0)  // For queries like WHERE ck > 1 AND ck < 2
                            {
                                open = baseBound;
                                close = newBound;
                            }
                            else
                            {
                                open = newBound;
                                close = baseBound;
                            }
                            if (withOpenEnd)  // For queries like WHERE ck > 1
                            {
                                close[lastCK] = null;
                            }
                            testRow.setRangeTombstones(ImmutableList.of(new RangeTombstone(
                                    new RangeTombstone.Bound(open, true), new RangeTombstone.Bound(close, true))));
                            rangeTombestones.put(row, testRow);
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                // Disable checker on the test row. The check is done in the sparkrow checker below.
                .withChecker((testRows, actualRows) -> { })
                .withSparkRowTestRowsChecker((testRows, sparkRows) -> {
                    assertEquals(numRows, (Object) sparkRows.size(), "Unexpected number of rows in output");
                    for (int rowIndex = 0; rowIndex < sparkRows.size(); rowIndex++)
                    {
                        Row row = sparkRows.get(rowIndex);
                        long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                        assertTrue(minTimestamp <= lmtInMillis, "Last modification time should have a lower bound of " + minTimestamp);
                        byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1);  // Indicator column
                        BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                        BitSet expected = new BitSet(numOfColumns);
                        if (rangeTombestones.containsKey(rowIndex))  // Verify deletion
                        {
                            for (int column = 0; column < numOfColumns; column++)
                            {
                                if (column < numOfPartitionKeys)
                                {
                                    Object actual = row.get(column);
                                    assertNotNull(actual, "All partition keys should exist for range tombstone");
                                    expected.set(column);
                                }
                                else
                                {
                                    Object actual = row.get(column);
                                    assertNull(actual, "Non-partition key columns should be null");
                                }
                                Object deletionColumn = row.get(numOfColumns + 4);  // Range deletion column
                                assertNotNull(null, (String) deletionColumn);
                                List<?> tombstones = mutableSeqAsJavaList((AbstractSeq<?>) deletionColumn);
                                assertEquals(1, (Object) tombstones.size(), "There should be 1 range tombstone");
                                TestSchema.TestRow sourceRow = rangeTombestones.get(rowIndex);
                                RangeTombstone expectedTombstone = sourceRow.rangeTombstones().get(0);
                                Row rangeTombstone = (Row) tombstones.get(0);
                                Object actual = rangeTombstone.length();
                                assertEquals(4, actual, "Range tombstone should have 4 fields");
                                Object actual4 = rangeTombstone.getAs("StartInclusive");
                                assertEquals(expectedTombstone.open.inclusive, actual4);
                                Object actual3 = rangeTombstone.getAs("EndInclusive");
                                assertEquals(expectedTombstone.close.inclusive, actual3);
                                Row open = rangeTombstone.getAs("Start");
                                Object actual2 = open.length();
                                assertEquals(numOfClusteringKeys, actual2);
                                Row close = rangeTombstone.getAs("End");
                                Object actual1 = close.length();
                                assertEquals(numOfClusteringKeys, actual1);
                                for (int keyIndex = 0; keyIndex < numOfClusteringKeys; keyIndex++)
                                {
                                    assertEqualsWithMessage(expectedTombstone.open.values[keyIndex],
                                                            open.get(keyIndex));
                                    assertEqualsWithMessage(expectedTombstone.close.values[keyIndex],
                                                            close.get(keyIndex));
                                }
                            }
                        }
                        else  // Verify update
                        {
                            for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++)
                            {
                                Object actual = row.get(columnIndex);
                                assertNotNull(actual, "All column values should exist for full row update");
                                expected.set(columnIndex);
                            }
                            Object actual = row.get(numOfColumns + 3);
                            assertNull(actual, "the cell deletion map should be absent");
                        }
                        assertEquals(expected, bs, "row" + rowIndex + " should have the expected columns set");
                    }
                })
                .run());
    }
}
