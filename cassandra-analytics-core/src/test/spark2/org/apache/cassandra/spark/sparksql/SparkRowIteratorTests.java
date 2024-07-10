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

package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;

public class SparkRowIteratorTests
{
    private static final int NUM_ROWS = 50;

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBasicKeyValue(CassandraBridge bridge)
    {
        // I.e. "create table keyspace.table (a %s, b %s, primary key(a));"
        qt().forAll(TestUtils.versions(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((version, type1, type2) -> runTest(version, TestSchema.builder(bridge)
                    .withPartitionKey("a", type1)
                    .withColumn("b", type2)
                    .build()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultiPartitionKeys(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.versions(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((version, type1, type2, type3) -> runTest(version, TestSchema.builder(bridge)
                    .withPartitionKey("a", type1)
                    .withPartitionKey("b", type2)
                    .withPartitionKey("c", type3)
                    .withColumn("d", bridge.bigint())
                    .build()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBasicClusteringKey(CassandraBridge bridge)
    {
        for (CassandraVersion version : TestUtils.testableVersions())
        {
            qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder())
                .checkAssert((type1, type2, type3, order) -> runTest(version, TestSchema.builder(bridge)
                        .withPartitionKey("a", type1)
                        .withClusteringKey("b", type2)
                        .withColumn("c", type3)
                        .withSortOrder(order)
                        .build()));
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultiClusteringKey(CassandraBridge bridge)
    {
        for (CassandraVersion version : TestUtils.testableVersions())
        {
            qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder(), TestUtils.sortOrder())
                .checkAssert((type1, type2, order1, order2) -> runTest(version, TestSchema.builder(bridge)
                        .withPartitionKey("a", bridge.bigint())
                        .withClusteringKey("b", type1)
                        .withClusteringKey("c", type2)
                        .withColumn("d", bridge.bigint())
                        .withSortOrder(order1)
                        .withSortOrder(order2)
                        .build()));
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdt(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> runTest(CassandraVersion.FOURZERO, TestSchema.builder(bridge)
                    .withPartitionKey("a", bridge.bigint())
                    .withClusteringKey("b", bridge.text())
                    .withColumn("c", bridge.udt("keyspace", "testudt")
                                           .withField("x", type1)
                                           .withField("y", bridge.ascii())
                                           .withField("z", type2)
                                           .build())
                    .build()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTuple(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> runTest(CassandraVersion.FOURZERO, TestSchema.builder(bridge)
                    .withPartitionKey("a", bridge.bigint())
                    .withClusteringKey("b", bridge.text())
                    .withColumn("c", bridge.tuple(bridge.aInt(), type1, bridge.ascii(), type2, bridge.date()))
                    .build()));
    }

    private static void runTest(CassandraVersion version, TestSchema schema)
    {
        runTest(version, schema, schema.randomRows(NUM_ROWS));
    }

    private static void runTest(CassandraVersion version, TestSchema schema, TestSchema.TestRow[] testRows)
    {
        try
        {
            schema.setCassandraVersion(version);
            testRowIterator(version, schema, testRows);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private static void testRowIterator(CassandraVersion version,
                                        TestSchema schema,
                                        TestSchema.TestRow[] testRows) throws IOException
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(version);
        CqlTable cqlTable = schema.buildTable();
        int numRows = testRows.length;
        int numColumns = cqlTable.fields().size() - cqlTable.numPartitionKeys() - cqlTable.numClusteringKeys();
        List<CqlField> columns = cqlTable.fields().stream()
                                                  .filter(field -> !field.isPartitionKey())
                                                  .filter(field -> !field.isClusteringColumn())
                                                  .sorted()
                                                  .collect(Collectors.toList());
        Rid rid = new Rid();
        AtomicInteger rowPos = new AtomicInteger();
        AtomicInteger colPos = new AtomicInteger();

        // Mock data layer
        DataLayer dataLayer = mock(DataLayer.class);
        when(dataLayer.cqlTable()).thenReturn(cqlTable);
        when(dataLayer.version()).thenCallRealMethod();
        when(dataLayer.isInPartition(anyInt(), any(BigInteger.class), any(ByteBuffer.class))).thenReturn(true);
        when(dataLayer.bridge()).thenReturn(bridge);
        when(dataLayer.stats()).thenReturn(Stats.DoNothingStats.INSTANCE);
        when(dataLayer.requestedFeatures()).thenCallRealMethod();

        // Mock scanner
        StreamScanner scanner = mock(StreamScanner.class);
        when(scanner.data()).thenReturn(rid);
        doAnswer(invocation -> {
            int col = colPos.getAndIncrement();
            if (rowPos.get() >= numRows)
            {
                return false;
            }
            TestSchema.TestRow testRow = testRows[rowPos.get()];
            // Write next partition key
            if (col == 0)
            {
                if (cqlTable.numPartitionKeys() == 1)
                {
                    CqlField partitionKey = cqlTable.partitionKeys().get(0);
                    rid.setPartitionKeyCopy(partitionKey.serialize(testRow.get(partitionKey.position())), BigInteger.ONE);
                }
                else
                {
                    assert cqlTable.numPartitionKeys() > 1;
                    ByteBuffer[] partitionBuffers = new ByteBuffer[cqlTable.numPartitionKeys()];
                    int position = 0;
                    for (CqlField partitionKey : cqlTable.partitionKeys())
                    {
                        partitionBuffers[position] = partitionKey.serialize(testRow.get(partitionKey.position()));
                        position++;
                    }
                    rid.setPartitionKeyCopy(ByteBufferUtils.build(false, partitionBuffers), BigInteger.ONE);
                }
            }

            // Write next clustering keys & column name
            CqlField column = columns.get(col);
            ByteBuffer[] colBuffers = new ByteBuffer[cqlTable.numClusteringKeys() + 1];
            int position = 0;
            for (CqlField clusteringColumn : cqlTable.clusteringKeys())
            {
                colBuffers[position] = clusteringColumn.serialize(testRow.get(clusteringColumn.position()));
                position++;
            }
            colBuffers[position] = bridge.ascii().serialize(column.name());
            rid.setColumnNameCopy(ByteBufferUtils.build(false, colBuffers));

            // Write value, timestamp and tombstone
            rid.setValueCopy(column.serialize(testRow.get(column.position())));

            // Move to next row
            if (colPos.get() == numColumns)
            {
                if (rowPos.getAndIncrement() >= numRows)
                {
                    throw new IllegalStateException("Went too far...");
                }
                // Reset column position
                colPos.set(0);
            }

            return true;
        }).when(scanner).next();

        when(dataLayer.openCompactionScanner(anyInt(), anyListOf(PartitionKeyFilter.class), any())).thenReturn(scanner);

        // Use SparkRowIterator and verify values match expected
        SparkRowIterator it = new SparkRowIterator(0, dataLayer);
        int rowCount = 0;
        while (it.next())
        {
            while (rowCount < testRows.length && testRows[rowCount].isTombstone())
            // Skip tombstones
            {
                rowCount++;
            }
            if (rowCount >= testRows.length)
            {
                break;
            }

            TestSchema.TestRow row = testRows[rowCount];
            assertEquals(row, schema.toTestRow(it.get()));
            rowCount++;
        }
        assertEquals(numRows, rowCount);
        it.close();
    }
}
