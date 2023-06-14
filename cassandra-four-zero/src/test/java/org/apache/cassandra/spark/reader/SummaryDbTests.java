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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class SummaryDbTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    private static final class ArrayTokenList implements SummaryDbUtils.TokenList
    {
        private final BigInteger[] tokens;

        ArrayTokenList(Long... tokens)
        {
            this(Arrays.stream(tokens)
                       .map(BigInteger::valueOf)
                       .toArray(BigInteger[]::new));
        }

        ArrayTokenList(BigInteger... tokens)
        {
            this.tokens = tokens;
        }

        public int size()
        {
            return tokens.length;
        }

        public BigInteger tokenAt(int index)
        {
            return tokens[index];
        }
    }

    @Test
    @SuppressWarnings("static-access")
    public void testSearchSummary()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.basicBuilder(BRIDGE).withCompression(false).build();
                    IPartitioner iPartitioner = BRIDGE.getPartitioner(partitioner);
                    int numRows = 1000;

                    // Write an SSTable and record token
                    List<BigInteger> tokens = new ArrayList<>(numRows);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < numRows; row++)
                        {
                            // Cast to ByteBuffer required when compiling with Java 8
                            ByteBuffer key = (ByteBuffer) ByteBuffer.allocate(4).putInt(row).flip();
                            BigInteger token = ReaderUtils.tokenToBigInteger(iPartitioner.decorateKey(key).getToken());
                            tokens.add(token);
                            writer.write(row, 0, row);
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));
                    Collections.sort(tokens);

                    TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
                    assertNotNull(metadata, "Could not find table metadata");

                    Path summaryDb = TestSSTable.firstIn(directory.path(), FileType.SUMMARY);
                    assertNotNull(summaryDb, "Could not find summary");

                    SSTable ssTable = TestSSTable.firstIn(directory.path());
                    assertNotNull(ssTable, "Could not find SSTable");

                    // Binary search Summary.db file in token order and verify offsets are ordered
                    SummaryDbUtils.Summary summary = SummaryDbUtils.readSummary(metadata, ssTable);
                    long previous = -1;
                    for (BigInteger token : tokens)
                    {
                        long offset = SummaryDbUtils.findIndexOffsetInSummary(summary.summary(), iPartitioner, token);
                        if (previous < 0)
                        {
                            assertEquals(offset, 0);
                        }
                        else
                        {
                            assertTrue(previous <= offset);
                        }
                        previous = offset;
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSummaryBinarySearch()
    {
        SummaryDbUtils.TokenList list = new ArrayTokenList(LongStream.range(5, 10000).boxed().toArray(Long[]::new));
        assertEquals(148, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(154L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(4L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L)));
        for (int token = 5; token < 10000; token++)
        {
            int index = SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(token));
            assertEquals(Math.max(0, token - 6), index);
        }
        assertEquals(9994, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10000L)));
        assertEquals(9994, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10001L)));
    }

    @Test
    public void testSummaryBinarySearchSparse()
    {
        SummaryDbUtils.TokenList list = new ArrayTokenList(5L, 10L, 15L, 20L, 25L);
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(5L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(6L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(11L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(13L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(15L)));
        assertEquals(2, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(16L)));
        assertEquals(3, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(25L)));
        assertEquals(4, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(26L)));
        assertEquals(4, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(100L)));
    }
}
