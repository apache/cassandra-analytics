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

import static org.assertj.core.api.Assertions.assertThat;
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
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);
                    Collections.sort(tokens);

                    TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
                    assertThat(metadata).as("Could not find table metadata").isNotNull();

                    Path summaryDb = TestSSTable.firstIn(directory.path(), FileType.SUMMARY);
                    assertThat(summaryDb).as("Could not find summary").isNotNull();

                    SSTable ssTable = TestSSTable.firstIn(directory.path());
                    assertThat(ssTable).as("Could not find SSTable").isNotNull();

                    // Binary search Summary.db file in token order and verify offsets are ordered
                    SummaryDbUtils.Summary summary = SummaryDbUtils.readSummary(metadata, ssTable);
                    long previous = -1;
                    for (BigInteger token : tokens)
                    {
                        long offset = SummaryDbUtils.findIndexOffsetInSummary(summary.summary(), iPartitioner, token);
                        if (previous < 0)
                        {
                            assertThat(offset).isEqualTo(0);
                        }
                        else
                        {
                            assertThat(previous).isLessThanOrEqualTo(offset);
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
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(154L))).isEqualTo(148);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(4L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L))).isEqualTo(0);
        for (int token = 5; token < 10000; token++)
        {
            int index = SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(token));
            assertThat(index).isEqualTo(Math.max(0, token - 6));
        }
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10000L))).isEqualTo(9994);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10001L))).isEqualTo(9994);
    }

    @Test
    public void testSummaryBinarySearchSparse()
    {
        SummaryDbUtils.TokenList list = new ArrayTokenList(5L, 10L, 15L, 20L, 25L);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(5L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(6L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10L))).isEqualTo(0);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(11L))).isEqualTo(1);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(13L))).isEqualTo(1);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(15L))).isEqualTo(1);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(16L))).isEqualTo(2);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(25L))).isEqualTo(3);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(26L))).isEqualTo(4);
        assertThat(SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(100L))).isEqualTo(4);
    }
}
