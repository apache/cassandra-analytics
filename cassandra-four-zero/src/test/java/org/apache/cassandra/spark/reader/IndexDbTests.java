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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.jetbrains.annotations.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.constant;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.maps;

public class IndexDbTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    private static final class IndexRow implements Comparable<IndexRow>
    {
        private final BigInteger token;
        private final int value;
        private int position = 0;

        IndexRow(IPartitioner partitioner, int value)
        {
            this.token = token(partitioner, value);
            this.value = value;
        }

        public int compareTo(@NotNull IndexRow that)
        {
            return this.token.compareTo(that.token);
        }
    }

    @Test
    @SuppressWarnings("static-access")
    public void testSearchIndex()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.basicBuilder(BRIDGE).withCompression(false).build();
                    IPartitioner iPartitioner = BRIDGE.getPartitioner(partitioner);
                    int numRows = 5000;

                    // Write an SSTable and record token
                    List<BigInteger> tokens = new ArrayList<>(numRows);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < numRows; row++)
                        {
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

                    int rowSize = 39;
                    int sample = 4;
                    // Sample the token list and read offset in Index.db for sampled list and verify the
                    // offset matches the expected we sample the list as IndexDbUtils.findStartOffset(...)
                    // returns the previous offset, so we want to test it correctly skips tokens less than
                    // the token we are looking for before returning
                    List<BigInteger> sparseList = IntStream.range(0, tokens.size())
                                                           .filter(index -> index > 0 && index % sample == 0)
                                                           .mapToObj(tokens::get)
                                                           .collect(Collectors.toList());
                    assertEquals((numRows / 4) - 1, sparseList.size());
                    try (DataInputStream in = new DataInputStream(Objects.requireNonNull(ssTable.openPrimaryIndexStream())))
                    {
                        try
                        {
                            for (int index = 0; index < sparseList.size(); index++)
                            {
                                BigInteger token = sparseList.get(index);
                                long expectedOffset = (((index + 1L) * sample) - 1) * rowSize;
                                long offset = IndexDbUtils.findStartOffset(in,
                                                                           iPartitioner,
                                                                           TokenRange.singleton(token),
                                                                           Stats.DoNothingStats.INSTANCE);
                                assertEquals(expectedOffset, offset);
                                ReaderUtils.skipRowIndexEntry(in);
                            }
                        }
                        catch (EOFException ignore)
                        {
                        }
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    @SuppressWarnings("static-access")
    public void testFindStartEndOffset()
    {
        int numValues = 5000;
        qt().forAll(arbitrary().enumValues(Partitioner.class),                           // Partitioner
                    maps().of(integers().allPositive(), constant(1)).ofSize(numValues),  // Unique keys (ignore values)
                    integers().between(1, numValues - 1))                                // Start position
            .checkAssert((partitioner, rawValues, startPos) -> {
                    IPartitioner iPartitioner = BRIDGE.getPartitioner(partitioner);
                    int rowSize = 256;

                    // Generate random index row values and sort by token
                    IndexRow[] rows = rawValues.keySet().stream()
                                                        .map(value -> new IndexRow(iPartitioner, value))
                                                        .sorted()
                                                        .toArray(IndexRow[]::new);
                    IntStream.range(0, rows.length).forEach(index -> rows[index].position = index * rowSize);  // Update position offset
                    IndexRow startRow = rows[startPos];
                    int[] valuesAndOffsets = Arrays.stream(rows)
                                                   .map(row -> new int[]{row.value, row.position})
                                                   .flatMapToInt(Arrays::stream)
                                                   .toArray();

                    try (DataInputStream in = mockDataInputStream(valuesAndOffsets))
                    {
                        long startOffset = IndexDbUtils.findStartOffset(in,
                                                                        iPartitioner,
                                                                        TokenRange.singleton(startRow.token),
                                                                        Stats.DoNothingStats.INSTANCE);
                        assertEquals(rows[startPos - 1].position, startOffset);
                        ReaderUtils.skipRowIndexEntry(in);
                    }
                    catch (IOException exception)
                    {
                        throw new RuntimeException(exception);
                    }
                });
    }

    @Test
    @SuppressWarnings("static-access")
    public void testReadToken()
    {
        qt().withExamples(500)
            .forAll(arbitrary().enumValues(Partitioner.class), integers().all())
            .checkAssert((partitioner, value) -> {
                IPartitioner iPartitioner = BRIDGE.getPartitioner(partitioner);
                BigInteger expectedToken = token(iPartitioner, value);
                try (DataInputStream in = mockDataInputStream(value, 0))
                {
                    IndexDbUtils.readNextToken(iPartitioner, in, new Stats()
                    {
                        public void readPartitionIndexDb(ByteBuffer key, BigInteger token)
                        {
                            assertEquals(value.intValue(), key.getInt());
                            assertEquals(expectedToken, token);
                        }
                    });
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            }
        );
    }

    @Test
    public void testLessThan()
    {
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), TokenRange.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), TokenRange.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));

        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), TokenRange.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), TokenRange.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));

        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), TokenRange.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), TokenRange.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
    }

    private static BigInteger token(IPartitioner iPartitioner, int value)
    {
        // Cast to ByteBuffer required when compiling with Java 8
        return ReaderUtils.tokenToBigInteger(iPartitioner.decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(value).flip()).getToken());
    }

    // Creates an in-memory DataInputStream mocking Index.db bytes, with length (short), key (int), position (vint)
    private static DataInputStream mockDataInputStream(int... valuesAndOffsets) throws IOException
    {
        Preconditions.checkArgument(valuesAndOffsets.length % 2 == 0);
        int numValues = valuesAndOffsets.length / 2;

        int size = (numValues * 7);  // 2 bytes short length, 4 bytes partition key value, 1 byte promoted index
        size += IntStream.range(0, valuesAndOffsets.length)  // Variable int for position offset
                         .filter(index -> (index + 1) % 2 == 0)
                         .map(index -> valuesAndOffsets[index])
                         .map(ReaderUtils::vIntSize)
                         .sum();

        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int index = 0; index < valuesAndOffsets.length; index += 2)
        {
            buffer.putShort((short) 4)
               .putInt(valuesAndOffsets[index]);  // Value
            ReaderUtils.writePosition(valuesAndOffsets[index + 1], buffer);  // Write variable int position offset
            ReaderUtils.writePosition(0L, buffer);  // Promoted index
        }

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        return new DataInputStream(new ByteArrayInputStream(bytes));
    }
}
