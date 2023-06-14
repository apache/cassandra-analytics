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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.Throwing;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class SSTableReaderTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableReaderTests.class);
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    private static final int ROWS = 50;
    private static final int COLUMNS = 25;

    @Test
    public void testOpenCompressedRawInputStream()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    // Verify we can open the CompressedRawInputStream and read through the Data.db file
                    Path dataFile = TestSSTable.firstIn(directory.path(), FileType.DATA);
                    Descriptor descriptor = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile.getFileName().toString()));
                    long size = Files.size(dataFile);
                    assertTrue(size > 0);
                    Path compressionFile = TestSSTable.firstIn(directory.path(), FileType.COMPRESSION_INFO);
                    long bytesRead = 0;
                    try (InputStream dis = new BufferedInputStream(Files.newInputStream(dataFile));
                         InputStream cis = new BufferedInputStream(Files.newInputStream(compressionFile));
                         DataInputPlus.DataInputStreamPlus in = new DataInputPlus.DataInputStreamPlus(new DataInputStream(
                             CompressedRawInputStream.fromInputStream(dis, cis, descriptor.version.hasMaxCompressedLength()))))
                    {
                        while (in.read() >= 0)
                        {
                            bytesRead++;
                        }
                    }
                    assertTrue(bytesRead > size);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testOpenSSTableReader()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTableReader reader = openReader(metadata, dataFile);

                    assertNotNull(reader.firstToken());
                    assertNotNull(reader.lastToken());
                    assertNotNull(reader.getSSTableMetadata());
                    assertFalse(reader.isRepaired());
                    assertEquals(ROWS * COLUMNS, countAndValidateRows(reader));
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testFileNameWithoutPrefix()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> writer.write(42, 43, 44));

                    String prefix = schema.keyspace + "-" + schema.table + "-";
                    Files.list(directory.path())
                         .filter(file -> file.getFileName().toString().startsWith(prefix))
                         .forEach(Throwing.consumer(file -> Files.move(file,
                             Paths.get(file.getParent().toString(), file.getFileName().toString().replaceFirst("^" + prefix, "")))));

                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable table = TestSSTable.firstIn(directory.path());
                    openReader(metadata, table);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testFileNameWithPrefix()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> writer.write(42, 43, 44));

                    String prefix = schema.keyspace + "-" + schema.table + "-";
                    Files.list(directory.path())
                         .filter(file -> !file.getFileName().toString().startsWith(prefix))
                         .forEach(Throwing.consumer(file -> Files.move(file,
                             Paths.get(file.getParent().toString(), prefix + file.getFileName().toString()))));

                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable table = TestSSTable.firstIn(directory.path());
                    openReader(metadata, table);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    @SuppressWarnings("static-access")
    public void testSSTableRange()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < 10; row++)
                        {
                            for (int column = 0; column < 1; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable table = TestSSTable.firstIn(directory.path());
                    SparkSSTableReader reader = openReader(metadata, table);
                    assertNotNull(reader.firstToken());
                    assertNotNull(reader.lastToken());

                    // Verify primary Index.db file matches first and last
                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    Pair<DecoratedKey, DecoratedKey> firstAndLast;
                    try (InputStream is = new BufferedInputStream(new FileInputStream(indexFile.toFile())))
                    {
                        Pair<ByteBuffer, ByteBuffer> keys = ReaderUtils.readPrimaryIndex(is, true, Collections.emptyList());
                        firstAndLast = Pair.create(BRIDGE.getPartitioner(partitioner).decorateKey(keys.left),
                                                   BRIDGE.getPartitioner(partitioner).decorateKey(keys.right));
                    }
                    BigInteger first = ReaderUtils.tokenToBigInteger(firstAndLast.left.getToken());
                    BigInteger last = ReaderUtils.tokenToBigInteger(firstAndLast.right.getToken());
                    assertEquals(first, reader.firstToken());
                    assertEquals(last, reader.lastToken());

                    switch (partitioner)
                    {
                        case Murmur3Partitioner:
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(Partitioner.Murmur3Partitioner.minToken(),
                                                      Partitioner.Murmur3Partitioner.minToken())));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(Partitioner.Murmur3Partitioner.minToken(),
                                                      Partitioner.Murmur3Partitioner.minToken())));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(-8710962479251732708L),
                                                      BigInteger.valueOf(-7686143364045646507L))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(-7509452495886106294L),
                                                     BigInteger.valueOf(-7509452495886106293L))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(-7509452495886106293L),
                                                     BigInteger.valueOf(-7509452495886106293L))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(-7509452495886106293L),
                                                     BigInteger.valueOf(2562047788015215502L))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(-7509452495886106293L),
                                                     BigInteger.valueOf(9010454139840013625L))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(9010454139840013625L),
                                                     BigInteger.valueOf(9010454139840013625L))));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(Partitioner.Murmur3Partitioner.maxToken(),
                                                      Partitioner.Murmur3Partitioner.maxToken())));
                            return;
                        case RandomPartitioner:
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(Partitioner.RandomPartitioner.minToken(),
                                                      Partitioner.RandomPartitioner.minToken())));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(BigInteger.valueOf(0L),
                                                      BigInteger.valueOf(500L))));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("18837662806270881894834867523173387677"),
                                                      new BigInteger("18837662806270881894834867523173387677"))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("18837662806270881894834867523173387678"),
                                                      new BigInteger("18837662806270881894834867523173387678"))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("18837662806270881894834867523173387679"),
                                                      new BigInteger("18837662806270881894834867523173387679"))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("18837662806270881894834867523173387679"),
                                                      new BigInteger("137731376325982006772573399291321493164"))));
                            assertTrue(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("137731376325982006772573399291321493164"),
                                                      new BigInteger("137731376325982006772573399291321493164"))));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(new BigInteger("137731376325982006772573399291321493165"),
                                                      new BigInteger("137731376325982006772573399291321493165"))));
                            assertFalse(SparkSSTableReader.overlaps(reader,
                                    TokenRange.closed(Partitioner.RandomPartitioner.maxToken(),
                                                      Partitioner.RandomPartitioner.maxToken())));
                            return;
                        default:
                            throw new RuntimeException("Unexpected partitioner: " + partitioner);
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSkipNoPartitions()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    Path summaryFile = TestSSTable.firstIn(directory.path(), FileType.SUMMARY);
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SummaryDbUtils.Summary summary;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
                    {
                        summary = SummaryDbUtils.readSummary(in,
                                                             metadata.partitioner,
                                                             metadata.params.minIndexInterval,
                                                             metadata.params.maxIndexInterval);
                    }
                    // Set Spark token range equal to SSTable token range
                    TokenRange sparkTokenRange = TokenRange.closed(ReaderUtils.tokenToBigInteger(summary.first().getToken()),
                                                                   ReaderUtils.tokenToBigInteger(summary.last().getToken()));
                    SparkRangeFilter rangeFilter = SparkRangeFilter.create(sparkTokenRange);
                    AtomicBoolean skipped = new AtomicBoolean(false);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedPartition(ByteBuffer key, BigInteger token)
                        {
                            LOGGER.error("Skipped partition when should not: " + token);
                            skipped.set(true);
                        }
                    };
                    SSTableReader reader = openReader(metadata, dataFile, rangeFilter, true, stats);
                    assertEquals(ROWS * COLUMNS, countAndValidateRows(reader));  // Shouldn't skip any partitions here
                    assertFalse(skipped.get());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSkipPartitions()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    TokenRange sparkTokenRange;
                    switch (partitioner)
                    {
                        case Murmur3Partitioner:
                            sparkTokenRange = TokenRange.closed(BigInteger.valueOf(-9223372036854775808L),
                                                                BigInteger.valueOf(3074457345618258602L));
                            break;
                        case RandomPartitioner:
                            sparkTokenRange = TokenRange.closed(BigInteger.ZERO,
                                                                new BigInteger("916176208424801638531839357843455255"));
                            break;
                        default:
                            throw new RuntimeException("Unexpected partitioner: " + partitioner);
                    }
                    SparkRangeFilter rangeFilter = SparkRangeFilter.create(sparkTokenRange);
                    AtomicInteger skipCount = new AtomicInteger(0);
                    AtomicBoolean pass = new AtomicBoolean(true);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedPartition(ByteBuffer key, BigInteger token)
                        {
                            LOGGER.info("Skipping partition: " + token);
                            skipCount.incrementAndGet();
                            if (sparkTokenRange.contains(token))
                            {
                                LOGGER.info("Should not skip partition: " + token);
                                pass.set(false);
                            }
                        }
                    };
                    SSTableReader reader = openReader(metadata, dataFile, rangeFilter, false, stats);
                    int rows = countAndValidateRows(reader);
                    assertTrue(skipCount.get() > 0);
                    assertEquals((ROWS - skipCount.get()) * COLUMNS, rows);  // Should skip out of range partitions here
                    assertTrue(pass.get());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testOpenCompactionScanner()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write 3 SSTables
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, -1);
                            }
                        }
                    });
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, -2);
                            }
                        }
                    });
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(3, TestSSTable.countIn(directory.path()));

                    // Open CompactionStreamScanner over 3 SSTables
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    Set<SSTableReader> toCompact = TestSSTable.allIn(directory.path()).stream()
                            .map(Throwing.function(table -> openReader(metadata, table)))
                            .collect(Collectors.toSet());

                    int count = 0;
                    try (CompactionStreamScanner scanner = new CompactionStreamScanner(metadata, partitioner, toCompact))
                    {
                        // Iterate through CompactionStreamScanner verifying it correctly compacts data together
                        Rid rid = scanner.rid();
                        while (scanner.hasNext())
                        {
                            scanner.advanceToNextColumn();

                            // Extract partition key value
                            int a = rid.getPartitionKey().asIntBuffer().get();

                            // Extract clustering key value and column name
                            ByteBuffer colBuf = rid.getColumnName();
                            ByteBuffer clusteringKey = ByteBufferUtils.readBytesWithShortLength(colBuf);
                            colBuf.get();
                            String colName = ByteBufferUtils.string(ByteBufferUtils.readBytesWithShortLength(colBuf));
                            colBuf.get();
                            if (StringUtils.isEmpty(colName))
                            {
                                continue;
                            }
                            assertEquals("c", colName);
                            int b = clusteringKey.asIntBuffer().get();

                            // Extract value column
                            int c = rid.getValue().asIntBuffer().get();

                            // Verify CompactionIterator compacts 3 SSTables to use last values written
                            assertEquals(c, a + b);
                            count++;
                        }
                    }
                    assertEquals(ROWS * COLUMNS, count);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testFiltersDoNotMatch()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    TableMetadata metadata = tableMetadata(schema, partitioner);

                    BigInteger token = BigInteger.valueOf(9010454139840013626L);
                    SparkRangeFilter outsideRange = SparkRangeFilter.create(TokenRange.singleton(token));

                    AtomicBoolean pass = new AtomicBoolean(true);
                    AtomicInteger skipCount = new AtomicInteger(0);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedSSTable(@Nullable SparkRangeFilter sparkRangeFilter,
                                                   @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                                                   @NotNull BigInteger firstToken,
                                                   @NotNull BigInteger lastToken)
                        {
                            skipCount.incrementAndGet();
                            if (sparkRangeFilter == null || partitionKeyFilters.size() != 0)
                            {
                                pass.set(false);
                            }
                        }
                    };
                    SSTableReader reader = openReader(metadata, dataFile, outsideRange, true, stats);
                    assertTrue(reader.ignore());
                    assertEquals(1, skipCount.get());
                    assertTrue(pass.get());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testFilterKeyMissingInIndex()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    TableMetadata metadata = tableMetadata(schema, partitioner);

                    ByteBuffer key1 = Int32Type.instance.fromString("51");
                    BigInteger token1 = BRIDGE.hash(partitioner, key1);
                    PartitionKeyFilter keyNotInSSTable1 = PartitionKeyFilter.create(key1, token1);
                    ByteBuffer key2 = Int32Type.instance.fromString("90");
                    BigInteger token2 = BRIDGE.hash(partitioner, key2);
                    PartitionKeyFilter keyNotInSSTable2 = PartitionKeyFilter.create(key2, token2);
                    List<PartitionKeyFilter> partitionKeyFilters = Arrays.asList(keyNotInSSTable1, keyNotInSSTable2);

                    AtomicBoolean pass = new AtomicBoolean(true);
                    AtomicInteger skipCount = new AtomicInteger(0);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedSSTable(@Nullable SparkRangeFilter sparkRangeFilter,
                                                   @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                                                   @NotNull BigInteger firstToken,
                                                   @NotNull BigInteger lastToken)
                        {
                            pass.set(false);
                        }

                        @Override
                        public void missingInIndex()
                        {
                            skipCount.incrementAndGet();
                            if (partitionKeyFilters.size() != 2)
                            {
                                pass.set(false);
                            }
                        }
                    };
                    SSTableReader reader = openReader(metadata, dataFile, partitionKeyFilters, true, stats);
                    assertTrue(reader.ignore());
                    assertEquals(1, skipCount.get());
                    assertTrue(pass.get());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testPartialFilterMatch()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    SSTable dataFile = TestSSTable.firstIn(directory.path());
                    TableMetadata metadata = tableMetadata(schema, partitioner);

                    ByteBuffer key1 = Int32Type.instance.fromString("0");
                    BigInteger token1 = BRIDGE.hash(partitioner, key1);
                    PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);
                    SparkRangeFilter rangeFilter = SparkRangeFilter.create(TokenRange.singleton(token1));

                    ByteBuffer key2 = Int32Type.instance.fromString("55");
                    BigInteger token2 = BRIDGE.hash(partitioner, key2);
                    PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key2, token2);
                    List<PartitionKeyFilter> partitionKeyFilters = Arrays.asList(keyInSSTable, keyNotInSSTable);

                    AtomicBoolean pass = new AtomicBoolean(true);
                    AtomicInteger skipCount = new AtomicInteger(0);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedPartition(ByteBuffer key, BigInteger token)
                        {
                            LOGGER.info("Skipping partition: " + token);
                            skipCount.incrementAndGet();
                            if (partitionKeyFilters.stream().anyMatch(filter -> filter.matches(key)))
                            {
                                LOGGER.info("Should not skip partition: " + token);
                                pass.set(false);
                            }
                        }
                    };
                    SSTableReader reader = openReader(metadata, dataFile, rangeFilter, partitionKeyFilters, false, stats);
                    int rows = countAndValidateRows(reader);
                    assertTrue(skipCount.get() > 0);
                    assertEquals(COLUMNS, rows);
                    assertEquals((ROWS - skipCount.get()) * COLUMNS, rows);  // Should skip partitions not matching filters
                    assertTrue(pass.get());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testConstructFilename()
    {
        // Standard SSTable data file name
        assertEquals(new File("./keyspace/table/na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "na-1-big-Data.db"));

        // Non-standard SSTable data file name
        assertEquals(new File("./keyspace/table/na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "keyspace-table-na-1-big-Data.db"));

        // Malformed SSTable data file names
        assertEquals(new File("./keyspace/table/keyspace-table-qwerty-na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "keyspace-table-qwerty-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/keyspace-qwerty-na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "keyspace-qwerty-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/qwerty-table-na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "qwerty-table-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/keyspace-na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "keyspace-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/table-na-1-big-Data.db"),
                              SSTableReader.constructFilename("keyspace", "table", "table-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/qwerty.db"),
                              SSTableReader.constructFilename("keyspace", "table", "qwerty.db"));
    }

    @Test
    public void testExtractRangeSparkFilter()
    {
        Optional<TokenRange> range1 = SSTableReader.extractRange(
                SparkRangeFilter.create(TokenRange.closed(BigInteger.valueOf(5L), BigInteger.valueOf(500L))),
                Collections.emptyList());
        assertTrue(range1.isPresent());
        assertEquals(BigInteger.valueOf(5L), range1.get().lowerEndpoint());
        assertEquals(BigInteger.valueOf(500L), range1.get().upperEndpoint());

        Optional<TokenRange> range2 = SSTableReader.extractRange(
                SparkRangeFilter.create(TokenRange.closed(BigInteger.valueOf(-10000L), BigInteger.valueOf(29593L))),
                Collections.emptyList());
        assertTrue(range2.isPresent());
        assertEquals(BigInteger.valueOf(-10000L), range2.get().lowerEndpoint());
        assertEquals(BigInteger.valueOf(29593L), range2.get().upperEndpoint());

        assertFalse(SSTableReader.extractRange(null, Collections.emptyList()).isPresent());
    }

    @Test
    public void testExtractRangePartitionKeyFilters()
    {
        List<ByteBuffer> keys = new ArrayList<>();
        for (int index = 0; index < 1000; index++)
        {
            // Cast to ByteBuffer required when compiling with Java 8
            keys.add((ByteBuffer) ByteBuffer.allocate(4).putInt(index).flip());
        }

        List<PartitionKeyFilter> partitionKeyFilters = keys.stream().map(buffer -> {
            BigInteger token = ReaderUtils.tokenToBigInteger(Murmur3Partitioner.instance.getToken(buffer).getToken());
            return PartitionKeyFilter.create(buffer, token);
        }).collect(Collectors.toList());

        TokenRange sparkRange = TokenRange.closed(new BigInteger("0"), new BigInteger("2305843009213693952"));
        SparkRangeFilter sparkRangeFilter = SparkRangeFilter.create(sparkRange);
        List<PartitionKeyFilter> inRangePartitionKeyFilters = partitionKeyFilters.stream()
                .filter(filter -> sparkRange.contains(filter.token()))
                .collect(Collectors.toList());
        assertTrue(inRangePartitionKeyFilters.size() > 1);

        Optional<TokenRange> range = SSTableReader.extractRange(sparkRangeFilter, inRangePartitionKeyFilters);
        assertTrue(range.isPresent());
        assertNotEquals(sparkRange, range.get());
        assertTrue(sparkRange.lowerEndpoint().compareTo(range.get().lowerEndpoint()) < 0);
        assertTrue(sparkRange.upperEndpoint().compareTo(range.get().upperEndpoint()) > 0);
    }

    // Incremental Repair

    @Test
    public void testIncrementalRepair()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    int numSSTables = 4;
                    int numRepaired = 2;
                    int numUnRepaired = numSSTables - numRepaired;

                    // Write some SSTables
                    for (int table = 0; table < numSSTables; table++)
                    {
                        int position = table * ROWS;
                        schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                            for (int row = position; row < position + ROWS; row++)
                            {
                                for (int column = 0; column < COLUMNS; column++)
                                {
                                    writer.write(row, column, row + column);
                                }
                            }
                        });
                    }
                    assertEquals(numSSTables, TestSSTable.countIn(directory.path()));

                    TableMetadata metadata = tableMetadata(schema, partitioner);

                    AtomicInteger skipCount = new AtomicInteger(0);
                    Stats stats = new Stats()
                    {
                        @Override
                        public void skippedRepairedSSTable(SSTable ssTable, long repairedAt)
                        {
                            skipCount.incrementAndGet();
                        }
                    };

                    // Mark some SSTables as repaired
                    Map<SSTable, Boolean> isRepaired = TestSSTable.allIn(directory.path()).stream()
                            .collect(Collectors.toMap(Function.identity(), ssTable -> false));
                    int count = 0;
                    for (SSTable ssTable : isRepaired.keySet())
                    {
                        if (count < numRepaired)
                        {
                            isRepaired.put(ssTable, true);
                            count++;
                        }
                    }

                    List<SSTableReader> primaryReaders = TestSSTable.allIn(directory.path()).stream()
                            .map(ssTable -> openIncrementalReader(metadata, ssTable, stats, true, isRepaired.get(ssTable)))
                            .filter(reader -> !reader.ignore())
                            .collect(Collectors.toList());
                    List<SSTableReader> nonPrimaryReaders = TestSSTable.allIn(directory.path()).stream()
                            .map(ssTable -> openIncrementalReader(metadata, ssTable, stats, false, isRepaired.get(ssTable)))
                            .filter(reader -> !reader.ignore())
                            .collect(Collectors.toList());

                    // Primary repair replica should read all SSTables
                    assertEquals(numSSTables, primaryReaders.size());

                    // Non-primary repair replica should only read unrepaired SSTables
                    assertEquals(numUnRepaired, nonPrimaryReaders.size());
                    for (SSTableReader reader : nonPrimaryReaders)
                    {
                        assertFalse(isRepaired.get(reader.sstable()));
                    }
                    assertEquals(numUnRepaired, skipCount.get());

                    Set<SSTableReader> toCompact = Stream.concat(
                            primaryReaders.stream().filter(reader -> isRepaired.get(reader.sstable())),
                            nonPrimaryReaders.stream()).collect(Collectors.toSet());
                    assertEquals(numSSTables, toCompact.size());

                    int rowCount = 0;
                    boolean[] found = new boolean[numSSTables * ROWS];
                    try (CompactionStreamScanner scanner = new CompactionStreamScanner(metadata, partitioner, toCompact))
                    {
                        // Iterate through CompactionScanner and verify we have all the partition keys we are looking for
                        Rid rid = scanner.rid();
                        while (scanner.hasNext())
                        {
                            scanner.advanceToNextColumn();
                            int a = rid.getPartitionKey().asIntBuffer().get();
                            found[a] = true;
                            // Extract clustering key value and column name
                            ByteBuffer colBuf = rid.getColumnName();
                            ByteBuffer clusteringKey = ByteBufferUtils.readBytesWithShortLength(colBuf);
                            colBuf.get();
                            String colName = ByteBufferUtils.string(ByteBufferUtils.readBytesWithShortLength(colBuf));
                            colBuf.get();
                            if (StringUtils.isEmpty(colName))
                            {
                                continue;
                            }
                            assertEquals("c", colName);
                            int b = clusteringKey.asIntBuffer().get();

                            // Extract value column
                            int c = rid.getValue().asIntBuffer().get();

                            assertEquals(c, a + b);
                            rowCount++;
                        }
                    }
                    assertEquals(numSSTables * ROWS * COLUMNS, rowCount);
                    for (boolean b : found)
                    {
                        assertTrue(b);
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testPartitionKeyFilter()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    TestSchema schema = TestSchema.builder()
                                                  .withPartitionKey("a", BRIDGE.text())
                                                  .withClusteringKey("b", BRIDGE.aInt())
                                                  .withColumn("c", BRIDGE.aInt())
                                                  .withColumn("d", BRIDGE.text())
                                                  .build();
                    CqlTable cqlTable = schema.buildTable();
                    int numSSTables = 24;
                    String partitionKeyStr = (String) BRIDGE.text().randomValue(1024);
                    AbstractMap.SimpleEntry<ByteBuffer, BigInteger> partitionKey =
                            BRIDGE.getPartitionKey(cqlTable, partitioner, Collections.singletonList(partitionKeyStr));
                    PartitionKeyFilter partitionKeyFilter = PartitionKeyFilter.create(partitionKey.getKey(),
                                                                                      partitionKey.getValue());
                    SparkRangeFilter sparkRangeFilter = SparkRangeFilter.create(TokenRange.closed(partitioner.minToken(),
                                                                                                  partitioner.maxToken()));
                    Integer[] expectedC = new Integer[COLUMNS];
                    String[] expectedD = new String[COLUMNS];

                    // Write some SSTables
                    for (int table = 0; table < numSSTables; table++)
                    {
                        boolean isLastSSTable = table == numSSTables - 1;
                        schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                            if (isLastSSTable)
                            {
                                // Write partition key in last SSTable only
                                for (int column = 0; column < COLUMNS; column++)
                                {
                                    expectedC[column] = (int) BRIDGE.aInt().randomValue(1024);
                                    expectedD[column] = (String) BRIDGE.text().randomValue(1024);
                                    writer.write(partitionKeyStr, column, expectedC[column], expectedD[column]);
                                }
                            }

                            for (int row = 0; row < 2; row++)
                            {
                                for (int column = 0; column < COLUMNS; column++)
                                {
                                    String key = null;
                                    while (key == null || key.equals(partitionKeyStr))
                                    {
                                        key = (String) BRIDGE.text().randomValue(1024);
                                    }
                                    writer.write(key,
                                                 row,
                                                 BRIDGE.aInt().randomValue(1024),
                                                 BRIDGE.text().randomValue(1024));
                                }
                            }
                        });
                    }

                    TableMetadata metadata = new SchemaBuilder(schema.createStatement,
                                                               schema.keyspace,
                                                               new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                                     ImmutableMap.of("replication_factor", 1)),
                                                               partitioner).tableMetaData();
                    List<SSTable> ssTables = TestSSTable.allIn(directory.path());
                    assertEquals(numSSTables, ssTables.size());

                    Set<String> keys = new HashSet<>();
                    for (SSTable ssTable : ssTables)
                    {
                        SSTableReader reader = readerBuilder(metadata, ssTable, Stats.DoNothingStats.INSTANCE, true, false)
                                .withPartitionKeyFilter(partitionKeyFilter)
                                .withSparkRangeFilter(sparkRangeFilter)
                                .build();
                        if (reader.ignore())
                        {
                            continue;
                        }

                        ISSTableScanner scanner = reader.scanner();
                        int colCount = 0;
                        while (scanner.hasNext())
                        {
                            UnfilteredRowIterator it = scanner.next();
                            it.partitionKey().getKey().mark();
                            String key = UTF8Serializer.instance.deserialize(it.partitionKey().getKey());
                            it.partitionKey().getKey().reset();
                            keys.add(key);
                            while (it.hasNext())
                            {
                                it.next();
                                colCount++;
                            }
                        }
                        assertEquals(COLUMNS, colCount);
                    }
                    assertEquals(1, keys.size());
                    assertEquals(partitionKeyStr, keys.stream()
                                                      .findFirst()
                                                      .orElseThrow(() -> new RuntimeException("No partition keys returned")));
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    private static TableMetadata tableMetadata(TestSchema schema, Partitioner partitioner)
    {
        return new SchemaBuilder(schema.createStatement,
                                 schema.keyspace,
                                 new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                       ImmutableMap.of("replication_factor", 1)),
                                 partitioner).tableMetaData();
    }

    private static SSTableReader openReader(TableMetadata metadata, SSTable ssTable) throws IOException
    {
        return openReader(metadata, ssTable, null, Collections.emptyList(), true, Stats.DoNothingStats.INSTANCE);
    }

    private static SSTableReader openReader(TableMetadata metadata,
                                            SSTable ssTable,
                                            SparkRangeFilter sparkRangeFilter) throws IOException
    {
        return openReader(metadata, ssTable, sparkRangeFilter, Collections.emptyList(), true, Stats.DoNothingStats.INSTANCE);
    }

    private static SSTableReader openReader(TableMetadata metadata,
                                            SSTable ssTable,
                                            SparkRangeFilter sparkRangeFilter,
                                            boolean readIndexOffset,
                                            Stats stats) throws IOException
    {
        return openReader(metadata, ssTable, sparkRangeFilter, Collections.emptyList(), readIndexOffset, stats);
    }

    private static SSTableReader openReader(TableMetadata metadata,
                                            SSTable ssTable,
                                            List<PartitionKeyFilter> partitionKeyFilters,
                                            boolean readIndexOffset,
                                            Stats stats) throws IOException
    {
        return openReader(metadata, ssTable, null, partitionKeyFilters, readIndexOffset, stats);
    }

    private static SSTableReader openReader(TableMetadata metadata,
                                            SSTable ssTable,
                                            SparkRangeFilter sparkRangeFilter,
                                            List<PartitionKeyFilter> partitionKeyFilters,
                                            boolean readIndexOffset,
                                            Stats stats) throws IOException
    {
        return SSTableReader.builder(metadata, ssTable)
                            .withSparkRangeFilter(sparkRangeFilter)
                            .withPartitionKeyFilters(partitionKeyFilters)
                            .withReadIndexOffset(readIndexOffset)
                            .withStats(stats)
                            .build();
    }

    private static SSTableReader.Builder readerBuilder(TableMetadata metadata,
                                                       SSTable ssTable,
                                                       Stats stats,
                                                       boolean isRepairPrimary,
                                                       boolean isRepaired)
    {
        return SSTableReader.builder(metadata, ssTable)
                            .withReadIndexOffset(true)
                            .withStats(stats)
                            .isRepairPrimary(isRepairPrimary)
                            .withIsRepairedFunction(statsMetadata -> isRepaired);
    }

    private static SSTableReader openIncrementalReader(TableMetadata metadata,
                                                       SSTable ssTable,
                                                       Stats stats,
                                                       boolean isRepairPrimary,
                                                       boolean isRepaired)
    {
        try
        {
            return readerBuilder(metadata, ssTable, stats, isRepairPrimary, isRepaired)
                    .useIncrementalRepair(true)
                    .build();
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private static int countAndValidateRows(@NotNull SSTableReader reader)
    {
        ISSTableScanner scanner = reader.scanner();
        int count = 0;
        while (scanner.hasNext())
        {
            UnfilteredRowIterator it = scanner.next();
            while (it.hasNext())
            {
                BufferDecoratedKey key = (BufferDecoratedKey) it.partitionKey();
                int a = key.getKey().asIntBuffer().get();
                Unfiltered unfiltered = it.next();
                assertTrue(unfiltered.isRow());
                AbstractRow row = (AbstractRow) unfiltered;
                int b = row.clustering().bufferAt(0).asIntBuffer().get();
                for (ColumnData data : row)
                {
                    Cell<?> cell = (Cell<?>) data;
                    int c = cell.buffer().getInt();
                    assertEquals(c, a + b);
                    count++;
                }
            }
        }
        return count;
    }
}
