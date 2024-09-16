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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

@SuppressWarnings("SameParameterValue")
public class IndexReaderTests
{
    static final ExecutorService EXECUTOR =
    Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("index-reader-tests-%d")
                                                              .setDaemon(true)
                                                              .build());
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexReaderTests.class);
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testPartialCompressedSizeWithinChunk()
    {
        assertEquals(64, IndexReader.partialCompressedSizeWithinChunk(0, 1024, 64, true));
        assertEquals(32, IndexReader.partialCompressedSizeWithinChunk(512, 1024, 64, true));
        assertEquals(16, IndexReader.partialCompressedSizeWithinChunk(768, 1024, 64, true));
        assertEquals(2, IndexReader.partialCompressedSizeWithinChunk(992, 1024, 64, true));
        assertEquals(2, IndexReader.partialCompressedSizeWithinChunk(995, 1024, 64, true));
        assertEquals(1, IndexReader.partialCompressedSizeWithinChunk(1008, 1024, 64, true));
        assertEquals(0, IndexReader.partialCompressedSizeWithinChunk(1023, 1024, 64, true));
        assertEquals(64, IndexReader.partialCompressedSizeWithinChunk(1024, 1024, 64, true));
        assertEquals(64, IndexReader.partialCompressedSizeWithinChunk(2048, 1024, 64, true));
        assertEquals(32, IndexReader.partialCompressedSizeWithinChunk(2560, 1024, 64, true));

        assertEquals(0, IndexReader.partialCompressedSizeWithinChunk(0, 1024, 64, false));
        assertEquals(1, IndexReader.partialCompressedSizeWithinChunk(16, 1024, 64, false));
        assertEquals(32, IndexReader.partialCompressedSizeWithinChunk(512, 1024, 64, false));
        assertEquals(64, IndexReader.partialCompressedSizeWithinChunk(1023, 1024, 64, false));
        assertEquals(32, IndexReader.partialCompressedSizeWithinChunk(2560, 1024, 64, false));
    }

    @Test
    public void testCompressedSizeWithinSameChunk()
    {
        // within the same chunk
        assertEquals(64, calculateCompressedSize(128, 256, 0, 512));
        assertEquals(128, calculateCompressedSize(0, 1024, 5, 128));
        assertEquals(25, calculateCompressedSize(32, 64, 5, 800));
    }

    @Test
    public void testCompressedSizeMultipleChunks()
    {
        // partition straddles more than one chunk
        assertEquals(448 + 128, calculateCompressedSize(128, 0, 512, 1536, 1, 256));
        assertEquals(112 + (256 * 10) + 32, calculateCompressedSize(128, 0, 128, 11392, 11, 256));
    }

    private static long calculateCompressedSize(long start, long end, int startIdx, int startCompressedChunkSize)
    {
        return IndexReader.calculateCompressedSize(mockMetaData(start, startIdx, startCompressedChunkSize, end), 160000000, start, end);
    }

    private static long calculateCompressedSize(long start, int startIdx, int startCompressedChunkSize,
                                                long end, int endIdx, int endCompressedChunkSize)
    {
        return IndexReader.calculateCompressedSize(
        mockMetaData(start, startIdx, startCompressedChunkSize, end, endIdx, endCompressedChunkSize), 160000000, start, end
        );
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize, long end)
    {
        return mockMetaData(start, startIdx, startCompressedChunkSize, end, startIdx, startCompressedChunkSize);
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize,
                                                    long end, int endIdx, int endCompressedChunkSize)
    {
        return mockMetaData(start, startIdx, startCompressedChunkSize, end, endIdx, endCompressedChunkSize, 1024);
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize,
                                                    long end, int endIdx, int endCompressedChunkSize,
                                                    int uncompressedChunkLength)
    {
        CompressionMetadata metadata = mock(CompressionMetadata.class);
        when(metadata.chunkIdx(eq(start))).thenReturn(startIdx);
        when(metadata.chunkIdx(eq(end))).thenReturn(endIdx);
        when(metadata.chunkLength()).thenReturn(uncompressedChunkLength);
        when(metadata.chunkAtIndex(eq(startIdx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, startCompressedChunkSize));
        when(metadata.chunkAtIndex(eq(endIdx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, endCompressedChunkSize));
        for (int idx = startIdx + 1; idx < endIdx; idx++)
        {
            // let intermediate chunks have same compressed size as end
            when(metadata.chunkAtIndex(eq(idx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, endCompressedChunkSize));
        }
        return metadata;
    }

    @Test
    public void testIndexReaderWithCompression()
    {
        testIndexReader(true);
    }

    @Test
    public void testIndexReaderWithoutCompression()
    {
        testIndexReader(false);
    }

    private static void testIndexReader(boolean withCompression)
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    Path dir = directory.path();
                    int numPartitions = 50000;
                    BigInteger eighth = partitioner.maxToken().divide(BigInteger.valueOf(8));
                    SparkRangeFilter rangeFilter = SparkRangeFilter.create(
                    TokenRange.closed(partitioner.minToken().add(eighth),
                                      partitioner.maxToken().subtract(eighth))
                    );
                    TestSchema schema = TestSchema.builder(BRIDGE)
                                                  .withPartitionKey("a", BRIDGE.aInt())
                                                  .withColumn("b", BRIDGE.blob())
                                                  .withCompression(withCompression)
                                                  .build();
                    CqlTable table = schema.buildTable();
                    TableMetadata metaData = new SchemaBuilder(schema.createStatement, schema.keyspace, schema.rf, partitioner).tableMetaData();

                    // write an SSTable
                    Map<Integer, Integer> expected = new HashMap<>();
                    schema.writeSSTable(dir, BRIDGE, partitioner, (writer) -> {
                        for (int i = 0; i < numPartitions; i++)
                        {
                            BigInteger token = ReaderUtils.tokenToBigInteger(
                            metaData.partitioner.decorateKey(Int32Serializer.instance.serialize(i)).getToken()
                            );
                            byte[] lowEntropyData = TestUtils.randomLowEntropyData();
                            if (rangeFilter.overlaps(token))
                            {
                                expected.put(i, lowEntropyData.length);
                            }
                            writer.write(i, ByteBuffer.wrap(lowEntropyData));
                        }
                    });
                    assertFalse(expected.isEmpty());
                    assertTrue(expected.size() < numPartitions);

                    List<Path> pathList;
                    try (Stream<Path> stream = TestUtils.getFileType(dir, FileType.DATA))
                    {
                        pathList = stream.collect(Collectors.toList());
                    }
                    List<SSTable> ssTables = pathList.stream().map(TestSSTable::at).collect(Collectors.toList());
                    assertFalse(ssTables.isEmpty());
                    AtomicReference<Throwable> error = new AtomicReference<>();
                    CountDownLatch latch = new CountDownLatch(ssTables.size());
                    AtomicInteger rowCount = new AtomicInteger(0);

                    IndexConsumer consumer = new IndexConsumer()
                    {
                        public void onFailure(Throwable t)
                        {
                            LOGGER.warn("Error reading index file", t);
                            if (error.get() == null)
                            {
                                error.compareAndSet(null, t);
                            }
                        }

                        public void onFinished(long runtimeNanos)
                        {
                            latch.countDown();
                        }

                        public void accept(IndexEntry indexEntry)
                        {
                            // we should only read in-range partition keys
                            rowCount.getAndIncrement();
                            int pk = indexEntry.partitionKey.getInt();
                            int blobSize = expected.get(pk);
                            assertTrue(expected.containsKey(pk));
                            assertTrue(indexEntry.compressed > 0);
                            assertTrue(withCompression
                                       ? indexEntry.compressed < indexEntry.uncompressed * 0.1
                                       : indexEntry.compressed == indexEntry.uncompressed);
                            assertTrue((int) indexEntry.uncompressed > blobSize);
                            // uncompressed size should be proportional to the blob size, with some serialization overhead
                            assertTrue(((int) indexEntry.uncompressed - blobSize) < 40);
                        }
                    };

                    ssTables
                    .forEach(ssTable -> CompletableFuture.runAsync(
                             () -> new IndexReader(ssTable, metaData, rangeFilter, Stats.DoNothingStats.INSTANCE, consumer), EXECUTOR)
                    );

                    try
                    {
                        latch.await();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    assertNull(error.get());
                    assertEquals(expected.size(), rowCount.get());
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
    }
}
