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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.IncompleteSSTableException;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.IIndexReader;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IndexReader implements IIndexReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexReader.class);

    private TokenRange ssTableRange = null;

    public IndexReader(@NotNull SSTable ssTable,
                       @NotNull TableMetadata metadata,
                       @Nullable SparkRangeFilter rangeFilter,
                       @NotNull Stats stats,
                       @NotNull IndexConsumer consumer)
    {
        long now = System.nanoTime();
        long startTimeNanos = now;
        try
        {
            File file = SSTableReader.constructFilename(metadata.keyspace, metadata.name, ssTable.getDataFileName());
            Descriptor descriptor = Descriptor.fromFilename(file);
            Version version = descriptor.version;

            // if there is a range filter we can use the Summary.db file to seek to approximate start token range location in Index.db file
            long skipAhead = -1;
            now = System.nanoTime();
            if (rangeFilter != null)
            {
                SummaryDbUtils.Summary summary = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable);
                if (summary != null)
                {
                    this.ssTableRange = TokenRange.closed(ReaderUtils.tokenToBigInteger(summary.first().getToken()),
                                                          ReaderUtils.tokenToBigInteger(summary.last().getToken()));
                    if (!rangeFilter.overlaps(this.ssTableRange))
                    {
                        LOGGER.info("Skipping non-overlapping Index.db file rangeFilter='[{},{}]' sstableRange='[{},{}]'",
                                    rangeFilter.tokenRange().firstEnclosedValue(), rangeFilter.tokenRange().upperEndpoint(),
                                    this.ssTableRange.firstEnclosedValue(), this.ssTableRange.upperEndpoint());
                        stats.indexFileSkipped();
                        return;
                    }

                    skipAhead = summary.summary().getPosition(
                    SummaryDbUtils.binarySearchSummary(summary.summary(), metadata.partitioner, rangeFilter.tokenRange().firstEnclosedValue())
                    );
                    stats.indexSummaryFileRead(System.nanoTime() - now);
                    now = System.nanoTime();
                }
            }

            // read CompressionMetadata if it exists
            CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetaData(ssTable, version.hasMaxCompressedLength());
            if (compressionMetadata != null)
            {
                stats.indexCompressionFileRead(System.nanoTime() - now);
                now = System.nanoTime();
            }

            // read through Index.db and consume Partition keys
            try (InputStream is = ssTable.openPrimaryIndexStream())
            {
                if (is == null)
                {
                    consumer.onFailure(new IncompleteSSTableException(FileType.INDEX));
                    return;
                }

                consumePrimaryIndex(metadata.partitioner,
                                    is,
                                    ssTable,
                                    compressionMetadata,
                                    rangeFilter,
                                    stats,
                                    skipAhead,
                                    consumer);
                stats.indexFileRead(System.nanoTime() - now);
            }
        }
        catch (Throwable t)
        {
            consumer.onFailure(t);
        }
        finally
        {
            consumer.onFinished(System.nanoTime() - startTimeNanos);
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    static void consumePrimaryIndex(@NotNull IPartitioner partitioner,
                                    @NotNull InputStream primaryIndex,
                                    @NotNull SSTable ssTable,
                                    @Nullable CompressionMetadata compressionMetadata,
                                    @Nullable SparkRangeFilter range,
                                    @NotNull Stats stats,
                                    long skipBytes,
                                    @NotNull IndexConsumer consumer) throws IOException
    {
        long primaryIndexLength = ssTable.length(FileType.INDEX);
        long dataDbFileLength = ssTable.length(FileType.DATA);
        try (DataInputStream dis = new DataInputStream(primaryIndex))
        {
            if (skipBytes > 0)
            {
                ByteBufferUtils.skipFully(dis, skipBytes);
                stats.indexBytesSkipped(skipBytes);
            }

            ByteBuffer prevKey = null;
            long prevPos = 0;
            BigInteger prevToken = null;
            boolean started = false;

            long totalBytesRead = 0;
            try
            {
                while (true)
                {
                    // read partition key length
                    int len = dis.readUnsignedShort();

                    // read partition key & decorate
                    byte[] buf = new byte[len];
                    dis.readFully(buf);
                    ByteBuffer key = ByteBuffer.wrap(buf);
                    DecoratedKey decoratedKey = partitioner.decorateKey(key);
                    BigInteger token = ReaderUtils.tokenToBigInteger(decoratedKey.getToken());

                    // read position & skip promoted index
                    long pos = ReaderUtils.readPosition(dis);
                    int promotedIndex = ReaderUtils.skipPromotedIndex(dis);
                    totalBytesRead += 2 + len + VIntCoding.computeUnsignedVIntSize(pos) + promotedIndex;

                    if (prevKey != null && (range == null || range.overlaps(prevToken)))
                    {
                        // previous key overlaps with range filter, so consume
                        started = true;
                        long uncompressed = pos - prevPos;
                        long compressed = compressionMetadata == null
                                                ? uncompressed
                                                : calculateCompressedSize(compressionMetadata, dataDbFileLength, prevPos, pos - 1);
                        consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                    }
                    else if (started)
                    {
                        // we have gone passed the range we care about so exit early
                        stats.indexBytesSkipped(primaryIndexLength - totalBytesRead - skipBytes);
                        return;
                    }

                    prevPos = pos;
                    prevKey = key;
                    prevToken = token;
                }
            }
            catch (EOFException ignored)
            {
                // finished
            }
            finally
            {
                stats.indexBytesRead(totalBytesRead);
            }

            if (prevKey != null && (range == null || range.overlaps(prevToken)))
            {
                // we reached the end of the file, so consume last key if overlaps
                long end = (compressionMetadata == null ? dataDbFileLength : compressionMetadata.getDataLength());
                long uncompressed = end - prevPos;
                long compressed = compressionMetadata == null
                                        ? uncompressed
                                        : calculateCompressedSize(compressionMetadata, dataDbFileLength, prevPos, end - 1);
                consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
            }
        }
    }

    /**
     * @param compressionMetadata  SSTable Compression Metadata
     * @param compressedDataLength full compressed length of the Data.db file
     * @param start                uncompressed start position.
     * @param end                  uncompressed end position.
     * @return the compressed size of a partition using the uncompressed start and end offset in the Data.db file to calculate.
     */
    public static long calculateCompressedSize(@NotNull CompressionMetadata compressionMetadata,
                                               long compressedDataLength,
                                               long start,
                                               long end)
    {
        int startIdx = compressionMetadata.chunkIdx(start);
        int endIdx = compressionMetadata.chunkIdx(end);
        AbstractCompressionMetadata.Chunk startChunk = compressionMetadata.chunkAtIndex(startIdx);
        long startLen = chunkCompressedLength(startChunk, compressedDataLength);
        // compressed chunk sizes vary, but uncompressed chunk length is the same for all chunks
        long uncompressedChunkLen = compressionMetadata.chunkLength();

        if (startIdx == endIdx)
        {
            // within the same chunk, so take % of uncompressed length and apply to compressed length
            float perc = (end - start) / (float) uncompressedChunkLen;
            return Math.round(perc * startLen);
        }

        long size = partialCompressedSizeWithinChunk(start, uncompressedChunkLen, startLen, true);
        AbstractCompressionMetadata.Chunk endChunk = compressionMetadata.chunkAtIndex(endIdx);
        long endLen = chunkCompressedLength(endChunk, compressedDataLength);

        size += partialCompressedSizeWithinChunk(end, uncompressedChunkLen, endLen, false);

        for (int idx = startIdx + 1; idx < endIdx; idx++)
        {
            // add compressed size of whole intermediate chunks
            size += chunkCompressedLength(compressionMetadata.chunkAtIndex(idx), compressedDataLength);
        }

        return size;
    }

    private static long chunkCompressedLength(AbstractCompressionMetadata.Chunk chunk, long compressedDataLength)
    {
        // chunk.length < 0 means it is the last chunk so use compressedDataLength to calculate compressed size
        return chunk.length >= 0 ? chunk.length : compressedDataLength - chunk.offset;
    }

    /**
     * Returns the partial compressed size of a partition whose start or end overlaps with a compressed chunk.
     * This is an estimate because of the variable compressibility of partitions within the chunk.
     *
     * @param uncompressedPos      uncompressed position in Data.db file
     * @param uncompressedChunkLen fixed size uncompressed chunk size
     * @param compressedChunkLen   compressed chunk size of this chunk
     * @param start                true if uncompressedPos is start position of partition and false if end position of partition
     * @return the estimated compressed size of partition start or end that overlaps with this chunk.
     */
    public static int partialCompressedSizeWithinChunk(long uncompressedPos,
                                                       long uncompressedChunkLen,
                                                       long compressedChunkLen,
                                                       boolean start)
    {
        long mod = uncompressedPos % uncompressedChunkLen;
        // if start position then it occupies remaining bytes to end of chunk, if end position it occupies bytes from start of chunk
        long usedBytes = start ? (uncompressedChunkLen - mod) : mod;
        // percentage of uncompressed bytes that it occupies in the chunk
        float perc = usedBytes / (float) uncompressedChunkLen;
        // apply percentage to compressed chunk length to give compressed bytes occupied
        return Math.round(perc * compressedChunkLen);
    }

    public BigInteger firstToken()
    {
        return ssTableRange != null ? ssTableRange.firstEnclosedValue() : null;
    }

    public BigInteger lastToken()
    {
        return ssTableRange != null ? ssTableRange.upperEndpoint() : null;
    }

    public boolean ignore()
    {
        return false;
    }
}
