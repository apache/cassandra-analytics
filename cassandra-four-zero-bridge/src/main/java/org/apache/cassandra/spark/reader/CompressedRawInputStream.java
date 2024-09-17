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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.ChunkCorruptException;
import org.apache.cassandra.spark.reader.common.RawInputStream;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ChecksumType;
import org.jetbrains.annotations.Nullable;

public final class CompressedRawInputStream extends RawInputStream
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressedRawInputStream.class);

    @Nullable
    private final SSTable ssTable;  // Only used for logging/stats
    private final CompressionMetadata metadata;

    // Used by reBuffer() to escape creating lots of temporary buffers
    private byte[] compressed;
    private long currentCompressed = 0;

    // Re-use single checksum object
    private final Checksum checksum;

    // Raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private CompressedRawInputStream(@Nullable SSTable ssTable,
                                     DataInputStream source,
                                     CompressionMetadata metadata,
                                     Stats stats)
    {
        super(source, new byte[metadata.chunkLength()], stats);
        this.ssTable = ssTable;
        this.metadata = metadata;
        this.checksum = ChecksumType.CRC32.newInstance();
        this.compressed = new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())];
    }

    @VisibleForTesting
    static CompressedRawInputStream fromInputStream(InputStream in,
                                                    InputStream compressionInfoInputStream,
                                                    boolean hasCompressedLength) throws IOException
    {
        return fromInputStream(null,
                               new DataInputStream(in),
                               compressionInfoInputStream,
                               hasCompressedLength,
                               Stats.DoNothingStats.INSTANCE);
    }

    static CompressedRawInputStream fromInputStream(@Nullable SSTable ssTable,
                                                    DataInputStream dataInputStream,
                                                    InputStream compressionInfoInputStream,
                                                    boolean hasCompressedLength,
                                                    Stats stats) throws IOException
    {
        return from(ssTable,
                    dataInputStream,
                    CompressionMetadata.fromInputStream(compressionInfoInputStream,
                                                        hasCompressedLength),
                    stats);
    }

    static CompressedRawInputStream from(@Nullable SSTable ssTable,
                                         DataInputStream dataInputStream,
                                         CompressionMetadata compressionMetadata,
                                         Stats stats)
    {
        return new CompressedRawInputStream(ssTable,
                                            dataInputStream,
                                            compressionMetadata,
                                            stats);
    }

    @Override
    public boolean isEOF()
    {
        return current >= metadata.getDataLength();
    }

    private void assertChunkPos(CompressionMetadata.Chunk chunk)
    {
        // We may be asked to skip ahead by more than one block
        assert currentCompressed <= chunk.offset
            : String.format("Requested chunk at input offset %d is less than current compressed position at %d",
                            chunk.offset, currentCompressed);
    }

    private void decompressChunk(CompressionMetadata.Chunk chunk, double crcChance) throws IOException
    {
        int checkSumFromChunk;

        assertChunkPos(chunk);

        source.skipBytes((int) (chunk.offset - currentCompressed));
        currentCompressed = chunk.offset;

        if (compressed.length < chunk.length)
        {
            compressed = new byte[chunk.length];
        }

        if (chunk.length > 0)
        {
            try
            {
                source.readFully(compressed, 0, chunk.length);
            }
            catch (EOFException exception)
            {
                throw new IOException(String.format("Failed to read %d bytes from offset %d.",
                                                    chunk.length, chunk.offset), exception);
            }

            checkSumFromChunk = source.readInt();
            stats.readBytes(chunk.length + checksumBytes.length);  // 4 bytes for CRC
        }
        else
        {
            // Last block; we don't have the length of the last chunk; try to read full buffer length; this
            // will almost certainly end up reading all of the compressed data; update current chunk length
            // to the number of the bytes read minus 4 to accommodate for the chunk length field
            int lastBytesLength = 0;
            while (true)
            {
                if (lastBytesLength >= compressed.length)
                {
                    byte[] buffer = new byte[lastBytesLength * 2];
                    System.arraycopy(compressed, 0, buffer, 0, lastBytesLength);
                    compressed = buffer;
                }
                int readLength = source.read(compressed, lastBytesLength, compressed.length - lastBytesLength);
                if (readLength < 0)
                {
                    break;
                }
                stats.readBytes(readLength);
                lastBytesLength += readLength;
            }

            chunk.setLength(lastBytesLength - 4);

            // We inadvertently also read the checksum; we need to grab it from the end of the buffer
            checkSumFromChunk = ByteBufferUtil.toInt(ByteBuffer.wrap(compressed, lastBytesLength - 4, 4));
        }

        validBufferBytes = metadata.compressor().uncompress(compressed, 0, chunk.length, buffer, 0);
        stats.decompressedBytes(chunk.length, validBufferBytes);

        if (crcChance > ThreadLocalRandom.current().nextDouble())
        {
            checksum.update(compressed, 0, chunk.length);

            if (checkSumFromChunk != (int) checksum.getValue())
            {
                throw new ChunkCorruptException("bad chunk " + chunk);
            }

            // Reset checksum object back to the original (blank) state
            checksum.reset();
        }

        currentCompressed += chunk.length + checksumBytes.length;

        alignBufferOffset();
    }

    /**
     * Buffer offset is always aligned
     */
    private void alignBufferOffset()
    {
        // See https://en.wikipedia.org/wiki/Data_structure_alignment#Computing_padding
        bufferOffset = current & -buffer.length;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        try
        {
            decompressChunk(metadata.chunkAtPosition(current), metadata.crcCheckChance());
        }
        catch (IOException exception)
        {
            if (ssTable != null)
            {
                LOGGER.warn("IOException decompressing SSTable position={} sstable='{}'", current, ssTable, exception);
                stats.decompressionException(ssTable, exception);
            }
            throw exception;
        }
    }

    /**
     * For Compressed input, we can only efficiently skip entire compressed chunks.
     * Therefore, we:
     *  1) Skip any uncompressed bytes already buffered. If that's enough to satisfy the skip request, we return.
     *  2) Count how many whole compressed chunks we can skip to satisfy the uncompressed skip request,
     *     then skip the compressed bytes at the source InputStream, decrementing the remaining bytes
     *     by the equivalent uncompressed bytes that have been skipped.
     *  3) Then we continue to skip any remaining bytes in-memory
     *     (decompress the next chunk and skip in-memory until we've satisfied the skip request).
     *
     * @param count number of uncompressed bytes to skip
     */
    @Override
    public long skip(long count) throws IOException
    {
        long precheck = maybeStandardSkip(count);
        if (precheck >= 0)
        {
            return precheck;
        }

        // Skip any buffered bytes
        long remaining = count - skipBuffered();

        // We can efficiently skip ahead by 0 or more whole compressed chunks by passing down to the source InputStream
        long totalCompressedBytes = 0L;
        long totalDecompressedBytes = 0L;
        long startCurrent = current;
        long startCompressed = currentCompressed;
        long startBufferOffset = bufferOffset;
        while (totalDecompressedBytes + buffer.length < remaining)  // We can only skip whole chunks
        {
            AbstractCompressionMetadata.Chunk chunk = metadata.chunkAtPosition(current);
            if (chunk.length < 0)
            {
                // For the last chunk we don't know the length so reset positions & skip as normal
                current = startCurrent;
                currentCompressed = startCompressed;
                bufferOffset = startBufferOffset;
                return standardSkip(remaining);
            }

            assertChunkPos(chunk);

            // Sum total compressed & decompressed bytes we can skip
            int chunkLength = chunk.length + checksumBytes.length;
            totalCompressedBytes += (int) (chunk.offset - currentCompressed);
            currentCompressed = chunk.offset;
            totalCompressedBytes += chunkLength;
            currentCompressed += chunkLength;
            totalDecompressedBytes += buffer.length;

            alignBufferOffset();
            current += buffer.length;
        }

        // Skip compressed chunks at the source
        long skipped = source.skip(totalCompressedBytes);
        assert skipped == totalCompressedBytes : "Bytes skipped should equal compressed length";
        remaining -= totalDecompressedBytes;  // Decrement decompressed bytes we have skipped

        // Skip any remaining bytes as normal
        remaining -= standardSkip(remaining);

        long total = count - remaining;
        stats.skippedBytes(total);
        return total;
    }
}
