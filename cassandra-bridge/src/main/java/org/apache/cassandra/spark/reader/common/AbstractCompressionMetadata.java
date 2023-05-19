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

package org.apache.cassandra.spark.reader.common;

import java.io.EOFException;
import java.io.IOException;

public abstract class AbstractCompressionMetadata
{
    // From 3.0, CRC check chance is part of table schema not compression metadata
    protected static final double CRC_CHECK_CHANCE = 1.0;

    private final long dataLength;
    private final BigLongArray chunkOffsets;

    protected AbstractCompressionMetadata(long dataLength, BigLongArray chunkOffsets)
    {
        this.dataLength = dataLength;
        this.chunkOffsets = chunkOffsets;
    }

    protected abstract int chunkLength();

    protected abstract double crcCheckChance();

    public long getDataLength()
    {
        return dataLength;
    }

    private Chunk chunkAtIndex(int index)
    {
        long chunkOffset = chunkOffsets.get(index);
        long nextChunkOffset = (index + 1 == chunkOffsets.size) ? -1 : chunkOffsets.get(index + 1);

        // "4" bytes reserved for checksum
        return new Chunk(chunkOffset, (nextChunkOffset == -1) ? -1 : (int) (nextChunkOffset - chunkOffset - 4));
    }

    /**
     * Get a chunk of compressed data (offset, length) corresponding to given position
     *
     * @param position Position in the file
     * @return chunk offset and length
     * @throws IOException on any I/O error
     */
    public Chunk chunkAtPosition(long position) throws IOException
    {
        // Position of the chunk
        int index = (int) (position / chunkLength());

        if (index >= chunkOffsets.size)
        {
            throw new EOFException();
        }

        return chunkAtIndex(index);
    }

    /**
     * Holds offset and length of the file chunk
     */
    public static class Chunk
    {
        public final long offset;
        public int length;  // CHECKSTYLE IGNORE: Public mutable field

        Chunk(long offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }

        public void setLength(int length)
        {
            this.length = length;
        }

        public String toString()
        {
            return String.format("Chunk<offset: %d, length: %d>", offset, length);
        }
    }
}
