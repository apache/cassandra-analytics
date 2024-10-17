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

package org.apache.cassandra.analytics.reader.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.jetbrains.annotations.NotNull;

public class RawInputStream extends InputStream
{
    protected final DataInputStream source;

    protected final byte[] buffer;

    // `bufferOffset` is the offset of the beginning of the buffer
    protected long bufferOffset;
    // `current` is the current position in source
    protected long current = 0;
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid;
    // this will be LESS than buffer capacity if buffer is not full!
    protected int validBufferBytes = 0;
    private boolean endOfStream = false;
    protected final Stats stats;

    public RawInputStream(DataInputStream source, byte[] buffer, Stats stats)
    {
        this.source = source;
        this.buffer = buffer;
        this.stats = stats;
        this.stats.openedDataInputStream();
    }

    public boolean isEOF()
    {
        return endOfStream && finishedReadingBuffer();
    }

    private int bufferCursor()
    {
        return (int) (current - bufferOffset);
    }

    private boolean bufferInit = false;

    protected boolean finishedReadingBuffer()
    {
        return current >= bufferOffset + validBufferBytes;
    }

    protected void maybeReBuffer() throws IOException
    {
        if (finishedReadingBuffer() || validBufferBytes < 0)
        {
            reBuffer();
        }
    }

    protected void reBuffer() throws IOException
    {
        if (endOfStream)
        {
            throw new RuntimeException("Shouldn't be reading from a known EOF stream");
        }

        if (bufferInit)
        {
            bufferOffset += buffer.length;
        }
        else
        {
            bufferInit = true;
        }

        validBufferBytes = ByteBufferUtils.readFully(source, buffer, buffer.length);
        stats.readBytes(validBufferBytes);

        if (validBufferBytes < buffer.length)
        {
            endOfStream = true;
        }
    }

    /**
     * `current` tracks the current position in the source, this isn't necessarily total bytes read
     * as skipping at the base InputStream might seek to the new offset without reading the bytes
     *
     * @return the current position in the source
     */
    public long position()
    {
        return current;
    }

    /**
     * Perform standard in-memory skip if n is less than or equal to the number of bytes buffered in memory
     *
     * @param count the number of bytes to be skipped
     * @return number of bytes skipped or -1 if not skipped
     * @throws IOException IOException
     */
    protected long maybeStandardSkip(long count) throws IOException
    {
        if (count <= 0)
        {
            return 0;
        }
        else if (count <= remainingBytes())
        {
            // We've already buffered more than n bytes, so do a standard in-memory skip
            return standardSkip(count);
        }
        else
        {
            return -1;
        }
    }

    /**
     * Skip any bytes already buffered in the 'buffer' array
     *
     * @return bytes actually skipped
     * @throws IOException IOException
     */
    protected long skipBuffered() throws IOException
    {
        return standardSkip(remainingBytes());
    }

    public long standardSkip(long count) throws IOException
    {
        long actual = super.skip(count);
        stats.skippedBytes(actual);
        return actual;
    }

    @Override
    public long skip(long count) throws IOException
    {
        long skipped = maybeStandardSkip(count);
        if (skipped >= 0)
        {
            return skipped;
        }
        long remaining = count - skipBuffered();

        // Skip remaining bytes at source
        skipped = source.skip(remaining);
        if (skipped > 0)
        {
            remaining -= skipped;

            // Update current position marker to account for skipped bytes.
            // Reset buffer so we rebuffer on next read.
            current += skipped;
            bufferOffset = current;
            validBufferBytes = -1;
            bufferInit = false;
        }

        long total = count - remaining;
        stats.skippedBytes(total);
        return total;
    }

    @Override
    public int read() throws IOException
    {
        if (buffer == null)
        {
            throw new IOException();
        }

        if (isEOF())
        {
            return -1;
        }

        maybeReBuffer();

        assert bufferOffset <= current && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xFF;
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    @Override
    public int read(@NotNull byte[] buff, int offset, int length) throws IOException
    {
        if (buffer == null)
        {
            throw new IOException();
        }

        if (length == 0)
        {
            return 0;
        }

        if (isEOF())
        {
            return -1;
        }

        maybeReBuffer();

        assert bufferOffset <= current && current < bufferOffset + validBufferBytes
            : String.format("Current offset %d, buffer offset %d, buffer limit %d",
                            current, bufferOffset, validBufferBytes);

        int toCopy = Math.min(length, remainingBytes());

        System.arraycopy(buffer, bufferCursor(), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }

    protected int remainingBytes()
    {
        return validBufferBytes - bufferCursor();
    }

    @Override
    public void close() throws IOException
    {
        source.close();
        stats.closedDataInputStream();
    }
}
