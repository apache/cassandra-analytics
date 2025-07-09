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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;

public class ReadOnlyInputStreamFileChannel extends FileChannel
{
    private BufferingInputStream<?> inputStream;
    private final long size;
    private long position;

    public ReadOnlyInputStreamFileChannel(BufferingInputStream<?> inputStream, long size)
    {
        this.inputStream = inputStream;
        this.size = size;
        this.position = 0;
    }

    public int read(ByteBuffer dst) throws IOException
    {
        // setup appropriate remaining size of the buffer
        int streamRemaining = Math.toIntExact(Math.min(size - position, Integer.MAX_VALUE));
        int newLimit = dst.position() + Math.min(streamRemaining, dst.remaining());
        dst.limit(Math.min(newLimit, dst.capacity()));

        int read = inputStream.read(dst);
        position += read;
        if (dst.position() == 0 && dst.limit() > 0)
        {
            // TODO(c4c5): C* SimpleChunkReader flips the buffer, so position should be set to the end.
            dst.position(read);
        }
        return read;
    }

    public int read(ByteBuffer dst, long position) throws IOException
    {
        if (this.size <= position)
        {
            return -1;
        }
        if (this.position != position)
        {
            // move to desired position
            position(position);
        }
        return read(dst);
    }

    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public long position() throws IOException
    {
        return position;
    }

    public long size() throws IOException
    {
        return size;
    }

    public FileChannel position(long newPosition) throws IOException
    {
        if (newPosition != position)
        {
            inputStream = inputStream.reBuffer(newPosition);
            position = newPosition;
        }
        return this;
    }

    public int write(ByteBuffer src) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public FileChannel truncate(long size) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public void force(boolean metaData) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public long transferTo(long position, long count, WritableByteChannel target) throws IOException
    {
        throw new UnsupportedOperationException("This channel does not support transferring");
    }

    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public int write(ByteBuffer src, long position) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
    {
        throw new UnsupportedOperationException("Mapping is not supported by this channel");
    }

    public FileLock lock(long position, long size, boolean shared) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    public FileLock tryLock(long position, long size, boolean shared) throws IOException
    {
        throw new UnsupportedOperationException("This is a read only channel");
    }

    protected void implCloseChannel() throws IOException
    {
        if (inputStream != null)
        {
            inputStream.close();
        }
    }
}
