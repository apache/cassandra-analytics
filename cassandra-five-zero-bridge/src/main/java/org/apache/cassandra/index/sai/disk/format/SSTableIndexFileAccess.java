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
package org.apache.cassandra.index.sai.disk.format;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.spark.data.SSTable;

/**
 * Adapts an Analytics {@link SSTable} custom components to Cassandra SAI
 * {@link FileHandle}-based random-access API without creating local files.
 */
public final class SSTableIndexFileAccess implements IndexDescriptor.FileAccess
{
    private final SSTable sstable;
    private final Set<String> components;

    public SSTableIndexFileAccess(SSTable sstable)
    {
        this.sstable = sstable;
        this.components = sstable.customComponentNames();
    }

    @Override
    public boolean exists(File file)
    {
        return components.contains(file.name());
    }

    @Override
    public long length(File file)
    {
        return exists(file) ? sstable.customComponentLength(file.name()) : 0;
    }

    @Override
    public FileHandle open(File file)
    {
        String componentName = file.name();
        if (!exists(file))
        {
            throw new IllegalArgumentException("Unknown SAI component: " + componentName);
        }

        long length = sstable.customComponentLength(componentName);
        return new FileHandle.Builder(file)
               .withLengthOverride(length)
               .bufferSize(Ints.checkedCast(sstable.customComponentChunkBufferSize(componentName)))
               .complete(ignored -> new ChannelProxy(file,
                                                     new SSTableFileChannel(sstable,
                                                                            componentName,
                                                                            length)));
    }

    public static final class SSTableFileChannel extends FileChannel
    {
        private final SSTable sstable;
        private final String componentName;
        private final long length;
        private long position;

        SSTableFileChannel(SSTable sstable, String componentName, long length)
        {
            this.sstable = sstable;
            this.componentName = componentName;
            this.length = length;
        }

        @Override
        public synchronized int read(ByteBuffer destination) throws IOException
        {
            int count = read(destination, position);
            if (count > 0)
            {
                position += count;
            }
            return count;
        }

        @Override
        public synchronized long read(ByteBuffer[] destinations, int offset, int count) throws IOException
        {
            if (offset < 0 || count < 0 || offset > destinations.length - count)
            {
                throw new IndexOutOfBoundsException();
            }

            long total = 0;
            for (int i = offset; i < offset + count; i++)
            {
                int read = read(destinations[i]);
                if (read < 0)
                {
                    return total == 0 ? -1 : total;
                }
                total += read;
                if (destinations[i].hasRemaining())
                {
                    break;
                }
            }
            return total;
        }

        @Override
        public int read(ByteBuffer destination, long absolutePosition) throws IOException
        {
            ensureOpen();
            Preconditions.checkState(absolutePosition >= 0, "position must be non-negative");
            if (!destination.hasRemaining())
            {
                return 0;
            }
            if (absolutePosition >= length)
            {
                return -1;
            }

            int requested = (int) Math.min(destination.remaining(), length - absolutePosition);
            int originalLimit = destination.limit();
            destination.limit(destination.position() + requested);
            try
            {
                int read = sstable.readCustomComponent(componentName, absolutePosition, destination);
                if (read != requested)
                {
                    if (read < 0)
                    {
                        return -1;
                    }
                    throw new EOFException("Short read for " + componentName + " at " + absolutePosition
                                           + ": expected " + requested + " bytes, got " + read);
                }
                return read;
            }
            finally
            {
                destination.limit(originalLimit);
            }
        }

        @Override
        public synchronized long position() throws IOException
        {
            ensureOpen();
            return position;
        }

        @Override
        public synchronized FileChannel position(long newPosition) throws IOException
        {
            ensureOpen();
            if (newPosition < 0)
            {
                throw new IllegalArgumentException("position must be non-negative");
            }
            position = newPosition;
            return this;
        }

        @Override
        public long size() throws IOException
        {
            ensureOpen();
            return length;
        }

        @Override
        public int write(ByteBuffer source) throws IOException
        {
            throw new NonWritableChannelException();
        }

        @Override
        public long write(ByteBuffer[] sources, int offset, int count) throws IOException
        {
            throw new NonWritableChannelException();
        }

        @Override
        public int write(ByteBuffer source, long position) throws IOException
        {
            throw new NonWritableChannelException();
        }

        @Override
        public FileChannel truncate(long size) throws IOException
        {
            throw new NonWritableChannelException();
        }

        @Override
        public void force(boolean metadata) throws IOException
        {
            ensureOpen();
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException
        {
            throw new UnsupportedOperationException("transferTo is not supported");
        }

        @Override
        public long transferFrom(ReadableByteChannel source, long position, long count) throws IOException
        {
            throw new NonWritableChannelException();
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
        {
            throw new UnsupportedOperationException("mmap is not supported for remote SAI components");
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException
        {
            throw new UnsupportedOperationException("locking is not supported");
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException
        {
            throw new UnsupportedOperationException("locking is not supported");
        }

        @Override
        protected void implCloseChannel()
        {
        }

        private void ensureOpen() throws ClosedChannelException
        {
            if (!isOpen())
            {
                throw new ClosedChannelException();
            }
        }
    }
}
