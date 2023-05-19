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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.SSTableInputStream;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

public class CdcRandomAccessReader extends RandomAccessReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcRandomAccessReader.class);

    private final CommitLog log;

    public CdcRandomAccessReader(CommitLog log)
    {
        super(new CdcRebufferer(log));
        this.log = log;
    }

    @Override
    public String getPath()
    {
        return log.path();
    }

    public static class CdcRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        ByteBuffer buffer;
        final CommitLog log;
        final int chunkSize;
        long offset = 0;
        final SSTableSource<? extends SSTable> source;
        private final SSTableInputStream<? extends SSTable> inputStream;

        CdcRebufferer(CommitLog log)
        {
            this(log, IOUtils.DEFAULT_CDC_BUFFER_SIZE);
        }

        CdcRebufferer(CommitLog log, int chunkSize)
        {
            Preconditions.checkArgument(chunkSize > 0, "Chunk size must be a positive integer");
            this.log = log;
            this.chunkSize = chunkSize;
            this.buffer = ByteBuffer.allocate(bufferSize());
            this.source = log.source();

            // We read the CommitLogs sequentially, so we can re-use the SSTableInputStream
            // to async read ahead and reduce time spent blocking on I/O
            this.inputStream = new SSTableInputStream<>(source, log.stats());
        }

        private int bufferSize()
        {
            return Math.toIntExact(Math.min(log.maxOffset() - offset, chunkSize));
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            offset = position;
            buffer.clear();
            int length = bufferSize();
            if (length < 0)
            {
                throw new IllegalStateException(String.format("Read passed maxOffset offset=%d maxOffset=%d",
                                                              offset, log.maxOffset()));
            }
            if (buffer.capacity() != length)
            {
                // The buffer size will always be chunkSize or CdcRandomAccessReader.DEFAULT_BUFFER_SIZE until we reach the end
                buffer = ByteBuffer.allocate(length);
            }

            long currentPos = inputStream.bytesRead();
            try
            {
                if (offset < currentPos)
                {
                    // Attempting to read bytes previously read. In practice, we read the CommitLogs sequentially,
                    // but we still need to respect random access reader API, it will just require blocking.
                    int requestLength = buffer.remaining() + 1;
                    long end = offset + requestLength;
                    BlockingStreamConsumer streamConsumer = new BlockingStreamConsumer();
                    source.request(offset, end, streamConsumer);
                    streamConsumer.getBytes(buffer);
                    buffer.flip();
                    return this;
                }

                if (offset > currentPos)
                {
                    // Skip ahead
                    ByteBufferUtils.skipFully(inputStream, offset - currentPos);
                }

                inputStream.read(buffer);
                assert buffer.remaining() == 0;
                buffer.flip();
            }
            catch (IOException exception)
            {
                throw new RuntimeException(ThrowableUtils.rootCause(exception));
            }

            return this;
        }

        @Override
        public void closeReader()
        {
            offset = -1;
            close();
        }

        @Override
        public void close()
        {
            assert offset == -1;  // Reader must be closed at this point
            inputStream.close();
            try
            {
                log.close();
            }
            catch (Exception exception)
            {
                LOGGER.error("Exception closing CommitLog", exception);
            }
            buffer = null;
        }

        @Override
        public ChannelProxy channel()
        {
            throw new IllegalStateException("Channel method should not be used");
        }

        @Override
        public long fileLength()
        {
            return log.maxOffset();
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;  // Only valid for compressed files
        }

        // Buffer holder
        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            // Nothing to do, we don't delete buffers before we're closed
        }
    }

    public static class BlockingStreamConsumer implements StreamConsumer
    {
        private final List<StreamBuffer> buffers;
        private final CompletableFuture<List<StreamBuffer>> future = new CompletableFuture<>();

        BlockingStreamConsumer()
        {
            buffers = new ArrayList<>();
        }

        /**
         * This method should be called by the same thread, but synchronized keyword is added to rely on biased locking
         *
         * @param buffer StreamBuffer wrapping the bytes
         */
        @Override
        public synchronized void onRead(StreamBuffer buffer)
        {
            buffers.add(buffer);
        }

        @Override
        public synchronized void onEnd()
        {
            future.complete(buffers);
        }

        @Override
        public void onError(Throwable throwable)
        {
            future.completeExceptionally(throwable);
        }

        public void getBytes(ByteBuffer destination)
        {
            try
            {
                for (StreamBuffer buffer : future.get())
                {
                    buffer.getBytes(0, destination, buffer.readableBytes());
                }
            }
            catch (InterruptedException exception)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            }
            catch (ExecutionException exception)
            {
                throw new RuntimeException(ThrowableUtils.rootCause(exception));
            }
        }
    }
}
