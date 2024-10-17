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

import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

public class CdcRandomAccessReader extends RandomAccessReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcRandomAccessReader.class);
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    final CommitLog log;

    public CdcRandomAccessReader(CommitLog log)
    {
        super(new CDCRebuffer(log));
        this.log = log;
    }

    public String getPath()
    {
        return log.path();
    }

    public static class CDCRebuffer implements Rebufferer, Rebufferer.BufferHolder
    {
        ByteBuffer buffer;
        final CommitLog log;
        final int chunkSize;
        long offset = 0;
        final CassandraFileSource<CommitLog> source;
        private final BufferingInputStream<CommitLog> inputStream;

        CDCRebuffer(CommitLog log)
        {
            this(log, DEFAULT_BUFFER_SIZE);
        }

        CDCRebuffer(CommitLog log, int chunkSize)
        {
            Preconditions.checkArgument(chunkSize > 0, "Chunk size must be a positive integer");
            this.log = log;
            this.chunkSize = chunkSize;
            this.buffer = ByteBuffer.allocate(bufferSize());
            this.source = log.source();

            // we read the CommitLogs sequentially so we can re-use the SSTableInputStream
            // to async read ahead and reduce time spent blocking on i/o
            this.inputStream = new BufferingInputStream<>(source, log.stats());
        }

        private int bufferSize()
        {
            return Math.toIntExact(Math.min(log.maxOffset() - offset, chunkSize));
        }

        public BufferHolder rebuffer(long l)
        {
            offset = l;
            buffer.clear();
            int len = bufferSize();
            if (len < 0)
            {
                throw new IllegalStateException(String.format("Read passed maxOffset offset=%d maxOffset=%d", offset, log.maxOffset()));
            }
            if (buffer.capacity() != len)
            {
                // the buffer size will always be {@link chunkSize} or {@link CdcRandomAccessReader.DEFAULT_BUFFER_SIZE} until we reach the end
                this.buffer = ByteBuffer.allocate(len);
            }

            long currentPos = inputStream.bytesRead();
            try
            {
                if (offset < currentPos)
                {
                    // attempting to read bytes previously read
                    // in practice we read the Commit Logs sequentially
                    // but we still need to respect random access reader API, it will just require blocking
                    int requestLen = buffer.remaining() + 1;
                    long end = offset + requestLen;
                    BlockingStreamConsumer streamConsumer = new BlockingStreamConsumer();
                    source.request(offset, end, streamConsumer);
                    streamConsumer.getBytes(buffer);
                    buffer.flip();
                    return this;
                }

                if (offset > currentPos)
                {
                    // skip ahead
                    ByteBufferUtils.skipFully(inputStream, offset - currentPos);
                }

                inputStream.read(buffer);
                assert buffer.remaining() == 0;
                buffer.flip();
            }
            catch (IOException e)
            {
                throw new RuntimeException(ThrowableUtils.rootCause(e));
            }

            return this;
        }

        public void closeReader()
        {
            offset = -1;
            close();
        }

        public void close()
        {
            assert offset == -1;    // reader must be closed at this point.
            inputStream.close();
            try
            {
                log.close();
            }
            catch (Exception e)
            {
                LOGGER.error("Exception closing CommitLog", e);
            }
            buffer = null;
        }

        @Override
        public ChannelProxy channel()
        {
            throw new IllegalStateException("Channel method should not be used");
        }

        public long fileLength()
        {
            return log.maxOffset();
        }

        public double getCrcCheckChance()
        {
            return 0; // Only valid for compressed files.
        }

        // buffer holder

        public ByteBuffer buffer()
        {
            return buffer;
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            // nothing to do, we don't delete buffers before we're closed.
        }
    }

    public static class BlockingStreamConsumer implements StreamConsumer
    {
        private final List<StreamBuffer> buffers;
        private final CompletableFuture<List<StreamBuffer>> future = new CompletableFuture<>();

        BlockingStreamConsumer()
        {
            this.buffers = new ArrayList<>();
        }

        /**
         * This method should be called by the same thread, but synchronized keyword is added to rely on biased locking.
         *
         * @param buffer StreamBuffer wrapping the bytes.
         */
        public synchronized void onRead(StreamBuffer buffer)
        {
            buffers.add(buffer);
        }

        public synchronized void onEnd()
        {
            future.complete(buffers);
        }

        public void onError(Throwable t)
        {
            future.completeExceptionally(t);
        }

        public void getBytes(ByteBuffer dst)
        {
            try
            {
                for (StreamBuffer buffer : future.get())
                {
                    buffer.getBytes(0, dst, buffer.readableBytes());
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(ThrowableUtils.rootCause(e));
            }
        }
    }
}
