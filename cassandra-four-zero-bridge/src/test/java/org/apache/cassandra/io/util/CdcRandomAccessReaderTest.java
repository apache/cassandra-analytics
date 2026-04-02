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
import io.vertx.core.buffer.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class CdcRandomAccessReaderTest
{
    private TestCommitLog testCommitLog;
    private TestCassandraFileSource testSource;

    private CdcRandomAccessReader reader;

    @BeforeEach
    public void setUp()
    {
        testSource = new TestCassandraFileSource();
        testCommitLog = new TestCommitLog("/test/path/commitlog", testSource, ICdcStats.STUB, 1024L);
        testSource.setCommitLog(testCommitLog);
    }

    @Test
    public void testConstructorInitialization()
    {
        // Verify reader is properly initialized with commit log
        reader = new CdcRandomAccessReader(testCommitLog);

        assertThat(reader).isNotNull();
        assertThat(reader.getPath()).isEqualTo("/test/path/commitlog");
    }

    @Test
    public void testCDCRebufferConstructorWithInvalidChunkSize()
    {
        // Verify constructor rejects zero chunk size
        assertThatThrownBy(() -> new CdcRandomAccessReader.CDCRebuffer(testCommitLog, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Chunk size must be a positive integer");

        // Verify constructor rejects negative chunk size
        assertThatThrownBy(() -> new CdcRandomAccessReader.CDCRebuffer(testCommitLog, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Chunk size must be a positive integer");
    }

    @Test
    public void testCDCRebufferSequentialReading() throws IOException
    {
        // Setup: 100 bytes total, 50-byte buffer chunks
        testCommitLog.setMaxOffset(100L);
        final int bufferSize = 50;
        testSource.setRequestHandler(call -> {
            // BufferingInputStream requests a range - we must provide ALL requested data
            long actualEnd = Math.min(call.end, testCommitLog.maxOffset());
            long position = call.start;

            // Deliver data in chunks until request is fulfilled
            while (position <= actualEnd) // range boundaries are inclusive
            {
                int chunkSize = (int) Math.min(actualEnd - position + 1, bufferSize);
                Buffer data = Buffer.buffer();
                for (int i = 0; i < chunkSize; i++)
                {
                    data.appendByte((byte) (position + i));
                }

                TestStreamBuffer streamBuffer = new TestStreamBuffer(data);
                call.consumer.onRead(streamBuffer);
                position += chunkSize;
            }

            // Signal end of stream when reaching EOF
            if (actualEnd >= testCommitLog.maxOffset())
            {
                call.consumer.onEnd();
            }
        });
        CdcRandomAccessReader.CDCRebuffer rebuffer = new CdcRandomAccessReader.CDCRebuffer(testCommitLog, bufferSize);

        // First read: bytes 0-49
        Rebufferer.BufferHolder holder = rebuffer.rebuffer(0);
        assertThat(holder).isNotNull();
        assertThat(rebuffer.offset()).isEqualTo(0L);

        // Verify buffer is in read mode (flipped by rebuffer)
        ByteBuffer buffer = holder.buffer();
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.remaining()).isEqualTo(50);

        // Verify actual byte values (buffer already flipped, ready to read)
        for (int i = 0; i < 50; i++)
        {
            assertThat(buffer.get()).isEqualTo((byte) i);
        }

        // Second read: bytes 50-99 (sequential)
        holder = rebuffer.rebuffer(50);
        assertThat(holder).isNotNull();
        assertThat(rebuffer.offset()).isEqualTo(50L);

        // Verify buffer is in read mode (flipped by rebuffer)
        buffer = holder.buffer();
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.remaining()).isEqualTo(50);

        // Verify actual byte values (buffer already flipped, ready to read)
        for (int i = 0; i < 50; i++)
        {
            assertThat(buffer.get()).isEqualTo((byte) (50 + i));
        }
    }

    @Test
    public void testCDCRebufferBackwardSeek() throws IOException
    {
        // Setup: 100 bytes total, 50-byte buffer chunks
        testCommitLog.setMaxOffset(100L);
        final int bufferSize = 50;
        testSource.setRequestHandler(call -> {
            // We cap delivery at buffer capacity to work around this and test flip() behavior
            long actualEnd = Math.min(call.end, testCommitLog.maxOffset());
            long requestedBytes = actualEnd - call.start + 1; // range boundaries are inclusive
            long position = call.start;
            long bytesToDeliver = Math.min(requestedBytes, bufferSize);

            // Deliver capped amount
            while (position < call.start + bytesToDeliver)
            {
                int chunkSize = (int) Math.min(call.start + bytesToDeliver - position, bufferSize);
                Buffer data = Buffer.buffer();
                for (int i = 0; i < chunkSize; i++)
                {
                    data.appendByte((byte) (position + i));
                }

                TestStreamBuffer streamBuffer = new TestStreamBuffer(data);
                call.consumer.onRead(streamBuffer);
                position += chunkSize;
            }

            // Signal end when complete
            call.consumer.onEnd();
        });
        CdcRandomAccessReader.CDCRebuffer rebuffer = new CdcRandomAccessReader.CDCRebuffer(testCommitLog, bufferSize);

        // First, advance to position 50 (sequential read)
        Rebufferer.BufferHolder holder = rebuffer.rebuffer(50);
        assertThat(holder).isNotNull();
        assertThat(rebuffer.offset()).isEqualTo(50L);

        // Now seek backward to position 0 - triggers backward seek code path
        holder = rebuffer.rebuffer(0);
        assertThat(holder).isNotNull();
        assertThat(rebuffer.offset()).isEqualTo(0L);

        // Verify buffer is in read mode (flipped by rebuffer)
        ByteBuffer buffer = holder.buffer();
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.remaining()).isEqualTo(50);

        // Verify byte values are correct (buffer already flipped, ready to read)
        for (int i = 0; i < 50; i++)
        {
            assertThat(buffer.get()).isEqualTo((byte) i);
        }
    }


    @Test
    public void testCdcRandomAccessReaderEndToEnd()
    {
        // Setup commit log with 1024 bytes
        testCommitLog.setMaxOffset(1024L);

        // Configure source to provide sequential data
        testSource.setRequestHandler(call -> {
            int dataSize = (int) (call.end - call.start + 1);
            Buffer data = Buffer.buffer(dataSize);
            for (int i = 0; i < dataSize; i++)
            {
                data.appendByte((byte) (call.start + i));
            }

            TestStreamBuffer streamBuffer = new TestStreamBuffer(data);

            // Deliver data and signal completion
            call.consumer.onRead(streamBuffer);
            call.consumer.onEnd();
        });

        // Create reader
        reader = new CdcRandomAccessReader(testCommitLog);

        assertThat(reader).isNotNull();
        assertThat(reader.getPath()).isEqualTo("/test/path/commitlog");

        // Verify no premature calls before rebuffer is used
        assertThat(testSource.requestCalls.size()).isEqualTo(0);
    }

    // Test stub classes

    private static class TestCommitLog implements CommitLog
    {
        private final String path;
        private final CassandraFileSource<CommitLog> source;
        private final ICdcStats stats;
        private long maxOffset;
        private boolean closed = false;
        private boolean completed = false;
        private Throwable closeException = null;

        TestCommitLog(String path, CassandraFileSource<CommitLog> source, ICdcStats stats, long maxOffset)
        {
            this.path = path;
            this.source = source;
            this.stats = stats;
            this.maxOffset = maxOffset;
        }

        @Override
        public String path()
        {
            return path;
        }

        @Override
        public CassandraFileSource<CommitLog> source()
        {
            return source;
        }

        @Override
        public ICdcStats stats()
        {
            return stats;
        }

        @Override
        public long maxOffset()
        {
            return maxOffset;
        }

        @Override
        public long length()
        {
            return maxOffset;
        }

        @Override
        public String name()
        {
            return path;
        }

        @Override
        public boolean completed()
        {
            return completed;
        }

        @Override
        public CassandraInstance instance()
        {
            // Return a mock instance - not needed for our tests
            return null;
        }

        @Override
        public void close() throws IOException
        {
            if (closeException != null)
            {
                if (closeException instanceof IOException)
                {
                    throw (IOException) closeException;
                }
                throw new IOException("Close failed", closeException);
            }
            closed = true;
        }

        void setMaxOffset(long maxOffset)
        {
            this.maxOffset = maxOffset;
        }

        void setCloseException(Throwable exception)
        {
            this.closeException = exception;
        }

        void setCompleted(boolean completed)
        {
            this.completed = completed;
        }

        boolean isClosed()
        {
            return closed;
        }
    }

    private static class TestCassandraFileSource implements CassandraFileSource<CommitLog>
    {
        final List<RequestCall> requestCalls = new ArrayList<>();
        Consumer<RequestCall> requestHandler = null;
        private TestCommitLog commitLog;

        @Override
        public void request(long start, long end, StreamConsumer consumer)
        {
            RequestCall call = new RequestCall(start, end, consumer);
            requestCalls.add(call);
            if (requestHandler != null)
            {
                requestHandler.accept(call);
            }
        }

        @Override
        public CommitLog cassandraFile()
        {
            return commitLog;
        }

        @Override
        public FileType fileType()
        {
            return FileType.COMMITLOG;
        }

        @Override
        public long size()
        {
            return commitLog != null ? commitLog.length() : 0L;
        }

        void setCommitLog(TestCommitLog commitLog)
        {
            this.commitLog = commitLog;
        }

        void setRequestHandler(Consumer<RequestCall> handler)
        {
            this.requestHandler = handler;
        }

        static class RequestCall
        {
            // start and end offsets are considered inclusive
            final long start;
            final long end;
            final StreamConsumer consumer;

            RequestCall(long start, long end, StreamConsumer consumer)
            {
                this.start = start;
                this.end = end;
                this.consumer = consumer;
            }
        }
    }

    private static class TestStreamBuffer implements StreamBuffer
    {
        private final Buffer buffer;

        TestStreamBuffer(Buffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public int readableBytes()
        {
            return buffer.length();
        }

        @Override
        public void getBytes(int sourceOffset, ByteBuffer destination, int length)
        {
            destination.put(buffer.getBytes(sourceOffset, sourceOffset + length));
//            destination.flip();
        }

        @Override
        public void getBytes(int sourceOffset, byte[] destination, int destinationIndex, int length)
        {
            buffer.getBytes(sourceOffset, sourceOffset + length, destination, destinationIndex);
        }

        @Override
        public byte getByte(int index)
        {
            return buffer.getByte(index);
        }

        @Override
        public void release()
        {
            // No-op for test implementation
        }
    }
}
