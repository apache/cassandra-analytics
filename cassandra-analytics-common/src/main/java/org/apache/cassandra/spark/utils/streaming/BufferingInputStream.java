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

package org.apache.cassandra.spark.utils.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.spark.stats.IStats;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;

/**
 * The InputStream into the CompactionIterator needs to be a blocking {@link java.io.InputStream},
 * but we don't want to block on network calls, or buffer too much data in memory otherwise we will hit OOMs for large Data.db files.
 * <p>
 * This helper class uses the {@link CassandraFileSource} implementation provided to asynchronously read
 * the T bytes on-demand, managing flow control if not ready for more bytes and buffering enough without reading entirely into memory.
 * <p>
 * The generic {@link CassandraFileSource} allows users to pass in their own implementations to read from any source.
 * <p>
 * This enables the Bulk Reader library to scale to read many SSTables without OOMing, and controls the flow by
 * buffering more bytes on-demand as the data is drained.
 * <p>
 * This class expects the consumer thread to be single-threaded, and the producer thread to be single-threaded OR serialized to ensure ordering of events.
 *
 * @param <FileType> CassandraFile type e.g. SSTable, CommitLog
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class BufferingInputStream<FileType extends CassandraFile> extends InputStream implements StreamConsumer
{
    private static final StreamBuffer.ByteArrayWrapper END_MARKER = StreamBuffer.wrap(new byte[0]);
    private static final StreamBuffer.ByteArrayWrapper FINISHED_MARKER = StreamBuffer.wrap(new byte[0]);
    private static final StreamBuffer.ByteArrayWrapper ERROR_MARKER = StreamBuffer.wrap(new byte[0]);

    private enum StreamState
    {
        Init,
        Reading,
        NextBuffer,
        End,
        Closed
    }

    private final BlockingQueue<StreamBuffer> queue;
    private final CassandraFileSource<FileType> source;
    private final IStats<FileType> stats;
    private final long startTimeNanos;

    // Variables accessed by both producer, consumer & timeout thread so must be volatile or atomic
    private volatile Throwable throwable = null;
    private volatile boolean activeRequest = false;
    private volatile boolean closed = false;
    private final AtomicLong bytesWritten = new AtomicLong(0L);

    // Variables only used by the InputStream consumer thread so do not need to be volatile or atomic
    private long rangeStart = 0L;
    private long bytesRead = 0L;
    private long timeBlockedNanos = 0L;
    private boolean skipping = false;
    private StreamState state = StreamState.Init;
    private StreamBuffer currentBuffer = null;
    private int position;
    private int length;

    /**
     * @param source CassandraFileSource to async provide the bytes after {@link CassandraFileSource#request(long, long, StreamConsumer)} is called
     *
     * @param stats {@link IStats} implementation for recording instrumentation
     */
    public BufferingInputStream(CassandraFileSource<FileType> source, IStats<FileType> stats)
    {
        this.source = source;
        this.queue = new LinkedBlockingQueue<>();
        this.startTimeNanos = System.nanoTime();
        this.stats = stats;
    }

    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    public long timeBlockedNanos()
    {
        return timeBlockedNanos;
    }

    public long bytesWritten()
    {
        return bytesWritten.get();
    }

    public long bytesRead()
    {
        return bytesRead;
    }

    public long bytesBuffered()
    {
        return bytesWritten() - bytesRead();
    }

    public boolean isFinished()
    {
        return bytesWritten() >= source.size();
    }

    private boolean isClosed()
    {
        return state == StreamState.Closed;
    }

    /**
     * Can request more bytes if:
     * 1. a request not already in-flight
     * 2. not in the middle of skip method call
     * 3. the queue buffer is not full i.e. bytes in memory not greater than or equal to maxBufferSize
     * 4. the InputStream is not closed
     *
     * @return true if can request more bytes
     */
    private boolean canRequestMore()
    {
        return !(activeRequest || skipping || isBufferFull() || isClosed());
    }

    /**
     * Maybe request more bytes if possible
     */
    private void maybeRequestMore()
    {
        if (canRequestMore())
        {
            requestMore();
        }
    }

    /**
     * Request more bytes using {@link CassandraFileSource#request(long, long, StreamConsumer)} for the next range
     */
    private void requestMore()
    {
        if (rangeStart >= source.size())
        {
            if (isFinished())
            {
                // If user skips to end of stream we still need to complete,
                // otherwise read() blocks waiting for FINISHED_MARKER
                queue.add(FINISHED_MARKER);
            }
            return;  // Finished
        }

        long chunkSize = rangeStart == 0 ? source.headerChunkSize() : source.chunkBufferSize();
        long rangeEnd = Math.min(source.size(), rangeStart + chunkSize);
        if (rangeEnd >= rangeStart)
        {
            activeRequest = true;
            source.request(rangeStart, rangeEnd, this);
            rangeStart += chunkSize + 1;  // Increment range start pointer for next request
        }
        else
        {
            throw new IllegalStateException(String.format("Tried to request invalid range start=%d end=%d",
                                                          rangeStart, rangeEnd));
        }
    }

    /**
     * The number of bytes buffered is greater than or equal to {@link CassandraFileSource#maxBufferSize()}
     * so wait for queue to drain before requesting more
     *
     * @return true if queue is full
     */
    public boolean isBufferFull()
    {
        return bytesBuffered() >= source.maxBufferSize();
    }

    // Timeout

    /**
     * @param timeout           duration timeout
     * @param nowNanos          current time now in nanoseconds
     * @param lastActivityNanos last activity time in nanoseconds
     * @return the timeout remaining in nanoseconds, or less than or equal to 0 if timeout already expired
     */
    public static long timeoutLeftNanos(Duration timeout, long nowNanos, long lastActivityNanos)
    {
        return Math.min(timeout.toNanos(), timeout.toNanos() - (nowNanos - lastActivityNanos));
    }

    private Throwable timeoutException(Duration timeout)
    {
        return new TimeoutException(String.format("No activity on BufferingInputStream for %d seconds",
                                                  timeout.getSeconds()));
    }

    private void timeoutError(Duration timeout)
    {
        onError(timeoutException(timeout));
    }

    /**
     * {@link StreamConsumer} method implementations
     */
    @Override
    public void onRead(StreamBuffer buffer)
    {
        int length = buffer.readableBytes();
        if (length <= 0 || closed)
        {
            return;
        }
        bytesWritten.addAndGet(length);
        queue.add(buffer);
        stats.inputStreamBytesWritten(source, length);
    }

    @Override
    public void onEnd()
    {
        activeRequest = false;
        if (isFinished())
        {
            queue.add(FINISHED_MARKER);
        }
        else
        {
            queue.add(END_MARKER);
        }
    }

    @Override
    public void onError(@NotNull Throwable throwable)
    {
        this.throwable = ThrowableUtils.rootCause(throwable);
        activeRequest = false;
        queue.add(ERROR_MARKER);
        stats.inputStreamFailure(source, throwable);
    }

    /**
     * {@link java.io.InputStream} method implementations
     */
    @Override
    public int available()
    {
        return Math.toIntExact(bytesBuffered());
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    /**
     * If the schema contains large blobs that can be filtered, then we can more efficiently
     * skip bytes by incrementing the startRange and avoid wastefully streaming bytes across the network
     *
     * @param count number of bytes
     * @return number of bytes actually skipped
     * @throws IOException IOException
     */
    @Override
    public long skip(long count) throws IOException
    {
        if (count <= 0)
        {
            return 0;
        }
        else if (count <= bytesBuffered())
        {
            long actual = super.skip(count);
            stats.inputStreamBytesSkipped(source, actual, 0);
            return actual;
        }

        skipping = true;
        long remaining = count;
        while (activeRequest || !queue.isEmpty())
        {
            // Drain any buffered bytes and block until active request completes and the queue is empty
            remaining -= super.skip(remaining);
            if (remaining <= 0)
            {
                break;
            }
        }

        // Increment range start pointer to efficiently skip without reading bytes across the network unnecessarily
        if (remaining > 0)
        {
            rangeStart += remaining;
            bytesWritten.addAndGet(remaining);
            bytesRead += remaining;
        }

        // Remove skip marker and resume requesting bytes
        skipping = false;
        switch (state)
        {
            case Reading:
            case NextBuffer:
                // Stream is active so request more bytes if queue is not full
                maybeRequestMore();
                break;
            default:
                // If skip() is called before calling read() the Stream will be in StreamState.Init,
                // in this case we need to initialize the stream before request more bytes
                checkState();
        }
        stats.inputStreamBytesSkipped(source, count - remaining, remaining);
        return count;
    }

    /**
     * Allows directly reading into ByteBuffer without intermediate copy
     *
     * @param buffer the ByteBuffer
     * @throws EOFException if attempts to read beyond the end of the file
     * @throws IOException  for failure during I/O
     */
    public void read(ByteBuffer buffer) throws IOException
    {
        for (int remainingLength = buffer.remaining(); 0 < remainingLength; remainingLength = buffer.remaining())
        {
            if (checkState() < 0)
            {
                throw new EOFException();
            }
            int readLength = Math.min(length - position, remainingLength);
            if (0 < readLength)
            {
                currentBuffer.getBytes(position, buffer, readLength);
                position += readLength;
                bytesRead += readLength;
            }
            maybeReleaseBuffer();
        }
    }

    public static void ensureOffsetWithinBounds(int offset, int length, int bufferLength)
    {
        if (offset < 0 || length < 0 || bufferLength < 0 || (length + offset) > bufferLength)
        {
            throw new IndexOutOfBoundsException(String.format("Out of bounds, offset=%d, length=%d, bufferLength=%d",
                                                              offset, length, bufferLength));
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        // Objects.checkFromIndexSize is not available in Java 1.8, so we provide our own implementation
        ensureOffsetWithinBounds(offset, length, buffer.length);
        if (length == 0)
        {
            return 0;
        }

        if (checkState() < 0)
        {
            return -1;
        }

        int readLength = Math.min(this.length - position, length);
        if (readLength > 0)
        {
            currentBuffer.getBytes(position, buffer, offset, readLength);
            position += readLength;
            bytesRead += readLength;
        }
        maybeReleaseBuffer();
        return readLength;
    }

    @Override
    public int read() throws IOException
    {
        do
        {
            if (checkState() < 0)
            {
                return -1;
            }

            if (currentBuffer.readableBytes() == 0)
            {
                // Current buffer might be empty, normally if it is a marker buffer e.g. END_MARKER
                maybeReleaseBuffer();
            }
        } while (currentBuffer == null);

        // Convert to unsigned byte
        int unsigned = currentBuffer.getByte(position++) & 0xFF;
        bytesRead++;
        maybeReleaseBuffer();
        return unsigned;
    }

    @Override
    public void close()
    {
        if (state == StreamState.Closed)
        {
            return;
        }
        else if (state != StreamState.End)
        {
            end();
        }
        state = StreamState.Closed;
        closed = true;
        releaseBuffer();
        queue.clear();
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("reset not supported");
    }

    // Internal Methods for java.io.InputStream

    /**
     * If position >= length, we have finished with this {@link BufferingInputStream#currentBuffer} so release and
     * move to the State {@link StreamState#NextBuffer} so next buffer is popped from the {@link LinkedBlockingQueue}
     * when {@link InputStream#read()} or {@link InputStream#read(byte[], int, int)} is next called
     */
    private void maybeReleaseBuffer()
    {
        maybeRequestMore();
        if (position < length)
        {
            // Still bytes remaining in the currentBuffer so keep reading
            return;
        }

        releaseBuffer();
        state = StreamState.NextBuffer;
        stats.inputStreamByteRead(source, position, queue.size(), (int) (position * 100.0 / (double) source.size()));
    }

    /**
     * Release current buffer
     */
    private void releaseBuffer()
    {
        if (currentBuffer != null)
        {
            currentBuffer.release();
            currentBuffer = null;
        }
    }

    /**
     * Pop next buffer from the queue, block on {@link LinkedBlockingQueue} until bytes are available
     *
     * @throws IOException exception on error
     */
    private void nextBuffer() throws IOException
    {
        long startNanos = System.nanoTime();
        try
        {
            // Block on queue until next buffer available
            Duration timeout = source.timeout();
            if (timeout != null && timeout.getSeconds() > 0)
            {
                currentBuffer = queue.poll(timeout.getSeconds(), TimeUnit.SECONDS);
                if (currentBuffer == null)
                {
                    throw new IOException(timeoutException(timeout));
                }
            }
            else
            {
                currentBuffer = queue.take();
            }
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
        long nanosBlocked = System.nanoTime() - startNanos;
        timeBlockedNanos += nanosBlocked;  // Measure time spent blocking for monitoring
        stats.inputStreamTimeBlocked(source, nanosBlocked);

        length = currentBuffer.readableBytes();
        state = StreamState.Reading;
        position = 0;
        if (currentBuffer == null)
        {
            throw new IOException("Obtained a null buffer from the queue");
        }
    }

    /**
     * When reading from the InputStream first check the state, if stream is already closed or we need to
     * pop off the next buffer from the {@link LinkedBlockingQueue}
     *
     * @return -1 if we have reached the end of the InputStream or 0 if still open
     * @throws IOException throw IOException if stream is already closed
     */
    private int checkState() throws IOException
    {
        switch (state)
        {
            case Closed:
                throw new IOException("Stream is closed");
            case End:
                return -1;
            case Init:
                // First request: start requesting bytes & schedule timeout
                requestMore();
                state = StreamState.NextBuffer;
            case NextBuffer:
                nextBuffer();
                if (currentBuffer == END_MARKER)
                {
                    return handleEndMarker();
                }
                else if (currentBuffer == FINISHED_MARKER)
                {
                    return handleFinishedMarker();
                }
                else if (currentBuffer == ERROR_MARKER)
                {
                    throw new IOException(throwable);
                }
            default:
                // Do nothing
        }
        return 0;
    }

    /**
     * Handle finished marker returned in the queue, indicating all bytes from source have been requested
     * and input stream can close
     *
     * @return always return -1 as stream is closed
     */
    private int handleFinishedMarker()
    {
        releaseBuffer();
        end();
        stats.inputStreamEndBuffer(source);
        return -1;
    }

    /**
     * Handle end marker returned in the queue, indicating previous request has finished
     *
     * @return -1 if we have reached the end of the InputStream or 0 if still open
     * @throws IOException throw IOException if stream is already closed
     */
    private int handleEndMarker() throws IOException
    {
        if (skipping)
        {
            return -1;
        }
        releaseBuffer();
        maybeRequestMore();
        state = StreamState.NextBuffer;
        return checkState();
    }

    /**
     * Reached the end of the InputStream and all bytes have been read
     */
    private void end()
    {
        state = StreamState.End;
        stats.inputStreamEnd(source, System.nanoTime() - startTimeNanos, timeBlockedNanos);
    }
}
