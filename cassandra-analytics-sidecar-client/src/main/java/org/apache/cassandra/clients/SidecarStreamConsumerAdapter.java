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

package org.apache.cassandra.clients;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.sidecar.client.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

/**
 * A delegate class that connects Sidecar's {@link org.apache.cassandra.sidecar.client.StreamConsumer} to the analytics
 * {@link StreamConsumer}
 */
public class SidecarStreamConsumerAdapter implements org.apache.cassandra.sidecar.client.StreamConsumer
{
    private final StreamConsumer delegate;

    public SidecarStreamConsumerAdapter(StreamConsumer delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void onRead(org.apache.cassandra.sidecar.client.StreamBuffer streamBuffer)
    {
        delegate.onRead(SidecarStreamBufferWrapper.wrap(streamBuffer));
    }

    @Override
    public void onComplete()
    {
        delegate.onEnd();
    }

    @Override
    public void onError(Throwable throwable)
    {
        delegate.onError(throwable);
    }

    /**
     * A {@link org.apache.cassandra.spark.utils.streaming.StreamBuffer} implementations that internally wraps
     * Sidecar's {@link org.apache.cassandra.sidecar.client.StreamBuffer}
     */
    static final class SidecarStreamBufferWrapper implements org.apache.cassandra.spark.utils.streaming.StreamBuffer
    {
        public final org.apache.cassandra.sidecar.client.StreamBuffer buffer;

        private SidecarStreamBufferWrapper(StreamBuffer buffer)
        {
            this.buffer = Objects.requireNonNull(buffer, "the buffer parameter must be non-null");
        }

        public static SidecarStreamBufferWrapper wrap(StreamBuffer buffer)
        {
            return new SidecarStreamBufferWrapper(buffer);
        }

        @Override
        public void getBytes(int index, ByteBuffer destination, int length)
        {
            buffer.copyBytes(index, destination, length);
        }

        @Override
        public void getBytes(int index, byte[] destination, int destinationIndex, int length)
        {
            buffer.copyBytes(index, destination, destinationIndex, length);
        }

        @Override
        public byte getByte(int index)
        {
            return buffer.getByte(index);
        }

        @Override
        public int readableBytes()
        {
            return buffer.readableBytes();
        }

        @Override
        public void release()
        {
            buffer.release();
        }
    }
}
