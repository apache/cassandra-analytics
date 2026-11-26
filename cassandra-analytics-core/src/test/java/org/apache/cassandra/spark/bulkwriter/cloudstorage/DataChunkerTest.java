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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.utils.ByteBufferUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataChunkerTest
{
    @Test
    public void testChunksGeneratedWithWholeChunks() throws IOException
    {
        testChunking(512, 4, 512 / 4);
    }

    @Test
    public void testChunksGeneratedWithSmallerLastChunk() throws IOException
    {
        testChunking(513, 4, 513 / 4 + 1);
    }

    @Test
    public void testReadFailureBeforeFirstChunk() throws IOException
    {
        ReadableByteChannel channel = mock(ReadableByteChannel.class);
        IOException failure = new IOException("Simulated read failure");
        when(channel.read(any(ByteBuffer.class))).thenThrow(failure);
        Iterator<ByteBuffer> chunks = new DataChunker(4, false).chunks(channel);
        assertThatThrownBy(chunks::hasNext).isInstanceOf(UncheckedIOException.class).hasCause(failure);
    }

    @Test
    public void testReadFailureAfterFirstChunk() throws IOException
    {
        ReadableByteChannel channel = mock(ReadableByteChannel.class);
        IOException failure = new IOException("Simulated read failure");
        when(channel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            buffer.putInt(13);
            return Integer.BYTES;
        }).thenThrow(failure);

        Iterator<ByteBuffer> chunks = new DataChunker(4, false).chunks(channel);
        assertThat(chunks.hasNext()).isTrue();
        assertThat(chunks.next().getInt()).isEqualTo(13);
        assertThatThrownBy(chunks::hasNext).isInstanceOf(UncheckedIOException.class).hasCause(failure);
    }

    private void testChunking(int totalSize, int chunkSize, int expectedChunks) throws IOException
    {
        DataChunker chunker = new DataChunker(chunkSize, false);
        Random rd = new Random();
        byte[] expected = new byte[totalSize];
        rd.nextBytes(expected);

        try (ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(expected)))
        {
            int size = 0;
            Iterator<ByteBuffer> chunks = chunker.chunks(channel);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            while (chunks.hasNext())
            {
                ByteBuffer buffer = chunks.next();
                bos.write(ByteBufferUtils.getArray(buffer));
                size += 1;
            }
            assertThat(size).isEqualTo(expectedChunks);
            assertThat(bos.toByteArray()).isEqualTo(expected);
        }
    }
}
