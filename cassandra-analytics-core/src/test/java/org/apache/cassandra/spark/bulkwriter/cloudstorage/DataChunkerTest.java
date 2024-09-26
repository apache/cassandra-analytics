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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.utils.ByteBufferUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
            assertEquals(expectedChunks, size);
            assertArrayEquals(expected, bos.toByteArray());
        }
    }
}
