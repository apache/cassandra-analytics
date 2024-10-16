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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * {@link DataChunker} helps break down data into chunks according to maxChunkSizeInBytes set.
 */
public class DataChunker
{
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    private static final int MINIMUM_CHUNK_SIZE_IN_BYTES = 5 * 1024 * 1024; // 5MiB. There is no size requirement for the last chunk
    private final int chunkSizeInBytes;

    public DataChunker(int chunkSizeInBytes)
    {
        this(chunkSizeInBytes, true);
    }

    /**
     * Do not use this constructor, unless testing
     * @param chunkSizeInBytes chunk size in bytes
     * @param validate whether enables validtion for chunkSizeInBytes or not
     */
    @VisibleForTesting
    DataChunker(int chunkSizeInBytes, boolean validate)
    {
        if (validate)
        {
            Preconditions.checkArgument(chunkSizeInBytes >= MINIMUM_CHUNK_SIZE_IN_BYTES,
                                        "Chunk size is too small. Minimum size: " + MINIMUM_CHUNK_SIZE_IN_BYTES);
        }
        this.chunkSizeInBytes = chunkSizeInBytes;
    }

    /**
     * Chunk the input stream based on chunkSize
     * @param channel data source file channel
     * @return iterator of ByteBuffers. Call-sites should check {@link ByteBuffer#limit()}
     *         to determine how much data to read, especially the last chunk
     */
    public Iterator<ByteBuffer> chunks(ReadableByteChannel channel)
    {
        return new Iterator<ByteBuffer>()
        {
            private boolean eosReached = false;
            private final ByteBuffer buffer = ByteBuffer.allocate(chunkSizeInBytes);

            public boolean hasNext()
            {
                buffer.clear();
                try
                {
                    eosReached = -1 == channel.read(buffer);
                    buffer.flip();
                }
                catch (IOException e)
                {
                    eosReached = true;
                }
                return !eosReached;
            }

            public ByteBuffer next()
            {
                if (eosReached)
                {
                    throw new NoSuchElementException("End of stream has reached");
                }

                return buffer;
            }
        };
    }
}
