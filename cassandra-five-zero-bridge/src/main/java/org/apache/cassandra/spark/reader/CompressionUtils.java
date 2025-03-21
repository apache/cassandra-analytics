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

package org.apache.cassandra.spark.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;

/**
 * Util class to make it easy to compress/decompress using any compressor
 */
final class CompressionUtils
{
    public ICompressor compressor()
    {
        return ZstdCompressor.getOrCreate(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
    }

    public ByteBuffer compress(byte[] bytes) throws IOException
    {
        ICompressor compressor = compressor();
        ByteBuffer input = compressor.preferredBufferType().allocate(bytes.length);
        input.put(bytes);
        input.flip();
        return compress(input, compressor);
    }

    public ByteBuffer compress(ByteBuffer input) throws IOException
    {
        return compress(input, compressor());
    }

    public ByteBuffer compress(ByteBuffer input, ICompressor compressor) throws IOException
    {
        int rawSize = input.remaining();  // Store uncompressed length as 4 byte int
        // 4 extra bytes to store uncompressed length
        ByteBuffer output = compressor.preferredBufferType().allocate(4 + compressor.initialCompressedBufferLength(rawSize));
        output.putInt(rawSize);
        compressor.compress(input, output);
        output.flip();
        return output;
    }

    public ByteBuffer uncompress(byte[] bytes) throws IOException
    {
        ICompressor compressor = compressor();
        ByteBuffer input = compressor.preferredBufferType().allocate(bytes.length);
        input.put(bytes);
        input.flip();
        return uncompress(input, compressor);
    }

    public ByteBuffer uncompress(ByteBuffer input) throws IOException
    {
        return uncompress(input, compressor());
    }

    public ByteBuffer uncompress(ByteBuffer input, ICompressor compressor) throws IOException
    {
        ByteBuffer output = compressor.preferredBufferType().allocate(input.getInt());
        compressor.uncompress(input, output);
        output.flip();
        return output;
    }
}
