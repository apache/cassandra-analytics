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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.util.CompressionUtil;

/**
 * Util class to make it easy to compress/decompress using any compressor.
 */
public class CompressionUtilImplementation implements CompressionUtil
{
    public static final CompressionUtilImplementation INSTANCE = new CompressionUtilImplementation();

    public ICompressor compressor()
    {
        return ZstdCompressor.getOrCreate(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
    }

    public ByteBuffer compress(final byte[] ar) throws IOException
    {
        final ICompressor compressor = compressor();
        final ByteBuffer input = compressor.preferredBufferType().allocate(ar.length);
        input.put(ar);
        input.flip();
        return compress(input, compressor);
    }

    public ByteBuffer compress(final ByteBuffer input) throws IOException
    {
        return compress(input, compressor());
    }

    public ByteBuffer compress(final ByteBuffer input,
                               final ICompressor compressor) throws IOException
    {
        final int rawSize = input.remaining(); // store uncompressed length as 4 byte int
        final ByteBuffer output = compressor.preferredBufferType()
                                            .allocate(4 + compressor.initialCompressedBufferLength(rawSize)); // 4 extra bytes to store uncompressed length
        output.putInt(rawSize);
        compressor.compress(input, output);
        output.flip();
        return output;
    }

    public ByteBuffer uncompress(final byte[] ar) throws IOException
    {
        final ICompressor compressor = compressor();
        final ByteBuffer input = compressor.preferredBufferType().allocate(ar.length);
        input.put(ar);
        input.flip();
        return uncompress(input, compressor);
    }

    public ByteBuffer uncompress(final ByteBuffer input) throws IOException
    {
        return uncompress(input, compressor());
    }

    public ByteBuffer uncompress(final ByteBuffer input,
                                 final ICompressor compressor) throws IOException
    {
        final ByteBuffer output = compressor.preferredBufferType().allocate(input.getInt());
        compressor.uncompress(input, output);
        output.flip();
        return output;
    }
}
