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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.BigLongArray;

/**
 * Holds metadata about compressed file
 */
// CompressionMetadata is mocked in IndexReaderTests and mockito does not support mocking final classes
// CHECKSTYLE IGNORE: FinalClass
public class CompressionMetadata extends AbstractCompressionMetadata
{
    private final CompressionParams parameters;
    private final double crcCheckChance; // CRC check chance defined on table level
    private final Constructor<org.apache.cassandra.io.compress.CompressionMetadata> constructor;

    private CompressionMetadata(long dataLength, BigLongArray chunkOffsets, CompressionParams parameters, double crcCheckChance)
    {
        super(dataLength, chunkOffsets);
        this.parameters = parameters;
        this.crcCheckChance = crcCheckChance;
        try
        {
            this.constructor = org.apache.cassandra.io.compress.CompressionMetadata.class
                               .getDeclaredConstructor(File.class, CompressionParams.class,
                                                       org.apache.cassandra.io.compress.CompressionMetadata.ChunkOffsetMemory.class,
                                                       long.class, long.class, int.class, int.class, boolean.class);
            this.constructor.setAccessible(true);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    static CompressionMetadata fromInputStream(InputStream inStream, boolean hasCompressedLength, double crcCheckChance) throws IOException
    {
        long dataLength;
        BigLongArray chunkOffsets;

        DataInputStream inData = new DataInputStream(inStream);

        String compressorName = inData.readUTF();
        int optionCount = inData.readInt();
        Map<String, String> options = new HashMap<>(optionCount);
        for (int option = 0; option < optionCount; ++option)
        {
            options.put(inData.readUTF(), inData.readUTF());
        }

        int chunkLength = inData.readInt();
        int minCompressRatio = 2147483647;
        if (hasCompressedLength)
        {
            minCompressRatio = inData.readInt();
        }

        CompressionParams params = new CompressionParams(compressorName, chunkLength, minCompressRatio, options);

        dataLength = inData.readLong();

        int chunkCount = inData.readInt();
        chunkOffsets = new BigLongArray(chunkCount);

        for (int chunk = 0; chunk < chunkCount; chunk++)
        {
            try
            {
                chunkOffsets.set(chunk, inData.readLong());
            }
            catch (EOFException exception)
            {
                throw new EOFException(String.format("Corrupted compression index: read %d but expected %d chunks.",
                                                     chunk, chunkCount));
            }
        }

        return new CompressionMetadata(dataLength, chunkOffsets, params, crcCheckChance);
    }

    ICompressor compressor()
    {
        return parameters.getSstableCompressor();
    }

    @Override
    protected int chunkLength()
    {
        return parameters.chunkLength();
    }

    @Override
    protected double crcCheckChance()
    {
        return crcCheckChance;
    }

    /**
     * @return Cassandra internal {@code CompressionMetadata}, which can be used to construct {@code FileHandle}.
     */
    public org.apache.cassandra.io.compress.CompressionMetadata toInternal(File file, long compressedFileLength)
    {
        try
        {
            AlignedReadonlyLongArrayMemory memory = new AlignedReadonlyLongArrayMemory(chunkOffsets);
            int count = Ints.checkedCast(dataLength / parameters.chunkLength());
            if (dataLength % parameters.chunkLength() != 0)
            {
                count++;
            }

            org.apache.cassandra.io.compress.CompressionMetadata.ChunkOffsetMemory chunkOffsetMemory =
                new org.apache.cassandra.io.compress.CompressionMetadata.ChunkOffsetMemory(memory, count);
            int chunkLengthBits = Integer.numberOfTrailingZeros(parameters.chunkLength());

            return constructor.newInstance(file,
                                           parameters,
                                           chunkOffsetMemory,
                                           getDataLength(),
                                           compressedFileLength,
                                           chunkLengthBits, 0, false);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
