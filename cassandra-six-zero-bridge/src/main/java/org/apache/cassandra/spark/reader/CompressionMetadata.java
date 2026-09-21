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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.BigLongArray;
import org.apache.cassandra.utils.concurrent.Ref;
import org.jetbrains.annotations.Nullable;

/**
 * Holds metadata about compressed file
 */
// CompressionMetadata is mocked in IndexReaderTests and mockito does not support mocking final classes
// CHECKSTYLE IGNORE: FinalClass
@SuppressWarnings("UnstableApiUsage")
public class CompressionMetadata extends AbstractCompressionMetadata implements AutoCloseable
{
    /**
     * Holds one dictionary instance per dictionary id, in place of the {@code CompressionDictionaryManager} that a
     * node runs and that an offline reader has none of. One instance per id matters for more than memory:
     * {@code ZstdDictionaryCompressor} keys its compressors, and the native zstd tables they own, by dictionary
     * equality, which is equality of the dictionary id alone. Two equal instances would share one compressor while
     * their reference counts moved apart, and the count that frees the tables would be the count of whichever
     * instance built the compressor.
     *
     * <p>This structure owns the primary reference of each dictionary and releases it on removal. Every reader
     * holds a reference of its own, so the tables survive a removal that happens mid-read.
     */
    private static final Cache<DictId, CompressionDictionary> DICTIONARIES =
    CacheBuilder.newBuilder()
                .maximumSize(SSTableCache.propOrDefault("sbr.cache.compressionDictionary.maxEntries", 16))
                .expireAfterAccess(SSTableCache.propOrDefault("sbr.cache.compressionDictionary.expireAfterMins", 60),
                                   TimeUnit.MINUTES)
                .<DictId, CompressionDictionary>removalListener(notification -> {
                    CompressionDictionary dictionary = notification.getValue();
                    if (dictionary != null)
                    {
                        dictionary.close();
                    }
                })
                .build();

    private final CompressionParams parameters;
    private final double crcCheckChance; // CRC check chance defined on table level
    @Nullable
    private final CompressionDictionary dictionary;
    @Nullable
    private final Ref<? extends CompressionDictionary> dictionaryRef;
    private volatile ICompressor resolvedCompressor;

    private CompressionMetadata(long dataLength,
                                BigLongArray chunkOffsets,
                                CompressionParams parameters,
                                double crcCheckChance,
                                @Nullable CompressionDictionary deserializedDictionary)
    {
        super(dataLength, chunkOffsets);
        this.parameters = parameters;
        this.crcCheckChance = crcCheckChance;
        if (deserializedDictionary == null)
        {
            this.dictionary = null;
            this.dictionaryRef = null;
        }
        else
        {
            CompressionDictionary shared = intern(deserializedDictionary);
            Ref<? extends CompressionDictionary> ref = shared.tryRef();
            if (ref == null)
            {
                // A removal released the shared instance between the two calls. Keep the instance that this reader
                // deserialized: it decompresses the same bytes, and it only misses the shared compressor
                shared = deserializedDictionary;
                ref = shared.tryRef();
                Preconditions.checkState(ref != null, "Compression dictionary %s is released", shared.dictId());
            }
            this.dictionary = shared;
            this.dictionaryRef = ref;
        }
    }

    /**
     * Releases the primary reference of every dictionary held here. Visible for testing.
     */
    static void evictDictionaries()
    {
        DICTIONARIES.invalidateAll();
    }

    private static CompressionDictionary intern(CompressionDictionary dictionary)
    {
        try
        {
            return DICTIONARIES.get(dictionary.dictId(), () -> dictionary);
        }
        catch (ExecutionException exception)
        {
            // The loader returns a value that the caller already holds, so it cannot fail
            throw new AssertionError(exception);
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

        // Cassandra 6.0 writes a self-contained dictionary section after the chunk offsets, hence the SSTable
        // versions big-pa and bti-ea. A null CompressionDictionaryManager selects the offline decode, and the
        // constructor takes over what the manager would own. deserialize returns null on end of file, which covers
        // every SSTable that an earlier Cassandra version wrote.
        CompressionDictionary dictionary = CompressionDictionary.deserialize(inData, null);

        return new CompressionMetadata(dataLength, chunkOffsets, params, crcCheckChance, dictionary);
    }

    @Nullable
    CompressionDictionary dictionary()
    {
        return dictionary;
    }

    /**
     * Acquires a reference that keeps the dictionary's native zstd tables valid until the caller closes it. A
     * reader that decompresses through {@link #compressor} needs one for as long as it reads, because the primary
     * reference goes away when this metadata leaves {@link SSTableCache}.
     *
     * @return null when this SSTable holds no dictionary
     */
    @Nullable
    Ref<? extends CompressionDictionary> acquireDictionaryRef()
    {
        if (dictionary == null)
        {
            return null;
        }
        Ref<? extends CompressionDictionary> ref = dictionary.tryRef();
        Preconditions.checkState(ref != null, "Compression dictionary %s is released", dictionary.dictId());
        return ref;
    }

    /**
     * Releases this metadata's reference to the compression dictionary. The native zstd tables go away when the
     * last reference does, which is either this one, the primary reference that {@link #DICTIONARIES} owns, or a
     * reference that a reader holds. The call is idempotent, and does nothing for an SSTable with no dictionary.
     */
    @Override
    public void close()
    {
        if (dictionaryRef != null)
        {
            dictionaryRef.close();
        }
    }

    /**
     * Repeats the two branches of the package-private {@code io.compress.CompressionMetadata.resolveCompressor}
     * through the public API, memoised because every chunk asks for the compressor and because attaching a
     * dictionary allocates native state
     */
    ICompressor compressor()
    {
        ICompressor result = resolvedCompressor;
        if (result != null)
        {
            return result;
        }

        synchronized (this)
        {
            if (resolvedCompressor == null)
            {
                ICompressor tableCompressor = parameters.getSstableCompressor();
                // A CompressionInfo component exists only for a compressed table, so the params name a compressor
                Preconditions.checkState(tableCompressor != null, "Compression parameters name no compressor: %s", parameters);
                resolvedCompressor = resolveCompressor(tableCompressor, dictionary);
            }
            return resolvedCompressor;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ICompressor resolveCompressor(ICompressor compressor, @Nullable CompressionDictionary dictionary)
    {
        if (dictionary == null)
        {
            return compressor;
        }

        if (compressor instanceof IDictionaryCompressor)
        {
            IDictionaryCompressor dictionaryCompressor = (IDictionaryCompressor) compressor;
            if (dictionaryCompressor.canConsumeDictionary(dictionary))
            {
                return dictionaryCompressor.getOrCopyWithDictionary(dictionary);
            }
        }

        // The table now compresses without a dictionary, or with a dictionary of another kind, so build the
        // compressor that matches the dictionary that this SSTable holds
        return dictionary.kind().createCompressor(dictionary);
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
        AlignedReadonlyLongArrayMemory memory = new AlignedReadonlyLongArrayMemory(chunkOffsets);
        return new org.apache.cassandra.io.compress.CompressionMetadata(file,
                                                                        parameters,
                                                                        memory,
                                                                        memory.size(),
                                                                        getDataLength(),
                                                                        compressedFileLength,
                                                                        dictionary);
    }
}
