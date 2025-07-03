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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.ReadOnlyInputStreamFileChannel;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.apache.cassandra.spark.reader.SSTableCache;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.utils.FilterFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.reader.BigIndexReader.calculateCompressedSize;

public class BtiReaderUtils
{
    private static final Set<Component> indexComponents = ImmutableSet.of(BtiFormat.Components.DATA,
                                                                          BtiFormat.Components.PARTITION_INDEX,
                                                                          BtiFormat.Components.ROW_INDEX);

    private BtiReaderUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static TokenRange partitionIndexTokenRange(@NotNull SSTable ssTable,
                                                      @NotNull IPartitioner partitioner,
                                                      @NotNull Descriptor descriptor) throws IOException
    {
        AtomicReference<DecoratedKey> firstKey = new AtomicReference<>();
        AtomicReference<DecoratedKey> lastKey = new AtomicReference<>();
        withPartitionIndex(ssTable, descriptor, partitioner, false, false, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            firstKey.set(partitionIndex.firstKey());
            lastKey.set(partitionIndex.lastKey());
        });
        return TokenRange.closed(ReaderUtils.tokenToBigInteger(firstKey.get().getToken()),
                                 ReaderUtils.tokenToBigInteger(lastKey.get().getToken()));
    }

    public static boolean primaryIndexContainsAnyKey(@NotNull SSTable ssTable,
                                                     @NotNull TableMetadata metadata,
                                                     @NotNull Descriptor descriptor,
                                                     @NotNull List<PartitionKeyFilter> filters) throws IOException
    {
        final AtomicBoolean exists = new AtomicBoolean(false);
        withPartitionIndex(ssTable, descriptor, metadata.partitioner, true, true, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            BtiTableReader btiTableReader = new BtiTableReader.Builder(descriptor)
                                            .setDataFile(dataFileHandle)
                                            .setPartitionIndex(partitionIndex)
                                            .setRowIndexFile(rowFileHandle)
                                            .setComponents(indexComponents)
                                            .setTableMetadataRef(metadataRef)
                                            .setFilter(FilterFactory.AlwaysPresent)
                                            .build(null, false, false);

            try (PartitionIterator iter = btiTableReader.keyReader())
            {
                while (!iter.isExhausted())
                {
                    ByteBuffer buffer = iter.key();
                    boolean anyMatch = filters.stream().anyMatch(filter -> filter.matches(buffer));
                    if (anyMatch)
                    {
                        exists.set(true);
                        return;
                    }
                    iter.advance();
                }
            }
            finally
            {
                btiTableReader.selfRef().release();
            }
            exists.set(false);
        });
        return exists.get();
    }

    public static void consumePrimaryIndex(@NotNull SSTable ssTable,
                                           @NotNull TableMetadata metadata,
                                           @NotNull Descriptor descriptor,
                                           @Nullable SparkRangeFilter range,
                                           @NotNull IndexConsumer consumer) throws IOException
    {
        long dataFileLength = ssTable.length(FileType.DATA);
        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
        org.apache.cassandra.spark.reader.CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(
        ssTable, descriptor.version.hasMaxCompressedLength());

        withPartitionIndex(ssTable, descriptor, metadata.partitioner, true, true, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            BtiTableReader btiTableReader = new BtiTableReader.Builder(descriptor)
                                            .setDataFile(dataFileHandle)
                                            .setPartitionIndex(partitionIndex)
                                            .setRowIndexFile(rowFileHandle)
                                            .setComponents(indexComponents)
                                            .setTableMetadataRef(metadataRef)
                                            .setFilter(FilterFactory.AlwaysPresent)
                                            .build(null, false, false);
            try (PartitionIterator iter = btiTableReader.keyReader())
            {
                ByteBuffer prevKey = null;
                long prevPos = 0;
                BigInteger prevToken = null;
                boolean started = false;
                while (!iter.isExhausted())
                {
                    ByteBuffer key = iter.key();
                    long pos = iter.dataPosition();
                    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(key);
                    BigInteger token = ReaderUtils.tokenToBigInteger(decoratedKey.getToken());

                    // TODO(c4c5): Implement reporting statistics.
                    if (prevKey != null && (range == null || range.overlaps(prevToken)))
                    {
                        // we reached the end of the file, so consume last key if overlaps
                        started = true;
                        long uncompressed = pos - prevPos;
                        long compressed = compressionMetadata == null
                                          ? uncompressed
                                          : calculateCompressedSize(compressionMetadata, dataFileLength, prevPos, pos - 1);
                        consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                    }
                    else if (started)
                    {
                        // we have gone passed the range we care about so exit early
                        return;
                    }

                    prevKey = key;
                    prevPos = pos;
                    prevToken = token;

                    iter.advance();
                }

                if (prevKey != null && (range == null || range.overlaps(prevToken)))
                {
                    // we reached the end of the file, so consume last key if overlaps
                    long end = (compressionMetadata == null ? dataFileLength : compressionMetadata.getDataLength());
                    long uncompressed = end - prevPos;
                    long compressed = compressionMetadata == null
                                      ? uncompressed
                                      : calculateCompressedSize(compressionMetadata, dataFileLength, prevPos, end - 1);
                    consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                }
            }
            finally
            {
                btiTableReader.selfRef().release();
            }
        });

    }

    public static void readPrimaryIndex(@NotNull SSTable ssTable,
                                        @NotNull IPartitioner partitioner,
                                        @NotNull Descriptor descriptor,
                                        @NotNull Function<ByteBuffer, Boolean> tracker) throws IOException
    {
        withPartitionIndex(ssTable, descriptor, partitioner, true, true, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            try (PartitionIterator iter = PartitionIterator.create(partitionIndex, partitioner, rowFileHandle, dataFileHandle, descriptor.version))
            {
                while (!iter.isExhausted())
                {
                    ByteBuffer key = iter.key();
                    if (tracker.apply(key))
                    {
                        // exit early if tracker returns true
                        return;
                    }
                    iter.advance();
                }
            }
        });
    }

    private static void withPartitionIndex(@NotNull SSTable ssTable,
                                           @NotNull Descriptor descriptor,
                                           @NotNull IPartitioner partitioner,
                                           boolean loadDataFile,
                                           boolean loadRowsIndex,
                                           @NotNull BtiPartitionIndexConsumer consumer) throws IOException
    {
        File file = new File(ssTable.getDataFileName());
        CompressionMetadata compression = getCompressionMetadata(ssTable, descriptor);

        try (FileHandle dataFileHandle = loadDataFile ? createFileHandle(file,
                                                                         ssTable.openDataStream(),
                                                                         ssTable.length(FileType.DATA),
                                                                         compression) : null;
             FileHandle partitionFileHandle = createFileHandle(file,
                                                               ssTable.openPrimaryIndexStream(),
                                                               ssTable.length(FileType.PARTITIONS_INDEX),
                                                               null);
             FileHandle rowFileHandle = loadRowsIndex ? createFileHandle(file,
                                                                         ssTable.openRowIndexStream(),
                                                                         ssTable.length(FileType.ROWS_INDEX),
                                                                         null) : null;
             PartitionIndex partitionIndex = PartitionIndex.load(partitionFileHandle, partitioner, false))
        {
            consumer.accept(dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex);
        }
    }

    private static FileHandle createFileHandle(File file, InputStream stream, long size, CompressionMetadata compression)
    {
        if (stream == null)
        {
            return null;
        }
        ReadOnlyInputStreamFileChannel fileChannel = new ReadOnlyInputStreamFileChannel((BufferingInputStream<?>) stream, size);
        ChannelProxy proxy = new ChannelProxy(file, fileChannel);
        FileHandle.Builder builder = new FileHandle.Builder(file);
        if (compression != null)
        {
            builder.withCompressionMetadata(compression);
        }
        return builder.complete(f -> proxy);
    }

    private static CompressionMetadata getCompressionMetadata(SSTable ssTable,
                                                              Descriptor descriptor) throws IOException
    {
        org.apache.cassandra.spark.reader.CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(
            ssTable, descriptor.version.hasMaxCompressedLength());
        if (compressionMetadata != null)
        {
            return compressionMetadata.toInternal(descriptor.fileFor(SSTableFormat.Components.COMPRESSION_INFO),
                                                  ssTable.length(FileType.DATA));
        }
        return null;
    }

    public interface BtiPartitionIndexConsumer
    {
        void accept(FileHandle dataFile, FileHandle partitionFile, FileHandle rowFile, PartitionIndex partitionIndex) throws IOException;
    }
}
