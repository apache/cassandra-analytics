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
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.Pair;
import org.jetbrains.annotations.NotNull;

/**
 * Basic cache to reduce wasteful requests on the DataLayer for cacheable SSTable metadata,
 * useful when running many Spark tasks on the same Spark worker
 */
@SuppressWarnings("UnstableApiUsage")
public class SSTableCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableCache.class);

    public static final SSTableCache INSTANCE = new SSTableCache();
    private final Cache<SSTable, SummaryDbUtils.Summary>             summary = buildCache(propOrDefault("sbr.cache.summary.maxEntries", 4096),
                                                                                          propOrDefault("sbr.cache.summary.expireAfterMins", 15));
    private final Cache<SSTable, Pair<DecoratedKey, DecoratedKey>>     index = buildCache(propOrDefault("sbr.cache.index.maxEntries", 128),
                                                                                          propOrDefault("sbr.cache.index.expireAfterMins", 60));
    private final Cache<SSTable, Map<MetadataType, MetadataComponent>> stats = buildCache(propOrDefault("sbr.cache.stats.maxEntries", 16384),
                                                                                          propOrDefault("sbr.cache.stats.expireAfterMins", 60));
    private final Cache<SSTable, BloomFilter>                         filter = buildCache(propOrDefault("sbr.cache.filter.maxEntries", 16384),
                                                                  propOrDefault("sbr.cache.filter.expireAfterMins", 60));
    private final Cache<SSTable, CompressionMetadata>                 compression = buildCache(propOrDefault("sbr.cache.compressionInfo.maxEntries", 128),
                                                                                               propOrDefault("sbr.cache.compressionInfo.expireAfterMins", 15));

    private static int propOrDefault(String name, int defaultValue)
    {
        return propOrDefault(name, defaultValue, Integer::parseInt);
    }

    private static long propOrDefault(String name, long defaultValue)
    {
        return propOrDefault(name, defaultValue, Long::parseLong);
    }

    private static boolean propOrDefault(String name, boolean defaultValue)
    {
        return propOrDefault(name, defaultValue, Boolean::parseBoolean);
    }

    private static <T> T propOrDefault(String name, T defaultValue, Function<String, T> parser)
    {
        String str = System.getProperty(name);
        if (str != null)
        {
            try
            {
                return parser.apply(str);
            }
            catch (NumberFormatException exception)
            {
                LOGGER.error("NumberFormatException for prop {} ", name, exception);
            }
        }
        return defaultValue;
    }

    private <T> Cache<SSTable, T> buildCache(int size, int expireAfterMins)
    {
        return CacheBuilder.newBuilder()
                           .expireAfterAccess(expireAfterMins, TimeUnit.MINUTES)
                           .maximumSize(size)
                           .build();
    }

    public SummaryDbUtils.Summary keysFromSummary(@NotNull TableMetadata metadata,
                                                  @NotNull SSTable ssTable) throws IOException
    {
        return get(summary, ssTable, () -> SummaryDbUtils.readSummary(metadata, ssTable));
    }

    public Pair<DecoratedKey, DecoratedKey> keysFromIndex(@NotNull TableMetadata metadata,
                                                          @NotNull SSTable ssTable) throws IOException
    {
        return get(index, ssTable, () -> ReaderUtils.keysFromIndex(metadata, ssTable));
    }

    public Map<MetadataType, MetadataComponent> componentMapFromStats(@NotNull SSTable ssTable,
                                                                      Descriptor descriptor) throws IOException
    {
        return get(stats, ssTable, () -> ReaderUtils.deserializeStatsMetadata(ssTable, descriptor));
    }

    public BloomFilter bloomFilter(@NotNull SSTable ssTable, Descriptor descriptor) throws IOException
    {
        return get(filter, ssTable, () -> ReaderUtils.readFilter(ssTable, descriptor.version.hasOldBfFormat()));
    }

    public CompressionMetadata compressionMetaData(@NotNull SSTable ssTable, boolean hasMaxCompressedLength) throws IOException
    {
        if (propOrDefault("sbr.cache.compressionInfo.enabled", true))
        {
            long maxSize = propOrDefault("sbr.cache.compressionInfo.maxSize", 0L);
            if (maxSize <= 0 || ssTable.length(FileType.COMPRESSION_INFO) < maxSize)
            {
                return get(compression, ssTable, () -> readCompressionMetadata(ssTable, hasMaxCompressedLength));
            }
        }
        return readCompressionMetadata(ssTable, hasMaxCompressedLength);
    }

    public static CompressionMetadata readCompressionMetadata(@NotNull SSTable ssTable, boolean hasMaxCompressedLength) throws IOException
    {
        try (InputStream cis = ssTable.openCompressionStream())
        {
            if (cis != null)
            {
                return CompressionMetadata.fromInputStream(cis, hasMaxCompressedLength);
            }
        }
        return null;
    }

    boolean containsSummary(@NotNull SSTable ssTable)
    {
        return contains(summary, ssTable);
    }

    boolean containsIndex(@NotNull SSTable ssTable)
    {
        return contains(index, ssTable);
    }

    boolean containsStats(@NotNull SSTable ssTable)
    {
        return contains(stats, ssTable);
    }

    boolean containsFilter(@NotNull SSTable ssTable)
    {
        return contains(filter, ssTable);
    }

    private static <T> boolean contains(@NotNull Cache<SSTable, T> cache, @NotNull SSTable ssTable)
    {
        return cache.getIfPresent(ssTable) != null;
    }

    private static <T> T get(@NotNull Cache<SSTable, T> cache,
                             @NotNull SSTable ssTable,
                             @NotNull Callable<T> callable) throws IOException
    {
        try
        {
            return cache.get(ssTable, callable);
        }
        catch (ExecutionException exception)
        {
            throw toIOException(exception);
        }
    }

    private static IOException toIOException(Throwable throwable)
    {
        IOException ioException = ThrowableUtils.rootCause(throwable, IOException.class);
        return ioException != null ? ioException : new IOException(ThrowableUtils.rootCause(throwable));
    }
}
