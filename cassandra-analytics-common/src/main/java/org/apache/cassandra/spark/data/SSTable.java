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

package org.apache.cassandra.spark.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.spark.utils.Preconditions;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_OVERRIDE;

/**
 * Abstract class representing a single SSTable.
 * Implementations must override hashCode and equals methods.
 */
public abstract class SSTable implements Serializable, CassandraFile
{
    public static final long serialVersionUID = 42L;

    protected static final Set<String> customComponentPrefixes = ImmutableSet.of("SAI+");

    private static final String FILENAME_SEPARATOR = "-";
    private static final Splitter filenameSplitter = Splitter.on(FILENAME_SEPARATOR);

    public SSTable()
    {
    }

    @Nullable
    protected abstract InputStream openInputStream(FileType fileType);

    @Nullable
    public InputStream openCompressionStream()
    {
        return openInputStream(FileType.COMPRESSION_INFO);
    }

    @Nullable
    public InputStream openStatsStream()
    {
        return openInputStream(FileType.STATISTICS);
    }

    @Nullable
    public InputStream openSummaryStream()
    {
        return openInputStream(FileType.SUMMARY);
    }

    @Nullable
    public InputStream openPrimaryIndexStream()
    {
        return openInputStream(isBigFormat() ? FileType.INDEX : FileType.PARTITIONS_INDEX);
    }

    @Nullable
    public InputStream openRowIndexStream()
    {
        return openInputStream(FileType.ROWS_INDEX);
    }

    @Nullable
    public InputStream openFilterStream()
    {
        return openInputStream(FileType.FILTER);
    }

    @NotNull
    public InputStream openDataStream()
    {
        return Objects.requireNonNull(openInputStream(FileType.DATA), "Data.db SSTable file component must exist");
    }

    public abstract long length(FileType fileType);

    public abstract boolean isMissing(FileType fileType);

    /**
     * Returns non-standard SSTable component file names exposed by this SSTable.
     *
     * SAI uses dynamically named custom components (files beginning with {@code SAI+})
     * which cannot be represented by {@link FileType} enumeration.
     */
    @NotNull
    public Set<String> customComponentNames()
    {
        return Collections.emptySet();
    }

    /**
     * Opens a non-standard SSTable component by its complete file name.
     */
    @Nullable
    public InputStream openCustomComponent(@NotNull String componentName)
    {
        return null;
    }

    /**
     * Reads a custom component at an absolute byte offset into {@code destination}.
     * Implementations that can perform native range reads should override this method.
     *
     * @return number of bytes read, or {@code -1} when {@code position} is at EOF
     */
    public int readCustomComponent(@NotNull String componentName,
                                   long position,
                                   @NotNull ByteBuffer destination) throws IOException
    {
        Preconditions.checkArgument(position >= 0, "position must be non-negative");
        if (!destination.hasRemaining())
        {
            return 0;
        }

        try (InputStream input = openCustomComponent(componentName))
        {
            if (input == null)
            {
                return -1;
            }

            long skipped = 0;
            while (skipped < position)
            {
                long count = input.skip(position - skipped);
                if (count > 0)
                {
                    skipped += count;
                    continue;
                }
                if (input.read() < 0)
                {
                    return -1;
                }
                skipped++;
            }

            int total = 0;
            byte[] buffer = new byte[Math.min(8192, destination.remaining())];
            while (destination.hasRemaining())
            {
                int count = input.read(buffer, 0, Math.min(buffer.length, destination.remaining()));
                if (count < 0)
                {
                    break;
                }
                destination.put(buffer, 0, count);
                total += count;
            }
            return total == 0 ? -1 : total;
        }
    }

    public long customComponentLength(@NotNull String componentName)
    {
        throw new IllegalArgumentException("Unknown SSTable component: " + componentName);
    }

    public void verify() throws IncompleteSSTableException
    {
        // Need Data.db file
        if (isMissing(FileType.DATA))
        {
            throw new IncompleteSSTableException(FileType.DATA);
        }
        // Need Statistics.db file to open SerializationHeader
        if (isMissing(FileType.STATISTICS))
        {
            throw new IncompleteSSTableException(FileType.STATISTICS);
        }
        // Need Summary.db or Index.db to read first/last partition key
        if (isBigFormat() && isMissing(FileType.SUMMARY) && isMissing(FileType.INDEX))
        {
            throw new IncompleteSSTableException(FileType.SUMMARY, FileType.INDEX);
        }
        // For BTI format, we may need both - partitions and rows index
        if (isBtiFormat() && (isMissing(FileType.PARTITIONS_INDEX) || isMissing(FileType.ROWS_INDEX)))
        {
            throw new IncompleteSSTableException(FileType.PARTITIONS_INDEX, FileType.ROWS_INDEX);
        }
    }

    public long customComponentChunkBufferSize(String componentName)
    {
        return DEFAULT_CHUNK_BUFFER_OVERRIDE.get(FileType.INDEX);
    }

    public abstract String getDataFileName();

    public boolean isBigFormat()
    {
        return getDataFileName().contains("-big-");
    }

    public boolean isBtiFormat()
    {
        return getDataFileName().contains("-bti-");
    }

    public String getFormat()
    {
        List<String> tokens = filenameSplitter.splitToList(getDataFileName());
        return tokens.get(tokens.size() - 2);
    }

    public String getVersion()
    {
        List<String> tokens = filenameSplitter.splitToList(getDataFileName());
        return tokens.get(tokens.size() - 4);
    }

    public static boolean isCustomComponentSupported(String fileName)
    {
        int separator = fileName.lastIndexOf('-');
        String componentName = separator > 0 ? fileName.substring(separator + 1) : fileName;
        for (String type : customComponentPrefixes)
        {
            if (componentName.startsWith(type))
            {
                return true;
            }
        }
        return false;
    }
}
