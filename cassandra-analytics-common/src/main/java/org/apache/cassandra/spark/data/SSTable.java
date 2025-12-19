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

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Splitter;

import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class representing a single SSTable.
 * Implementations must override hashCode and equals methods.
 */
public abstract class SSTable implements Serializable, CassandraFile
{
    public static final long serialVersionUID = 42L;

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
        // For BTI format, we just need partitions index
        if (isBtiFormat() && isMissing(FileType.PARTITIONS_INDEX))
        {
            throw new IncompleteSSTableException(FileType.PARTITIONS_INDEX);
        }
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
}
