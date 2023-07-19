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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.SSTableInputStream;
import org.jetbrains.annotations.Nullable;

class FileSystemSSTable extends SSTable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemSSTable.class);

    private final Path dataFilePath;
    private final boolean useSSTableInputStream;
    private final Supplier<Stats> stats;

    FileSystemSSTable(Path dataFilePath, boolean useSSTableInputStream, Supplier<Stats> stats)
    {
        this.dataFilePath = dataFilePath;
        this.useSSTableInputStream = useSSTableInputStream;
        this.stats = stats;
    }

    @Override
    protected InputStream openInputStream(FileType fileType)
    {
        Path filePath = FileType.resolveComponentFile(fileType, dataFilePath);
        if (filePath == null)
        {
            return null;
        }
        try
        {
            return useSSTableInputStream
                    ? new SSTableInputStream<>(new FileSystemSource(this, fileType, filePath), stats.get())
                    : new BufferedInputStream(new FileInputStream(filePath.toFile()));
        }
        catch (FileNotFoundException exception)
        {
            return null;
        }
        catch (IOException exception)
        {
            Throwable cause = ThrowableUtils.rootCause(exception);
            LOGGER.warn("IOException reading local sstable", cause);
            throw new RuntimeException(cause);
        }
    }

    public long length(FileType fileType)
    {
        return IOUtils.size(resolveComponentFile(fileType));
    }

    @Override
    public boolean isMissing(FileType fileType)
    {
        return resolveComponentFile(fileType) == null;
    }

    @Nullable
    private Path resolveComponentFile(FileType fileType)
    {
        return FileType.resolveComponentFile(fileType, dataFilePath);
    }

    @Override
    public String getDataFileName()
    {
        return dataFilePath.getFileName().toString();
    }

    @Override
    public int hashCode()
    {
        return dataFilePath.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof FileSystemSSTable
            && this.dataFilePath.equals(((FileSystemSSTable) other).dataFilePath);
    }
}
