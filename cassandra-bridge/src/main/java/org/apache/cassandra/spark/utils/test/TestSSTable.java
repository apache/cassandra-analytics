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

package org.apache.cassandra.spark.utils.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.IOUtils;
import org.jetbrains.annotations.Nullable;

public final class TestSSTable extends SSTable
{
    private final Path dataFile;

    private TestSSTable(Path dataFile)
    {
        this.dataFile = dataFile;
    }

    public static SSTable at(Path dataFile)
    {
        return new TestSSTable(dataFile);
    }

    public static List<SSTable> at(Path... dataFiles)
    {
        return Arrays.stream(dataFiles)
                     .map(TestSSTable::at)
                     .collect(Collectors.toList());
    }

    private static Stream<Path> list(Path directory, FileType type) throws IOException
    {
        return Files.list(directory)
                    .filter(path -> path.getFileName().toString().endsWith("-" + type.getFileSuffix()));
    }

    public static long countIn(Path directory) throws IOException
    {
        return list(directory, FileType.DATA)
                .count();
    }

    public static List<SSTable> allIn(Path directory) throws IOException
    {
        return list(directory, FileType.DATA)
                .map(TestSSTable::at)
                .collect(Collectors.toList());
    }

    public static SSTable firstIn(Path directory) throws IOException
    {
        return list(directory, FileType.DATA)
                .findFirst()
                .map(TestSSTable::at)
                .orElseThrow(FileNotFoundException::new);
    }

    @VisibleForTesting
    public static Path firstIn(Path directory, FileType type) throws IOException
    {
        return list(directory, type)
                .findFirst()
                .orElseThrow(FileNotFoundException::new);
    }

    @Nullable
    @Override
    protected InputStream openInputStream(FileType fileType)
    {
        Path filePath = FileType.resolveComponentFile(fileType, dataFile);
        try
        {
            return filePath != null ? new BufferedInputStream(new FileInputStream(filePath.toFile())) : null;
        }
        catch (FileNotFoundException exception)
        {
            return null;
        }
    }

    public long length(FileType fileType)
    {
        return IOUtils.size(FileType.resolveComponentFile(fileType, dataFile));
    }

    public boolean isMissing(FileType fileType)
    {
        return FileType.resolveComponentFile(fileType, dataFile) == null;
    }

    @Override
    public String getDataFileName()
    {
        return dataFile.getFileName().toString();
    }

    @Override
    public int hashCode()
    {
        return dataFile.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof TestSSTable && this.dataFile.equals(((TestSSTable) other).dataFile);
    }
}
