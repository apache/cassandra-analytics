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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.Nullable;

public enum FileType
{
    DATA("Data.db"),
    INDEX("Index.db"),
    FILTER("Filter.db"),
    STATISTICS("Statistics.db"),
    SUMMARY("Summary.db"),
    COMPRESSION_INFO("CompressionInfo.db"),
    TOC("TOC.txt"),
    DIGEST("Digest.sha1"),
    CRC("CRC.db"),
    CRC32("Digest.crc32"),
    COMMITLOG(".log");

    private final String fileSuffix;

    FileType(String fileSuffix)
    {
        this.fileSuffix = fileSuffix;
    }

    private static final Map<String, FileType> FILE_TYPE_HASH_MAP = new HashMap<>();

    static
    {
        for (FileType fileType : FileType.values())
        {
            FILE_TYPE_HASH_MAP.put(fileType.getFileSuffix(), fileType);
        }
    }

    public static FileType fromExtension(String extension)
    {
        Preconditions.checkArgument(FILE_TYPE_HASH_MAP.containsKey(extension),
                                    "Unknown sstable file type: " + extension);
        return FILE_TYPE_HASH_MAP.get(extension);
    }

    @Nullable
    public static Path resolveComponentFile(FileType fileType, Path dataFilePath)
    {
        Path filePath = fileType == FileType.DATA ? dataFilePath : dataFilePath.resolveSibling(dataFilePath
                .getFileName()
                .toString()
                .replace(FileType.DATA.getFileSuffix(), fileType.getFileSuffix()));
        return Files.exists(filePath) ? filePath : null;
    }

    public String getFileSuffix()
    {
        return fileSuffix;
    }
}
