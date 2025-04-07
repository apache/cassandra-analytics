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

package org.apache.cassandra.spark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.utils.RandomUtils;

public class TestUtils
{
    public static final SSTableFormat<?, ?> BIG_FORMAT = BigFormat.getInstance();
    public static final SSTableFormat<?, ?> BTI_FORMAT = new BtiFormat.BtiFormatFactory().getInstance(Collections.emptyMap());
    public static final List<SSTableFormat<?, ?>> SSTABLE_FORMATS = Arrays.asList(BIG_FORMAT, BTI_FORMAT);

    private TestUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static FileType primaryIndexFile(SSTableFormat<?, ?> format)
    {
        if (format instanceof BigFormat)
        {
            return FileType.INDEX;
        }
        if (format instanceof BtiFormat)
        {
            return FileType.PARTITIONS_INDEX;
        }
        throw new IllegalStateException("Unexpected format: " + format);
    }

    public static byte[] randomLowEntropyData()
    {
        return randomLowEntropyData(RandomUtils.randomPositiveInt(16384 - 512) + 512);
    }

    public static byte[] randomLowEntropyData(int size)
    {
        return randomLowEntropyData("Hello world!", size);
    }

    public static byte[] randomLowEntropyData(String str, int size)
    {
        return StringUtils.repeat(str, size / str.length() + 1)
                          .substring(0, size)
                          .getBytes(StandardCharsets.UTF_8);
    }

    public static Stream<Path> getFileType(Path directory, FileType fileType) throws IOException
    {
        return Files.list(directory)
                    .filter(path -> path.getFileName().toString().endsWith("-" + fileType.getFileSuffix()));
    }
}
