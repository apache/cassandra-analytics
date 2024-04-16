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

package org.apache.cassandra.spark.bulkwriter.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public final class IOUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

    private static final int HASH_BUFFER_SIZE = 512 * 1024; // 512KiB

    private IOUtils()
    {
        throw new UnsupportedOperationException("Cannot instantiate utility class");
    }

    /**
     * Zip the files under source path. It doest not zip recursively.
     * @param sourcePath directory that contains files to be zipped
     * @param targetPath output zip file path
     * @return compressed size, i.e. the size of the zip file
     * @throws IOException I/O exception during zipping
     */
    public static long zip(Path sourcePath, Path targetPath) throws IOException
    {
        return zip(sourcePath, targetPath, 1);
    }

    /**
     * Zip the files under source path. The files within the maxDepth directory levels are considered.
     * @param sourcePath directory that contains files to be zipped
     * @param targetPath output zip file path
     * @param maxDepth the maximum number of directory levels to visit
     * @return compressed size, i.e. the size of the zip file
     * @throws IOException I/O exception during zipping
     */
    public static long zip(Path sourcePath, Path targetPath, int maxDepth) throws IOException
    {
        if (!Files.isDirectory(sourcePath))
        {
            throw new IOException("Not a directory. sourcePath: " + sourcePath);
        }

        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(targetPath));
             Stream<Path> stream = Files.walk(sourcePath, maxDepth))
        {
            stream.filter(Files::isRegularFile)
                  .forEach(path -> {
                      ZipEntry zipEntry = new ZipEntry(sourcePath.relativize(path).toString());
                      try
                      {
                          zos.putNextEntry(zipEntry);
                          Files.copy(path, zos);
                          zos.closeEntry();
                      }
                      catch (IOException e)
                      {
                          LOGGER.error("Unexpected error while zipping SSTable components, path = {} not zipped, ",
                                       path, e);
                          throw new RuntimeException(e);
                      }
                  });
        }
        return targetPath.toFile().length();
    }

    /**
     * Calculate the checksum of the file using the specified buffer size
     * @param path file
     * @param bufferSize buffer size for file content to calculate checksum
     * @return checksum string
     * @throws IOException I/O exception during checksum calculation
     */
    public static String xxhash32(Path path, int bufferSize) throws IOException
    {
        XXHashFactory factory = XXHashFactory.safeInstance();
        try (InputStream inputStream = Files.newInputStream(path);
             StreamingXXHash32 hasher = factory.newStreamingHash32(0))
        {
            int len;
            byte[] buffer = new byte[bufferSize];
            while ((len = inputStream.read(buffer)) != -1)
            {
                hasher.update(buffer, 0, len);
            }
            return Integer.toHexString(hasher.getValue());
        }
    }

    /**
     * Calculate the checksum of the file using the default buffer size
     * @param path file
     * @return checksum string
     * @throws IOException I/O exception during checksum calculation
     */
    public static String xxhash32(Path path) throws IOException
    {
        return xxhash32(path, HASH_BUFFER_SIZE);
    }
}
