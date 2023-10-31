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

package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.Nullable;

public final class IOUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static void closeQuietly(@Nullable AutoCloseable closeable)
    {
        if (closeable != null)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable throwable)
            {
                LOGGER.warn("Exception closing {}", closeable.getClass().getName(), throwable);
            }
        }
    }

    public static boolean isNotEmpty(Path path)
    {
        return size(path) > 0;
    }

    public static long size(Path path)
    {
        try
        {
            return Files.size(path);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public static void clearDirectory(Path path)
    {
        clearDirectory(path, log -> { });
    }

    public static void clearDirectory(Path path, Consumer<Path> logger)
    {
        try (Stream<Path> walker = Files.walk(path))
        {
            walker.sorted(Comparator.reverseOrder())
                  .filter(Files::isRegularFile)
                  .forEach(file -> {
                      try
                      {
                          logger.accept(file);
                          Files.delete(file);
                      }
                      catch (IOException exception)
                      {
                          throw new RuntimeException(exception);
                      }
                  });
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
