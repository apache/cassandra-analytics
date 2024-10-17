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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

public class FileSystemSource<T extends CassandraFile> implements CassandraFileSource<T>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemSource.class);
    static final ExecutorService FILE_IO_EXECUTOR =
            Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("file-io-%d")
                                                                      .setDaemon(true)
                                                                      .build());

    private final T cassandraFile;
    private final RandomAccessFile file;
    private final FileType fileType;
    private final long length;

    public FileSystemSource(T cassandraFile, FileType fileType, Path path) throws IOException
    {
        this.cassandraFile = cassandraFile;
        this.fileType = fileType;
        this.length = Files.size(path);
        this.file = new RandomAccessFile(path.toFile(), "r");
    }

    @Override
    public long maxBufferSize()
    {
        return chunkBufferSize() * 4L;
    }

    @Override
    public long chunkBufferSize()
    {
        return 16L * 1024L;
    }

    @Override
    public long headerChunkSize()
    {
        return chunkBufferSize();
    }

    public ExecutorService executor()
    {
        return FILE_IO_EXECUTOR;
    }

    @Override
    public void request(long start, long end, StreamConsumer consumer)
    {
        executor().submit(() -> {
            boolean close = length <= end;
            try
            {
                // Start-end range is inclusive but on the final request end == length so we need to exclude
                int increment = close ? 0 : 1;
                byte[] bytes = new byte[(int) (end - start + increment)];
                if (file.getChannel().read(ByteBuffer.wrap(bytes), start) >= 0)
                {
                    consumer.onRead(StreamBuffer.wrap(bytes));
                    consumer.onEnd();
                }
                else
                {
                    close = true;
                }
            }
            catch (Throwable throwable)
            {
                close = true;
                consumer.onError(throwable);
            }
            finally
            {
                if (close)
                {
                    closeSafe();
                }
            }
        });
    }

    public T cassandraFile()
    {
        return cassandraFile;
    }

    public FileType fileType()
    {
        return fileType;
    }

    public long size()
    {
        return length;
    }

    private void closeSafe()
    {
        try
        {
            close();
        }
        catch (Exception exception)
        {
            LOGGER.warn("Exception closing InputStream", exception);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (file != null)
        {
            file.close();
        }
    }
}
