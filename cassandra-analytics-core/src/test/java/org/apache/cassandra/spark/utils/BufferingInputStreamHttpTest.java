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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

/**
 * Test the {@link BufferingInputStream} by firing up an in-test HTTP server and reading the files with an HTTP client
 * Compares the MD5s to verify file bytes match bytes returned by {@link BufferingInputStream}.
 */
public class BufferingInputStreamHttpTest
{
    static final ExecutorService HTTP_EXECUTOR =
            Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("http-server-%d")
                                                                      .setDaemon(true)
                                                                      .build());
    static final ExecutorService HTTP_CLIENT =
            Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("http-client-%d")
                                                                      .setDaemon(true)
                                                                      .build());
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingInputStreamHttpTest.class);

    @TempDir
    private static Path directory;
    private static final String HOST = "localhost";
    private static final int PORT = 8001;
    private static final int HTTP_CLIENT_CHUNK_SIZE = 8192;

    private static HttpServer SERVER;           // CHECKSTYLE IGNORE: Constant cannot be made final
    private static CloseableHttpClient CLIENT;  // CHECKSTYLE IGNORE: Constant cannot be made final

    @BeforeAll
    public static void setup() throws IOException
    {
        // Create in-test HTTP server to handle range requests and tmp files
        SERVER = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);
        SERVER.setExecutor(HTTP_EXECUTOR);
        SERVER.createContext("/", exchange -> {
            try
            {
                String uri = exchange.getRequestURI().getPath().replaceFirst("/", "");
                Path path = directory.resolve(uri);

                // Extract Range from header
                long size = Files.size(path);
                long start;
                long end;
                String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                if (rangeHeader != null)
                {
                    String[] range = rangeHeader.split("=")[1].split("-");
                    start = Long.parseLong(range[0]);
                    long endValue = Long.parseLong(range[1]);
                    if (endValue < start)
                    {
                        exchange.sendResponseHeaders(416, -1);
                        return;
                    }
                    end = Math.min(size - 1, endValue);
                }
                else
                {
                    start = 0;
                    end = size;
                }

                // Return file bytes within range
                int length = (int) (end - start + 1);
                if (length <= 0)
                {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }

                LOGGER.info("Serving file filename={} start={} end={} length={}",
                            path.getFileName(), start, end, length);
                exchange.sendResponseHeaders(200, length);
                try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"))
                {
                    byte[] bytes = new byte[length];
                    raf.seek(start);
                    raf.read(bytes, 0, length);
                    exchange.getResponseBody().write(bytes);
                }
                exchange.getResponseBody().flush();
            }
            catch (Throwable throwable)
            {
                LOGGER.error("Unexpected exception in in-test HTTP server", throwable);
                exchange.sendResponseHeaders(500, -1);
            }
            finally
            {
                exchange.close();
            }
        });
        SERVER.start();
        LOGGER.info("Started in-test HTTP Server host={} port={}", HOST, PORT);

        CLIENT = HttpClients.createDefault();
    }

    @AfterAll
    public static void tearDown()
    {
        SERVER.stop(0);
    }

    // HTTP client source for reading SSTable from HTTP server
    private static CassandraFileSource<SSTable> buildHttpSource(String filename,
                                                                long size,
                                                                Long maxBufferSize,
                                                                Long chunkBufferSize)
    {
        return new CassandraFileSource<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                CompletableFuture.runAsync(() -> {
                    LOGGER.info("Reading file using HTTP client filename={} start={} end={}", filename, start, end);
                    HttpGet get = new HttpGet("http://" + HOST + ":" + PORT + "/" + filename);
                    get.setHeader("Range", String.format("bytes=%d-%d", start, end));
                    try
                    {
                        CloseableHttpResponse resp = CLIENT.execute(get);
                        if (resp.getStatusLine().getStatusCode() == 200)
                        {
                            try (InputStream is = resp.getEntity().getContent())
                            {
                                int length;
                                byte[] bytes = new byte[HTTP_CLIENT_CHUNK_SIZE];
                                while ((length = is.read(bytes, 0, bytes.length)) >= 0)
                                {
                                    if (length < HTTP_CLIENT_CHUNK_SIZE)
                                    {
                                        byte[] copy = new byte[length];
                                        System.arraycopy(bytes, 0, copy, 0, length);
                                        bytes = copy;
                                    }
                                    consumer.onRead(StreamBuffer.wrap(bytes));
                                    bytes = new byte[HTTP_CLIENT_CHUNK_SIZE];
                                }
                                consumer.onEnd();
                            }
                        }
                        else
                        {
                            consumer.onError(new RuntimeException("Unexpected status code: " + resp.getStatusLine().getStatusCode()));
                        }
                    }
                    catch (IOException exception)
                    {
                        consumer.onError(exception);
                    }
                }, HTTP_CLIENT);
            }

            public long maxBufferSize()
            {
                return maxBufferSize != null ? maxBufferSize : CassandraFileSource.super.maxBufferSize();
            }

            public long chunkBufferSize()
            {
                return chunkBufferSize != null ? chunkBufferSize : CassandraFileSource.super.chunkBufferSize();
            }

            public SSTable cassandraFile()
            {
                return null;
            }

            public FileType fileType()
            {
                return null;
            }

            public long size()
            {
                return size;
            }
        };
    }

    // Tests

    @Test
    public void testSmallChunkSizes()
    {
        List<Long> fileSizes = Arrays.asList(1L, 1536L, 32768L, 250000L, 10485800L);
        qt().forAll(arbitrary().pick(fileSizes)).checkAssert(size -> runHttpTest(size, 8192L, 4096L));
    }

    @Test
    public void testDefaultClientConfig()
    {
        List<Long> fileSizes = Arrays.asList(1L, 13L, 512L, 1024L, 1536L, 32768L, 1000000L, 10485800L, 1000000000L);
        qt().forAll(arbitrary().pick(fileSizes)).checkAssert(this::runHttpTest);
    }

    // HTTP Tester

    private void runHttpTest(long size)
    {
        runHttpTest(size, null, null);
    }

    // Test BufferingInputStream against test HTTP server using HTTP client
    private void runHttpTest(long size, Long maxBufferSize, Long chunkBufferSize)
    {
        try
        {
            Path path = Files.createTempFile(directory, null, null);
            MessageDigest digest = DigestUtils.getMd5Digest();
            try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(path)))
            {
                long remaining = size;
                while (remaining > 0)
                {
                    byte[] bytes = RandomUtils.randomBytes((int) Math.min(remaining, BufferingInputStreamTests.DEFAULT_CHUNK_SIZE));
                    out.write(bytes);
                    digest.update(bytes);
                    remaining -= bytes.length;
                }
            }
            byte[] expectedMD5 = digest.digest();
            assertEquals(size, Files.size(path));
            LOGGER.info("Created random file path={} fileSize={}", path, size);

            // Use HTTP client source to read files across HTTP and verify MD5 matches expected
            byte[] actualMD5;
            long blockingTimeMillis;
            CassandraFileSource<SSTable> source = buildHttpSource(path.getFileName().toString(),
                                                                  Files.size(path),
                                                                  maxBufferSize,
                                                                  chunkBufferSize);
            try (BufferingInputStream<SSTable> is = new BufferingInputStream<>(source, BufferingInputStreamTests.STATS.bufferingInputStreamStats()))
            {
                actualMD5 = DigestUtils.md5(is);
                blockingTimeMillis = TimeUnit.MILLISECONDS.convert(is.timeBlockedNanos(), TimeUnit.NANOSECONDS);
                assertEquals(size, is.bytesRead());
                assertEquals(0L, is.bytesBuffered());
            }
            LOGGER.info("Time spent blocking on InputStream thread blockingTimeMillis={} fileSize={}",
                        blockingTimeMillis, size);
            assertArrayEquals(actualMD5, expectedMD5);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
