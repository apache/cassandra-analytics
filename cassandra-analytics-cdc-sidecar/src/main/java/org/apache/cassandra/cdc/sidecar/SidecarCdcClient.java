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

package org.apache.cassandra.cdc.sidecar;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import o.a.c.sidecar.client.shaded.common.utils.HttpRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.clients.Sidecar;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.StreamBuffer;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.exceptions.TransportFailureException;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_OVERRIDE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_CHUNK_BUFFER_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_OVERRIDE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_BUFFER_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_MILLIS_TO_SLEEP;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_POOL_SIZE;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MAX_RETRIES;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_MILLIS_TO_SLEEP;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_SIDECAR_PORT;
import static org.apache.cassandra.spark.utils.Properties.DEFAULT_TIMEOUT_SECONDS;

public class SidecarCdcClient
{
    final ClientConfig config;
    final SidecarClient sidecarClient;
    final ICdcStats stats;

    public SidecarCdcClient(ClientConfig config,
                            SidecarClient sidecarClient,
                            ICdcStats stats)
    {
        this.config = config;
        this.sidecarClient = sidecarClient;
        this.stats = stats;
    }

    public CompletableFuture<List<CommitLog>> listCdcCommitLogSegments(CassandraInstance instance)
    {
        return sidecarClient.listCdcSegments(toSidecarInstance(instance))
                            .thenApply(
                            response ->
                            response.segmentsInfo()
                                    .stream()
                                    .map(segment -> (CommitLog) new SidecarCdcCommitLogSegment(this, instance, segment, config))
                                    .collect(Collectors.toList())
                            ).exceptionally(throwable -> {
            Throwable cause = ThrowableUtils.rootCause(throwable);
            if (cause instanceof TransportFailureException.Nonretryable
                && ((TransportFailureException.Nonretryable) cause).isNotFound())
            {
                // Rescue the 404 not found exception - it is a permitted error
                return Collections.emptyList();
            }
            // Rethrow the other exception
            if (throwable instanceof Error)
            {
                throw (Error) throwable;
            }
            throw new RuntimeException(cause);
        });
    }

    public void streamCdcCommitLogSegment(CassandraInstance instance, String segment, HttpRange httpRange, StreamConsumer streamConsumer)
    {
        sidecarClient.streamCdcSegments(toSidecarInstance(instance), segment, httpRange, new o.a.c.sidecar.client.shaded.client.StreamConsumer()
        {
            @Override
            public void onRead(StreamBuffer streamBuffer)
            {
                streamConsumer.onRead(new org.apache.cassandra.spark.utils.streaming.StreamBuffer()
                {
                    @Override
                    public void getBytes(int index, ByteBuffer destination, int length)
                    {
                        streamBuffer.copyBytes(index, destination, length);
                    }

                    @Override
                    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
                    {
                        streamBuffer.copyBytes(index, destination, destinationIndex, length);
                    }

                    @Override
                    public byte getByte(int index)
                    {
                        return streamBuffer.getByte(index);
                    }

                    @Override
                    public int readableBytes()
                    {
                        return streamBuffer.readableBytes();
                    }

                    @Override
                    public void release()
                    {
                        streamBuffer.release();
                    }
                });
            }

            @Override
            public void onComplete()
            {
                streamConsumer.onEnd();
            }

            @Override
            public void onError(Throwable throwable)
            {
                streamConsumer.onError(throwable);
            }
        });
    }

    protected SidecarInstance toSidecarInstance(CassandraInstance instance)
    {
        return new SidecarInstance()
        {
            @Override
            public int port()
            {
                return config.effectivePort();
            }

            @Override
            public String hostname()
            {
                return instance.nodeName();
            }
        };
    }

    public static final class ClientConfig
    {
        public static final String SIDECAR_PORT = "sidecar_port";
        public static final String MAX_RETRIES_KEY = "maxRetries";
        public static final String DEFAULT_MILLIS_TO_SLEEP_KEY = "defaultMillisToSleep";
        public static final String MAX_MILLIS_TO_SLEEP_KEY = "maxMillisToSleep";
        public static final String MAX_BUFFER_SIZE_BYTES_KEY = "maxBufferSizeBytes";
        public static final String CHUNK_BUFFER_SIZE_BYTES_KEY = "chunkBufferSizeBytes";
        public static final String MAX_POOL_SIZE_KEY = "maxPoolSize";
        public static final String TIMEOUT_SECONDS_KEY = "timeoutSeconds";
        public static final String CASSANDRA_ROLE_KEY = "cassandra_role";
        public static final String DEFAULT_CASSANDRA_ROLE = null;

        private final int userProvidedPort;
        private final int maxRetries;
        private final int maxPoolSize;
        private final int timeoutSeconds;
        private final long millisToSleep;
        private final long maxMillisToSleep;
        private final long maxBufferSize;
        private final long chunkSize;
        private final String cassandraRole;
        private final Map<FileType, Long> maxBufferOverride;
        private final Map<FileType, Long> chunkBufferOverride;

        // CHECKSTYLE IGNORE: Constructor with many parameters
        private ClientConfig(int userProvidedPort,
                             int maxRetries,
                             long millisToSleep,
                             long maxMillisToSleep,
                             long maxBufferSize,
                             long chunkSize,
                             int maxPoolSize,
                             int timeoutSeconds,
                             String cassandraRole,
                             Map<FileType, Long> maxBufferOverride,
                             Map<FileType, Long> chunkBufferOverride)
        {
            this.userProvidedPort = userProvidedPort;
            this.maxRetries = maxRetries;
            this.millisToSleep = millisToSleep;
            this.maxMillisToSleep = maxMillisToSleep;
            this.maxBufferSize = maxBufferSize;
            this.chunkSize = chunkSize;
            this.maxPoolSize = maxPoolSize;
            this.timeoutSeconds = timeoutSeconds;
            this.cassandraRole = cassandraRole;
            this.maxBufferOverride = maxBufferOverride;
            this.chunkBufferOverride = chunkBufferOverride;
        }

        public int userProvidedPort()
        {
            return userProvidedPort;
        }

        public int effectivePort()
        {
            return userProvidedPort == -1 ? DEFAULT_SIDECAR_PORT : userProvidedPort;
        }

        public int maxRetries()
        {
            return maxRetries;
        }

        public long millisToSleep()
        {
            return millisToSleep;
        }

        public long maxMillisToSleep()
        {
            return maxMillisToSleep;
        }

        public long maxBufferSize()
        {
            return maxBufferSize(FileType.COMMITLOG);
        }

        public long maxBufferSize(FileType fileType)
        {
            return maxBufferOverride.getOrDefault(fileType, maxBufferSize);
        }

        public Map<FileType, Long> maxBufferOverride()
        {
            return maxBufferOverride;
        }

        public long chunkBufferSize()
        {
            return chunkBufferSize(FileType.COMMITLOG);
        }

        public long chunkBufferSize(FileType fileType)
        {
            return chunkBufferOverride.getOrDefault(fileType, chunkSize);
        }

        public Map<FileType, Long> chunkBufferOverride()
        {
            return chunkBufferOverride;
        }

        public int maxPoolSize()
        {
            return maxPoolSize;
        }

        public int timeoutSeconds()
        {
            return timeoutSeconds;
        }

        @Nullable
        public String cassandraRole()
        {
            return cassandraRole;
        }

        public static ClientConfig create()
        {
            return ClientConfig.create(-1, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int userProvidedPort, int effectivePort)
        {
            return ClientConfig.create(userProvidedPort, DEFAULT_MAX_RETRIES, DEFAULT_MILLIS_TO_SLEEP);
        }

        public static ClientConfig create(int userProvidedPort, int maxRetries, long millisToSleep)
        {
            Map<FileType, Long> chunkOverride = new HashMap<>();
            chunkOverride.put(FileType.COMMITLOG, 4 * 1024 * 1024L);  // 4MB chunks

            return ClientConfig.create(userProvidedPort,
                                       maxRetries,
                                       millisToSleep,
                                       DEFAULT_MAX_MILLIS_TO_SLEEP,
                                       DEFAULT_MAX_BUFFER_SIZE,
                                       4 * 1024 * 1024L,
                                       DEFAULT_MAX_POOL_SIZE,
                                       DEFAULT_TIMEOUT_SECONDS,
                                       DEFAULT_CASSANDRA_ROLE,
                                       DEFAULT_MAX_BUFFER_OVERRIDE,
                                       chunkOverride);
        }

        public static Map<FileType, Long> buildMaxBufferOverride(Map<String, String> options,
                                                                 Map<FileType, Long> defaultValue)
        {
            return buildOverrideMap(MAX_BUFFER_SIZE_BYTES_KEY, options, defaultValue);
        }

        public static Map<FileType, Long> buildChunkBufferOverride(Map<String, String> options,
                                                                   Map<FileType, Long> defaultValue)
        {
            return buildOverrideMap(CHUNK_BUFFER_SIZE_BYTES_KEY, options, defaultValue);
        }

        private static Map<FileType, Long> buildOverrideMap(String keyPrefix,
                                                            Map<String, String> options,
                                                            Map<FileType, Long> defaultValue)
        {
            Map<FileType, Long> result = new HashMap<>(defaultValue);
            for (FileType type : FileType.values())
            {
                // Override with DataSourceOptions if set, e.g. maxBufferSizeBytes_Index.db
                String key = MapUtils.lowerCaseKey(String.format("%s_%s", keyPrefix, type.getFileSuffix()));
                Optional.ofNullable(options.get(key)).map(Long::parseLong).ifPresent(s -> result.put(type, s));
            }
            return result;
        }

        // CHECKSTYLE IGNORE: Method with many parameters
        public static ClientConfig create(int userProvidedPort,
                                          int maxRetries,
                                          long millisToSleep,
                                          long maxMillisToSleep,
                                          long maxBufferSizeBytes,
                                          long chunkSizeBytes,
                                          int maxPoolSize,
                                          int timeoutSeconds,
                                          String cassandraRole,
                                          Map<FileType, Long> maxBufferOverride,
                                          Map<FileType, Long> chunkBufferOverride)
        {
            return new ClientConfig(userProvidedPort,
                                    maxRetries,
                                    millisToSleep,
                                    maxMillisToSleep,
                                    maxBufferSizeBytes,
                                    chunkSizeBytes,
                                    maxPoolSize,
                                    timeoutSeconds,
                                    cassandraRole,
                                    maxBufferOverride,
                                    chunkBufferOverride);
        }

        public Sidecar.ClientConfig toGenericSidecarConfig()
        {
            return Sidecar.ClientConfig.create(this.userProvidedPort,
                                               this.maxRetries,
                                               this.millisToSleep,
                                               this.maxMillisToSleep,
                                               DEFAULT_MAX_BUFFER_SIZE,
                                               4 * 1024 * 1024L,
                                               this.maxPoolSize,
                                               this.timeoutSeconds,
                                               this.cassandraRole,
                                               this.maxBufferOverride,
                                               this.chunkBufferOverride);
        }
    }
}
