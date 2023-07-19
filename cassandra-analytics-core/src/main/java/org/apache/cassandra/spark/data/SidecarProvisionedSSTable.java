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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;

import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.clients.SidecarStreamConsumerAdapter;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.SSTableInputStream;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An SSTable that is streamed from Sidecar
 */
public class SidecarProvisionedSSTable extends SSTable
{
    private static final long serialVersionUID = 6452703925812602832L;
    private static final Cache<String, byte[]> COMPRESSION_CACHE = CacheBuilder.newBuilder()
                                                                               .expireAfterAccess(1, TimeUnit.HOURS)
                                                                               .maximumSize(2048)
                                                                               .build();
    private final SidecarClient sidecar;
    private final SidecarInstance instance;
    private final Sidecar.ClientConfig sidecarClientConfig;
    private final String keyspace;
    private final String table;
    private final String snapshotName;
    @NotNull
    private final String dataFileName;
    @NotNull
    private final Map<FileType, ListSnapshotFilesResponse.FileInfo> components;
    private final int partitionId;
    private final Stats stats;

    // CHECKSTYLE IGNORE: Constructor with many parameters
    protected SidecarProvisionedSSTable(SidecarClient sidecar,
                                        Sidecar.ClientConfig sidecarClientConfig,
                                        SidecarInstance instance,
                                        String keyspace,
                                        String table,
                                        String snapshotName,
                                        @NotNull Map<FileType, ListSnapshotFilesResponse.FileInfo> components,
                                        int partitionId,
                                        Stats stats)
    {
        this.sidecar = sidecar;
        this.sidecarClientConfig = sidecarClientConfig;
        this.instance = instance;
        this.keyspace = keyspace;
        this.table = table;
        this.snapshotName = snapshotName;
        this.components = components;
        this.partitionId = partitionId;
        this.stats = stats;
        String fileName = Objects.requireNonNull(components.get(FileType.DATA), "Data.db SSTable file component must exist").fileName;
        String[] ssTableNameParts = fileName.split("-");
        this.dataFileName = parseDataFileName(ssTableNameParts);
    }

    protected String parseDataFileName(String[] ssTableNameParts)
    {
        return String.join("-", ssTableNameParts[0], ssTableNameParts[1], ssTableNameParts[2], ssTableNameParts[3]);
    }

    public SidecarInstance instance()
    {
        return instance;
    }

    public int partitionId()
    {
        return partitionId;
    }

    @NotNull
    @Override
    public String getDataFileName()
    {
        return dataFileName;
    }

    @Nullable
    @Override
    protected InputStream openInputStream(FileType fileType)
    {
        ListSnapshotFilesResponse.FileInfo snapshotFile = components.get(fileType);
        if (snapshotFile == null)
        {
            return null;
        }
        return openStream(snapshotFile.fileName, snapshotFile.size, fileType);
    }

    public long length(FileType fileType)
    {
        ListSnapshotFilesResponse.FileInfo snapshotFile = components.get(fileType);
        if (snapshotFile == null)
        {
            throw new IncompleteSSTableException(fileType);
        }
        return snapshotFile.size;
    }

    @Override
    public boolean isMissing(FileType fileType)
    {
        return !components.containsKey(fileType);
    }

    @Nullable
    private InputStream openStream(String component, long size, FileType fileType)
    {
        if (component == null)
        {
            return null;
        }

        if (fileType == FileType.COMPRESSION_INFO)
        {
            String key = String.format("%s/%s/%s/%s/%s", instance.hostname(), keyspace, table, snapshotName, component);
            byte[] bytes;
            try
            {
                bytes = COMPRESSION_CACHE.get(key, () -> IOUtils.toByteArray(open(component, fileType, size)));
            }
            catch (ExecutionException exception)
            {
                throw new RuntimeException(ThrowableUtils.rootCause(exception));
            }
            return new ByteArrayInputStream(bytes);
        }

        return open(component, fileType, size);
    }

    public InputStream open(String component, FileType fileType, long size)
    {
        SSTableSource<SidecarProvisionedSSTable> ssTableSource = source(component, fileType, size);
        return new SSTableInputStream<>(ssTableSource, stats);
    }

    /**
     * Build an SSTableSource to async provide the bytes
     *
     * @param componentName the SSTable component to stream
     * @param fileType      SSTable file type
     * @param size          file size in bytes
     * @return an SSTableSource implementation that uses Sidecar client to request bytes
     */
    private SSTableSource<SidecarProvisionedSSTable> source(String componentName, FileType fileType, long size)
    {
        SidecarProvisionedSSTable thisSSTable = this;
        return new SSTableSource<SidecarProvisionedSSTable>()
        {
            @Override
            public void request(long start, long end, StreamConsumer consumer)
            {
                sidecar.streamSSTableComponent(instance,
                                               keyspace,
                                               table,
                                               snapshotName,
                                               componentName,
                                               HttpRange.of(start, end),
                                               new SidecarStreamConsumerAdapter(consumer));
            }

            @Override
            public long maxBufferSize()
            {
                return sidecarClientConfig.maxBufferSize(fileType);
            }

            @Override
            public long chunkBufferSize()
            {
                return sidecarClientConfig.chunkBufferSize(fileType);
            }

            @Nullable
            @Override
            public Duration timeout()
            {
                int timeout = sidecarClientConfig.timeoutSeconds();
                return timeout > 0 ? Duration.ofSeconds(timeout) : null;
            }

            @Override
            public SidecarProvisionedSSTable sstable()
            {
                return thisSSTable;
            }

            @Override
            public FileType fileType()
            {
                return fileType;
            }

            @Override
            public long size()
            {
                return size;
            }
        };
    }

    @Override
    public String toString()
    {
        return String.format("{\"hostname\"=\"%s\", \"port\"=\"%d\", \"dataFileName\"=\"%s\", \"partitionId\"=\"%d\"}",
                             instance.hostname(), instance.port(), dataFileName, partitionId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(instance, dataFileName);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        SidecarProvisionedSSTable that = (SidecarProvisionedSSTable) other;
        return this.instance.equals(that.instance)
            && this.dataFileName.equals(that.dataFileName);
    }
}
