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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import o.a.c.sidecar.client.shaded.common.response.ListSnapshotFilesResponse;
import o.a.c.sidecar.client.shaded.common.utils.HttpRange;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.clients.SidecarStreamConsumerAdapter;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.Preconditions;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An SSTable that is streamed from Sidecar
 */
public class SidecarProvisionedSSTable extends SSTable
{
    private static final long serialVersionUID = 6452703925812602832L;
    private static final FileType customComponentFileType = FileType.INDEX; // using INDEX file type for SAI indexes
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
    @NotNull
    private final Map<String, ListSnapshotFilesResponse.FileInfo> customComponents;
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
                                        @NotNull Map<String, ListSnapshotFilesResponse.FileInfo> customComponents,
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
        this.customComponents = customComponents;
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
        return openStream(snapshotFile, fileType);
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

    @NotNull
    @Override
    public Set<String> customComponentNames()
    {
        return customComponents.keySet();
    }

    @Nullable

    @Override
    public int readCustomComponent(@NotNull String componentName,
                                   long position,
                                   @NotNull ByteBuffer destination) throws IOException
    {
        Preconditions.checkArgument(position >= 0, "position must be non-negative");
        if (!destination.hasRemaining())
        {
            return 0;
        }

        ListSnapshotFilesResponse.FileInfo snapshotFile = customComponents.get(componentName);
        if (snapshotFile == null || position >= snapshotFile.size)
        {
            return -1;
        }

        int requested = (int) Math.min(destination.remaining(), snapshotFile.size - position);
        int originalLimit = destination.limit();
        destination.limit(destination.position() + requested);
        CassandraFileSource<SidecarProvisionedSSTable> source = source(snapshotFile,
                                                                       customComponentFileType,
                                                                       position + requested);
        try (BufferingInputStream<SidecarProvisionedSSTable> input =
             new BufferingInputStream<>(source, stats.bufferingInputStreamStats(), position))
        {
            return input.read(destination);
        }
        finally
        {
            destination.limit(originalLimit);
        }
    }

    @Override
    public InputStream openCustomComponent(@NotNull String componentName)
    {
        ListSnapshotFilesResponse.FileInfo snapshotFile = customComponents.get(componentName);
        return snapshotFile == null ? null : open(snapshotFile, customComponentFileType);
    }

    @Override
    public long customComponentLength(@NotNull String componentName)
    {
        ListSnapshotFilesResponse.FileInfo snapshotFile = customComponents.get(componentName);
        Preconditions.checkArgument(snapshotFile != null, "Unknown SSTable component: " + componentName);
        return snapshotFile.size;
    }

    @Nullable
    private InputStream openStream(ListSnapshotFilesResponse.FileInfo snapshotFile, FileType fileType)
    {
        if (snapshotFile == null)
        {
            return null;
        }

        return open(snapshotFile, fileType);
    }

    public InputStream open(ListSnapshotFilesResponse.FileInfo fileInfo, FileType fileType)
    {
        CassandraFileSource<SidecarProvisionedSSTable> ssTableSource = source(fileInfo, fileType);
        return new BufferingInputStream<>(ssTableSource, stats.bufferingInputStreamStats());
    }

    /**
     * Build an CassandraFileSource to async provide the bytes
     *
     * @param fileInfo contains information about the file to stream
     * @param fileType SSTable file type
     * @return an CassandraFileSource implementation that uses Sidecar client to request bytes
     */
    private CassandraFileSource<SidecarProvisionedSSTable> source(ListSnapshotFilesResponse.FileInfo fileInfo, FileType fileType)
    {
        return source(fileInfo, fileType, fileInfo.size);
    }

    private CassandraFileSource<SidecarProvisionedSSTable> source(ListSnapshotFilesResponse.FileInfo fileInfo, FileType fileType, long size)
    {
        SidecarProvisionedSSTable thisSSTable = this;
        return new CassandraFileSource<SidecarProvisionedSSTable>()
        {
            @Override
            public void request(long start, long end, StreamConsumer consumer)
            {
                sidecar.streamSSTableComponent(instance, fileInfo, HttpRange.of(start, end),
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
            public SidecarProvisionedSSTable cassandraFile()
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
    public long customComponentChunkBufferSize(String componentName)
    {
        return sidecarClientConfig.chunkBufferSize(customComponentFileType);
    }

    @Override
    public String toString()
    {
        return "SidecarProvisionedSSTable{" +
               "hostname='" + instance.hostname() + '\'' +
               ", port=" + instance.port() +
               ", keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", snapshotName='" + snapshotName + '\'' +
               ", dataFileName='" + dataFileName + '\'' +
               ", partitionId=" + partitionId +
               '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(instance, keyspace, table, snapshotName, dataFileName);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }
        SidecarProvisionedSSTable that = (SidecarProvisionedSSTable) object;
        return Objects.equals(instance, that.instance)
               && Objects.equals(keyspace, that.keyspace)
               && Objects.equals(table, that.table)
               && Objects.equals(snapshotName, that.snapshotName)
               && Objects.equals(dataFileName, that.dataFileName);
    }
}
