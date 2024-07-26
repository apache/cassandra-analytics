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

package org.apache.cassandra.bridge;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SSTableWriterImplementation implements SSTableWriter
{
    static
    {
        Config.setClientMode(true);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriterImplementation.class);
    private static final String TOC_COMPONENT_SUFFIX = "-TOC.txt";

    private final CQLSSTableWriter writer;
    private final Path outputDir;
    private final WatchService watchService;
    private Consumer<Set<String>> producedSSTablesListener;

    public SSTableWriterImplementation(String inDirectory,
                                       String partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       int bufferSizeMB)
    {
        IPartitioner cassPartitioner = partitioner.toLowerCase().contains("random") ? new RandomPartitioner()
                                                                                    : new Murmur3Partitioner();

        this.writer = configureBuilder(inDirectory,
                                       createStatement,
                                       insertStatement,
                                       bufferSizeMB,
                                       userDefinedTypeStatements,
                                       cassPartitioner)
                      .build();
        this.outputDir = Paths.get(inDirectory);
        this.watchService = setupWatchService();
    }

    protected @Nullable WatchService setupWatchService()
    {
        WatchService ws = null;
        try
        {
            ws = outputDir.getFileSystem().newWatchService();
            outputDir.register(ws, StandardWatchEventKinds.ENTRY_CREATE);
        }
        catch (IOException e)
        {
            LOGGER.warn("Failed to set up WatchService to monitor the newly produced sstables", e);
        }
        return ws;
    }

    @Override
    public void addRow(Map<String, Object> values) throws IOException
    {
        Set<String> sstables = producedSSTables();
        if (!sstables.isEmpty())
        {
            producedSSTablesListener.accept(sstables);
        }

        try
        {
            writer.addRow(values);
        }
        catch (InvalidRequestException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void setSSTablesProducedListener(Consumer<Set<String>> listener)
    {
        producedSSTablesListener = Objects.requireNonNull(listener);
    }

    /**
     * Returns a set of the produced SSTables or null. The method does not block
     * Null is returned when
     * - there is no sstable, or
     * - watchService is not started, or
     * - no producedSSTablesListener is registered
     * <p>
     * The implementation is based on WatchService and there is always certain delay between a file is created and captured by watcher.
     * The consumer also need to note that there may be missing event in rare cases. The consumer is required to find the missing sstables and handle them.
     */
    private Set<String> producedSSTables()
    {
        if (watchService == null || producedSSTablesListener == null)
        {
            return Collections.emptySet();
        }

        Set<String> produced = new HashSet<>();
        WatchKey key;
        while ((key = watchService.poll()) != null)
        {
            List<WatchEvent<?>> events = key.pollEvents();
            // Reset the key ASAP in order to continue receiving events
            // Note that it is still possible that new events happens during key.pollEvents(), and those events won't be captured, since reset was not called
            key.reset();
            for (WatchEvent<?> event : events)
            {
                if (event.kind() != StandardWatchEventKinds.ENTRY_CREATE)
                    continue;

                // for ENTRY_CREATE kind, the context type is Path
                Path createdFile = (Path) event.context();
                // The TOC component is the last one flushed when finishing a SSTable.
                // Therefore, it monitors the creation of the TOC component to determine the creation of SSTable
                if (createdFile.toString().endsWith(TOC_COMPONENT_SUFFIX))
                {
                    String sstableBaseName = createdFile.toString().replace(TOC_COMPONENT_SUFFIX, "");
                    produced.add(sstableBaseName);
                }
            }
        }
        return produced;
    }

    @Override
    public void close() throws IOException
    {
        // close watchservice first. There is no need to continue monitoring the new sstables.
        // writer.close is guaranteed to create one more sstable
        if (watchService != null)
        {
            watchService.close();
        }
        writer.close();
    }

    @VisibleForTesting
    static CQLSSTableWriter.Builder configureBuilder(String inDirectory,
                                                     String createStatement,
                                                     String insertStatement,
                                                     int bufferSizeMB,
                                                     Set<String> udts,
                                                     IPartitioner cassPartitioner)
    {
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();

        for (String udt : udts)
        {
            builder.withType(udt);
        }

        return builder.inDirectory(inDirectory)
                      .forTable(createStatement)
                      .withPartitioner(cassPartitioner)
                      .using(insertStatement)
                      // The data frame to write is always sorted,
                      // see org.apache.cassandra.spark.bulkwriter.CassandraBulkSourceRelation.insert
                      .sorted()
                      .withMaxSSTableSizeInMiB(bufferSizeMB);
    }
}
