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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.util.ThreadUtil;
import org.jetbrains.annotations.NotNull;

public class SSTableWriterImplementation implements SSTableWriter
{
    static
    {
        Config.setClientMode(true);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriterImplementation.class);

    private final CQLSSTableWriter writer;
    private final Path outputDir;
    private final SSTableWatcher sstableWatcher;
    private Consumer<Set<SSTableDescriptor>> producedSSTablesListener;

    public SSTableWriterImplementation(String inDirectory,
                                       String partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       int bufferSizeMB)
    {
        this(inDirectory, partitioner, createStatement, insertStatement, userDefinedTypeStatements, bufferSizeMB, 10);
    }

    @VisibleForTesting
    SSTableWriterImplementation(String inDirectory,
                                String partitioner,
                                String createStatement,
                                String insertStatement,
                                @NotNull Set<String> userDefinedTypeStatements,
                                int bufferSizeMB,
                                long sstableWatcherDelaySeconds)
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
        this.sstableWatcher = new SSTableWatcher(sstableWatcherDelaySeconds);
    }

    private class SSTableWatcher implements Closeable
    {
        // The TOC component is the last one flushed when finishing a SSTable.
        // Therefore, it monitors the creation of the TOC component to determine the creation of SSTable
        private static final String TOC_COMPONENT_SUFFIX = "-TOC.txt";
        private static final String GLOB_PATTERN_FOR_TOC = "*" + TOC_COMPONENT_SUFFIX;

        private final ScheduledExecutorService sstableWatcherScheduler;
        private final Set<SSTableDescriptor> knownSSTables;
        private final Set<SSTableDescriptor> newlyProducedSSTables;

        SSTableWatcher(long delaySeconds)
        {
            ThreadFactory tf = ThreadUtil.threadFactory("SSTableWatcher-" + outputDir.getFileName().toString());
            this.sstableWatcherScheduler = Executors.newSingleThreadScheduledExecutor(tf);
            this.knownSSTables = new HashSet<>();
            this.newlyProducedSSTables = new HashSet<>();
            sstableWatcherScheduler.scheduleWithFixedDelay(this::listSSTables, delaySeconds, delaySeconds, TimeUnit.SECONDS);
        }

        Set<SSTableDescriptor> newlyProducedSSTables()
        {
            if (newlyProducedSSTables.isEmpty())
            {
                return Collections.emptySet();
            }

            synchronized (this)
            {
                Set<SSTableDescriptor> result = new HashSet<>(newlyProducedSSTables);
                knownSSTables.addAll(newlyProducedSSTables);
                newlyProducedSSTables.clear();
                return result;
            }
        }

        private synchronized void listSSTables()
        {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputDir, GLOB_PATTERN_FOR_TOC))
            {
                stream.forEach(path -> {
                    String baseFilename = path.getFileName().toString().replace(TOC_COMPONENT_SUFFIX, "");
                    SSTableDescriptor sstable = new SSTableDescriptor(baseFilename);
                    if (!knownSSTables.contains(sstable))
                    {
                        newlyProducedSSTables.add(sstable);
                    }
                });
            }
            catch (IOException e)
            {
                LOGGER.warn("Fails to list SSTables", e);
            }
        }

        @Override
        public void close()
        {
            sstableWatcherScheduler.shutdown();
            try
            {
                boolean terminated = sstableWatcherScheduler.awaitTermination(10, TimeUnit.SECONDS);
                if (!terminated)
                {
                    LOGGER.debug("SSTableWatcher scheduler termination times out");
                }
            }
            catch (InterruptedException e)
            {
                LOGGER.debug("Closing SSTableWatcher scheduler is interrupted");
            }
            knownSSTables.clear();
            newlyProducedSSTables.clear();
        }
    }

    @Override
    public void addRow(Map<String, Object> values) throws IOException
    {
        Set<SSTableDescriptor> sstables = sstableWatcher.newlyProducedSSTables();
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
    public void setSSTablesProducedListener(Consumer<Set<SSTableDescriptor>> listener)
    {
        producedSSTablesListener = Objects.requireNonNull(listener);
    }

    @Override
    public void close() throws IOException
    {
        // close sstablewatcher first. There is no need to continue monitoring the new sstables. StreamSession should handle the last set of sstables.
        // writer.close is guaranteed to create one more sstable
        sstableWatcher.close();
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
