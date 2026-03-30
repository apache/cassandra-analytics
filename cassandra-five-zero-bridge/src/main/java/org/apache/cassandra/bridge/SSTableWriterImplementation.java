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
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.jetbrains.annotations.NotNull;

/**
 * Cassandra 5.0 SSTableWriter implementation with SAI index generation support.
 * <p>
 * When provided with CREATE INDEX statements, this writer attempts to configure the
 * CQLSSTableWriter to generate SAI index component files alongside SSTables.
 * This requires Cassandra 5.0+ with SAI support in CQLSSTableWriter.
 */
@NotThreadSafe
public class SSTableWriterImplementation implements SSTableWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriterImplementation.class);

    static
    {
        Config.setClientMode(true);
    }

    private final CQLSSTableWriter writer;
    private Consumer<Set<SSTableDescriptor>> producedSSTablesListener;

    public SSTableWriterImplementation(String inDirectory,
                                       String partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       int bufferSizeMB)
    {
        this(inDirectory, determineSupportedPartitioner(partitioner), createStatement, insertStatement,
             userDefinedTypeStatements, java.util.Collections.emptySet(), bufferSizeMB);
    }

    public SSTableWriterImplementation(String inDirectory,
                                       String partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       @NotNull Set<String> indexCreateStatements,
                                       int bufferSizeMB)
    {
        this(inDirectory, determineSupportedPartitioner(partitioner), createStatement, insertStatement,
             userDefinedTypeStatements, indexCreateStatements, bufferSizeMB);
    }

    @VisibleForTesting
    public SSTableWriterImplementation(String inDirectory,
                                       IPartitioner partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       int bufferSizeMB)
    {
        this(inDirectory, partitioner, createStatement, insertStatement,
             userDefinedTypeStatements, java.util.Collections.emptySet(), bufferSizeMB);
    }

    @VisibleForTesting
    public SSTableWriterImplementation(String inDirectory,
                                       IPartitioner partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       @NotNull Set<String> userDefinedTypeStatements,
                                       @NotNull Set<String> indexCreateStatements,
                                       int bufferSizeMB)
    {
        CQLSSTableWriter.Builder builder = configureBuilder(inDirectory,
                                                            createStatement,
                                                            insertStatement,
                                                            bufferSizeMB,
                                                            userDefinedTypeStatements,
                                                            this::onSSTablesProduced,
                                                            partitioner);
        if (!indexCreateStatements.isEmpty())
        {
            configureIndexes(builder, indexCreateStatements);
        }
        this.writer = builder.build();
    }

    private static IPartitioner determineSupportedPartitioner(String partitioner)
    {
        return partitioner.toLowerCase().contains("random")
               ? new RandomPartitioner()
               : new Murmur3Partitioner();
    }

    private void onSSTablesProduced(Collection<SSTableReader> sstables)
    {
        Objects.requireNonNull(producedSSTablesListener, "producedSSTablesListener is not set");
        Set<SSTableDescriptor> sstableDescriptors = sstables
                                                    .stream()
                                                    .map(sstable -> {
                                                        String baseFilename = CassandraBridgeImplementation.baseFilename(sstable.descriptor);
                                                        sstable.selfRef().close();
                                                        return new SSTableDescriptor(baseFilename);
                                                    })
                                                    .collect(Collectors.toSet());

        producedSSTablesListener.accept(sstableDescriptors);
    }

    @Override
    public void addRow(Map<String, Object> values) throws IOException
    {
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
        writer.close();
    }

    @VisibleForTesting
    static CQLSSTableWriter.Builder configureBuilder(String inDirectory,
                                                     String createStatement,
                                                     String insertStatement,
                                                     int bufferSizeMB,
                                                     Set<String> udts,
                                                     Consumer<Collection<SSTableReader>> producedSSTablesListener,
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
                      .sorted()
                      .withSSTableProducedListener(producedSSTablesListener)
                      .openSSTableOnProduced()
                      .withMaxSSTableSizeInMiB(bufferSizeMB);
    }

    /**
     * Configures the CQLSSTableWriter builder to generate SAI index components.
     * Uses reflection to call withIndexes/withBuildIndexes methods since they may not be
     * present in all Cassandra 5.0.x patch versions.
     *
     * @param builder              the CQLSSTableWriter builder
     * @param indexCreateStatements the CREATE INDEX CQL statements
     */
    @VisibleForTesting
    static void configureIndexes(CQLSSTableWriter.Builder builder, Set<String> indexCreateStatements)
    {
        try
        {
            // Use reflection to check for SAI index generation support in CQLSSTableWriter.Builder
            Method withIndexMethod = CQLSSTableWriter.Builder.class.getMethod("withIndex", String.class);
            Method withBuildIndexesMethod = CQLSSTableWriter.Builder.class.getMethod("withBuildIndexes", boolean.class);

            for (String indexStatement : indexCreateStatements)
            {
                withIndexMethod.invoke(builder, indexStatement);
            }
            withBuildIndexesMethod.invoke(builder, true);

            LOGGER.info("SAI index generation enabled for {} index(es)", indexCreateStatements.size());
        }
        catch (NoSuchMethodException exception)
        {
            LOGGER.warn("CQLSSTableWriter does not support inline SAI index generation in this Cassandra version. "
                      + "Indexes will need to be rebuilt after import. indexCount={}", indexCreateStatements.size());
        }
        catch (ReflectiveOperationException exception)
        {
            LOGGER.error("Failed to configure SAI index generation", exception);
            throw new RuntimeException("Failed to configure SAI index generation", exception);
        }
    }
}
