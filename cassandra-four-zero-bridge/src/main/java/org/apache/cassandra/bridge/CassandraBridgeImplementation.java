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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableTombstoneWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.complex.CqlTuple;
import org.apache.cassandra.spark.data.complex.CqlUdt;
import org.apache.cassandra.spark.reader.CompactionStreamScanner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.IndexReader;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.SchemaBuilder;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.reader.SummaryDbUtils;
import org.apache.cassandra.analytics.reader.common.IndexIterator;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.SparkClassLoaderOverride;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class CassandraBridgeImplementation extends CassandraBridge
{
    private static volatile boolean setup = false;

    private final Map<Class<?>, Serializer<?>> kryoSerializers;

    public static void main(String[] args)
    {
        System.out.println(UUIDGen.unixTimestamp(UUID.fromString("ac3f2f40-8637-11ef-a12b-2d1e10948b4b")));

    }

    static
    {
        CassandraBridgeImplementation.setup();
    }

    public static synchronized void setup()
    {
        if (!CassandraBridgeImplementation.setup)
        {
            // We never want to enable mbean registration in the Cassandra code we use so disable it here
            System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
            Config.setClientMode(true);
            // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
            // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
            // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            DatabaseDescriptor.clientInitialization();
            Config config = DatabaseDescriptor.getRawConfig();
            config.memtable_flush_writers = 8;
            config.diagnostic_events_enabled = false;
            config.max_mutation_size_in_kb = config.commitlog_segment_size_in_mb * 1024 / 2;
            config.concurrent_compactors = 4;
            Path tempDirectory;
            try
            {
                tempDirectory = Files.createTempDirectory(UUID.randomUUID().toString());
            }
            catch (IOException exception)
            {
                throw new RuntimeException(exception);
            }
            config.data_file_directories = new String[]{tempDirectory.toString()};
            setupCommitLogConfigs(tempDirectory);
            DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
            Keyspace.setInitialized();

            setup = true;
        }
    }

    public static void setupCommitLogConfigs(Path path)
    {
        String commitLogPath = path + "/commitlog";
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath;
        DatabaseDescriptor.getRawConfig().hints_directory = path + "/hints";
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path + "/saved_caches";
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor",
                                                                          Collections.emptyMap()));
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(32);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(commitLog -> new CommitLogSegmentManagerStandard(commitLog, commitLogPath));
    }

    public CassandraBridgeImplementation()
    {
        // Cassandra-version-specific Kryo serializers
        kryoSerializers = new LinkedHashMap<>();
        kryoSerializers.put(CqlField.class, new CqlField.Serializer(cassandraTypes()));
        kryoSerializers.put(CqlTable.class, new CqlTable.Serializer(cassandraTypes()));
        kryoSerializers.put(CqlUdt.class, new CqlUdt.Serializer(cassandraTypes()));
    }

    public CassandraTypes cassandraTypes()
    {
        return CassandraTypesImplementation.INSTANCE;
    }

    @Override
    public AbstractMap.SimpleEntry<ByteBuffer, BigInteger> getPartitionKey(@NotNull CqlTable table,
                                                                           @NotNull Partitioner partitioner,
                                                                           @NotNull List<String> keys)
    {
        Preconditions.checkArgument(table.partitionKeys().size() > 0);
        ByteBuffer partitionKey = buildPartitionKey(table, keys);
        BigInteger partitionKeyTokenValue = hash(partitioner, partitionKey);
        return new AbstractMap.SimpleEntry<>(partitionKey, partitionKeyTokenValue);
    }

    @VisibleForTesting
    public static ByteBuffer buildPartitionKey(@NotNull CqlTable table, @NotNull List<String> keys)
    {
        List<AbstractType<?>> partitionKeyColumnTypes = partitionKeyColumnTypes(table);
        if (table.partitionKeys().size() == 1)
        {
            // Single partition key
            return partitionKeyColumnTypes.get(0).fromString(keys.get(0));
        }
        else
        {
            // Composite partition key
            ByteBuffer[] buffers = new ByteBuffer[keys.size()];
            for (int index = 0; index < buffers.length; index++)
            {
                buffers[index] = partitionKeyColumnTypes.get(index).fromString(keys.get(index));
            }
            return CompositeType.build(ByteBufferAccessor.instance, buffers);
        }
    }

    @VisibleForTesting
    public static List<AbstractType<?>> partitionKeyColumnTypes(@NotNull CqlTable table)
    {
        return table.partitionKeys().stream()
                    .map(CqlField::type)
                    .map(type -> (CqlType) type)
                    .map(type -> type.dataType(true))
                    .collect(Collectors.toList());
    }

    @Override
    public StreamScanner<RowData> getCompactionScanner(@NotNull CqlTable table,
                                                       @NotNull Partitioner partitioner,
                                                       @NotNull SSTablesSupplier ssTables,
                                                       @Nullable SparkRangeFilter sparkRangeFilter,
                                                       @NotNull Collection<PartitionKeyFilter> partitionKeyFilters,
                                                       @Nullable PruneColumnFilter columnFilter,
                                                       @NotNull TimeProvider timeProvider,
                                                       boolean readIndexOffset,
                                                       boolean useIncrementalRepair,
                                                       @NotNull Stats stats)
    {
        // NOTE: Need to use SchemaBuilder to init keyspace if not already set in Cassandra Schema instance
        SchemaBuilder schemaBuilder = new SchemaBuilder(table, partitioner);
        TableMetadata metadata = schemaBuilder.tableMetaData();
        return new CompactionStreamScanner(metadata, partitioner, timeProvider, ssTables.openAll((ssTable, isRepairPrimary) -> {
            return org.apache.cassandra.spark.reader.SSTableReader.builder(metadata, ssTable)
                                                                  .withSparkRangeFilter(sparkRangeFilter)
                                                                  .withPartitionKeyFilters(partitionKeyFilters)
                                                                  .withColumnFilter(columnFilter)
                                                                  .withReadIndexOffset(readIndexOffset)
                                                                  .withStats(stats)
                                                                  .useIncrementalRepair(useIncrementalRepair)
                                                                  .isRepairPrimary(isRepairPrimary)
                                                                  .build();
        }));
    }

    @Override
    public StreamScanner<IndexEntry> getPartitionSizeIterator(@NotNull CqlTable table,
                                                              @NotNull Partitioner partitioner,
                                                              @NotNull SSTablesSupplier ssTables,
                                                              @Nullable SparkRangeFilter rangeFilter,
                                                              @NotNull TimeProvider timeProvider,
                                                              @NotNull Stats stats,
                                                              @NotNull ExecutorService executor)
    {
        //NOTE: need to use SchemaBuilder to init keyspace if not already set in C* Schema instance
        SchemaBuilder schemaBuilder = new SchemaBuilder(table, partitioner);
        TableMetadata metadata = schemaBuilder.tableMetaData();
        return new IndexIterator<>(ssTables, stats, ((ssTable, isRepairPrimary, consumer) -> new IndexReader(ssTable, metadata, rangeFilter, stats, consumer)));
    }

    @Override
    public CassandraVersion getVersion()
    {
        return CassandraVersion.FOURZERO;
    }

    @Override
    public BigInteger hash(Partitioner partitioner, ByteBuffer key)
    {
        switch (partitioner)
        {
            case RandomPartitioner:
                return RandomPartitioner.instance.getToken(key).getTokenValue();
            case Murmur3Partitioner:
                return BigInteger.valueOf((long) Murmur3Partitioner.instance.getToken(key).getTokenValue());
            default:
                throw new UnsupportedOperationException("Unexpected partitioner: " + partitioner);
        }
    }

    @Override
    public UUID getTimeUUID()
    {
        return UUIDGen.getTimeUUID();
    }

    @Override
    public CqlTable buildSchema(String createStatement,
                                String keyspace,
                                ReplicationFactor replicationFactor,
                                Partitioner partitioner,
                                Set<String> udts,
                                @Nullable UUID tableId,
                                int indexCount)
    {
        return new SchemaBuilder(createStatement, keyspace, replicationFactor, partitioner, cassandraTypes -> udts, tableId, indexCount).build();
    }

    @Override
    public synchronized void writeSSTable(Partitioner partitioner,
                                          String keyspace,
                                          String table,
                                          Path directory,
                                          String createStatement,
                                          String insertStatement,
                                          String updateStatement,
                                          boolean upsert,
                                          Set<CqlField.CqlUdt> udts,
                                          Consumer<Writer> writer)
    {
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .inDirectory(directory.toFile())
                                                           .forTable(createStatement)
                                                           .withPartitioner(getPartitioner(partitioner))
                                                           .using(upsert ? updateStatement : insertStatement)
                                                           .withBufferSizeInMB(128);

        for (CqlField.CqlUdt udt : udts)
        {
            // Add user-defined types to CQL writer
            String statement = udt.createStatement(cassandraTypes(), keyspace);
            builder.withType(statement);
        }

        try (CQLSSTableWriter ssTable = builder.build())
        {
            writer.accept(values -> {
                try
                {
                    ssTable.addRow(values);
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

    public static IPartitioner getPartitioner(Partitioner partitioner)
    {
        return CassandraTypesImplementation.getPartitioner(partitioner);
    }

    @Override
    public SSTableWriter getSSTableWriter(String inDirectory,
                                          String partitioner,
                                          String createStatement,
                                          String insertStatement,
                                          @NotNull Set<String> userDefinedTypeStatements,
                                          int bufferSizeMB)
    {
        return new SSTableWriterImplementation(inDirectory, partitioner, createStatement, insertStatement,
                                               userDefinedTypeStatements, bufferSizeMB);
    }

    @Override
    public SSTableSummary getSSTableSummary(@NotNull String keyspace,
                                            @NotNull String table,
                                            @NotNull SSTable ssTable)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        if (metadata == null)
        {
            throw new RuntimeException("Could not create table metadata needed for reading SSTable summaries for keyspace: " + keyspace);
        }
        try
        {
            SummaryDbUtils.Summary summary = SummaryDbUtils.readSummary(metadata, ssTable);
            Pair<DecoratedKey, DecoratedKey> keys = Pair.create(summary.first(), summary.last());
            if (keys.left == null || keys.right == null)
            {
                keys = ReaderUtils.keysFromIndex(metadata, ssTable);
            }
            if (keys.left == null || keys.right == null)
            {
                throw new RuntimeException("Could not load SSTable first or last tokens for SSTable: " + ssTable.getDataFileName());
            }
            DecoratedKey first = keys.left;
            DecoratedKey last = keys.right;
            BigInteger firstToken = ReaderUtils.tokenToBigInteger(first.getToken());
            BigInteger lastToken = ReaderUtils.tokenToBigInteger(last.getToken());
            return new SSTableSummary(firstToken, lastToken, getSSTablePrefix(ssTable.getDataFileName()));
        }
        catch (final IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private String getSSTablePrefix(String dataFileName)
    {
        return dataFileName.substring(0, dataFileName.lastIndexOf('-') + 1);
    }

    // Version-Specific Test Utility Methods

    @Override
    @VisibleForTesting
    public void writeTombstoneSSTable(Partitioner partitioner,
                                      Path directory,
                                      String createStatement,
                                      String deleteStatement,
                                      Consumer<Writer> consumer)
    {
        try (SSTableTombstoneWriter writer = SSTableTombstoneWriter.builder()
                                                                   .inDirectory(directory.toFile())
                                                                   .forTable(createStatement)
                                                                   .withPartitioner(getPartitioner(partitioner))
                                                                   .using(deleteStatement)
                                                                   .withBufferSizeInMB(128)
                                                                   .build())
        {
            consumer.accept(values -> {
                try
                {
                    writer.addRow(values);
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

    @Override
    @VisibleForTesting
    public void sstableToJson(Path dataDbFile, OutputStream output) throws FileNotFoundException
    {
        if (!Files.exists(dataDbFile))
        {
            throw new FileNotFoundException("Cannot find file " + dataDbFile.toAbsolutePath());
        }
        if (!Descriptor.isValidFile(dataDbFile.toFile()))
        {
            throw new RuntimeException("Invalid sstable file");
        }

        Descriptor desc = Descriptor.fromFilename(dataDbFile.toAbsolutePath().toString());
        try
        {
            TableMetadataRef metadata = TableMetadataRef.forOfflineTools(Util.metadataFromSSTable(desc));
            SSTableReader ssTable = SSTableReader.openNoValidation(desc, metadata);
            ISSTableScanner currentScanner = ssTable.getScanner();
            Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner);
            JsonTransformer.toJson(currentScanner, partitions, false, metadata.get(), output);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    @VisibleForTesting
    public Object toTupleValue(CqlField.CqlTuple type, Object[] values)
    {
        return CqlTuple.toTupleValue(getVersion(), (CqlTuple) type, values);
    }

    @Override
    @VisibleForTesting
    public Object toUserTypeValue(CqlField.CqlUdt type, Map<String, Object> values)
    {
        return CqlUdt.toUserTypeValue(getVersion(), (CqlUdt) type, values);
    }

    // Compression Utils

    private static final ICompressor COMPRESSOR = LZ4Compressor.create(Collections.emptyMap());

    @Override
    public ByteBuffer compress(byte[] bytes) throws IOException
    {
        ByteBuffer input = COMPRESSOR.preferredBufferType().allocate(bytes.length);
        input.put(bytes);
        input.flip();
        return compress(input);
    }

    @Override
    public ByteBuffer compress(ByteBuffer input) throws IOException
    {
        int length = input.remaining();  // Store uncompressed length as 4 byte int
        // 4 extra bytes to store uncompressed length
        ByteBuffer output = COMPRESSOR.preferredBufferType().allocate(4 + COMPRESSOR.initialCompressedBufferLength(length));
        output.putInt(length);
        COMPRESSOR.compress(input, output);
        output.flip();
        return output;
    }

    @Override
    public ByteBuffer uncompress(byte[] bytes) throws IOException
    {
        ByteBuffer input = COMPRESSOR.preferredBufferType().allocate(bytes.length);
        input.put(bytes);
        input.flip();
        return uncompress(input);
    }

    @Override
    public ByteBuffer uncompress(ByteBuffer input) throws IOException
    {
        ByteBuffer output = COMPRESSOR.preferredBufferType().allocate(input.getInt());
        COMPRESSOR.uncompress(input, output);
        output.flip();
        return output;
    }

    // Kryo/Java (De-)Serialization

    @Override
    public void kryoRegister(Kryo kryo)
    {
        kryoSerializers.forEach(kryo::register);
    }

    @Override
    public void javaSerialize(ObjectOutputStream out, Serializable object)
    {
        try
        {
            out.writeObject(object);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public <T> T javaDeserialize(ObjectInputStream in, Class<T> type)
    {
        try (SparkClassLoaderOverride override = new SparkClassLoaderOverride(in, getClass().getClassLoader()))
        {
            return type.cast(in.readObject());
        }
        catch (IOException | ClassNotFoundException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
