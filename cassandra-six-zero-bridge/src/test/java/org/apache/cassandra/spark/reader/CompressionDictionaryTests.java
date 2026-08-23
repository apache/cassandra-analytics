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

package org.apache.cassandra.spark.reader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.github.luben.zstd.ZstdDictTrainer;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.BridgeInitializationParameters;
import org.apache.cassandra.bridge.CassandraTypesImplementation;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.analytics.stats.Stats;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for the Cassandra 6.0 compression dictionary.
 * <p>
 * Cassandra 6.0 appends a compression dictionary to the CompressionInfo component, which is the only reason
 * for the SSTable versions big-pa and bti-ea. These tests write an SSTable with a trained zstd dictionary
 * through the Cassandra 6.0 {@link CQLSSTableWriter}, then read every row back through the analytics
 * {@link SSTableReader}. The negative case writes a second table with a dictionary-less compressor, so that a
 * reader that ignores the dictionary section cannot pass both tests.
 */
class CompressionDictionaryTests
{
    private static final String KEYSPACE = "dictionary_keyspace";
    // CQLSSTableWriter submits the CREATE TABLE statement with ignoreIfExists, and every test in this JVM
    // shares one schema, so each test needs its own table to get its own compression options.
    private static final String DICTIONARY_TABLE = "dictionary_table";
    private static final String PLAIN_TABLE = "plain_table";
    private static final String LIFECYCLE_TABLE = "lifecycle_table";
    // A compressor that accepts a dictionary. CompressionParams.isDictionaryCompressionEnabled() is true only
    // for this class, and CQLSSTableWriter rejects a dictionary on any other compressor.
    private static final String DICTIONARY_COMPRESSION = "{'class': 'ZstdDictionaryCompressor'}";
    private static final String PLAIN_COMPRESSION = "{'class': 'ZstdCompressor'}";
    private static final int PARTITIONS = 64;
    private static final int ROWS_PER_PARTITION = 8;
    // One id per table. CompressionMetadata holds one dictionary instance per id, and ZstdDictionaryCompressor
    // keys its compressors by id, so a shared id would make one test's references visible to another
    private static final long DICTIONARY_ID = 4242L;
    private static final long LIFECYCLE_DICTIONARY_ID = 4343L;

    static
    {
        CassandraTypesImplementation.setup(BridgeInitializationParameters.fromEnvironment());
    }

    @Test
    void testReadSSTableCompressedWithDictionary() throws IOException
    {
        try (TemporaryDirectory directory = new TemporaryDirectory())
        {
            CompressionDictionary dictionary = trainDictionary(DICTIONARY_ID);
            writeSSTable(directory.path(), DICTIONARY_TABLE, DICTIONARY_COMPRESSION, dictionary);

            // The reader decodes the dictionary that the writer appended, and hands it to the compressor
            try (CompressionMetadata metadata = openCompressionMetadata(directory.path()))
            {
                assertThat(metadata.dictionary()).isNotNull();
                assertThat(metadata.dictionary().dictId().kind).isEqualTo(CompressionDictionary.Kind.ZSTD);
                assertThat(metadata.dictionary().dictId().id).isEqualTo(DICTIONARY_ID);
                assertThat(metadata.dictionary().rawDictionary()).isEqualTo(dictionary.rawDictionary());
                assertThat(metadata.compressor()).isInstanceOf(ZstdDictionaryCompressor.class);
                // ZstdDictionaryCompressor caches one instance per compression level for the dictionary-less case,
                // and one instance per dictionary otherwise, so a compressor that carries a dictionary is never
                // the instance that the table options alone produce.
                assertThat(metadata.compressor()).isNotSameAs(ZstdDictionaryCompressor.create(new HashMap<>()));
            }

            // Every row decompresses, which is only possible with the dictionary attached
            assertThat(readRows(directory.path(), DICTIONARY_TABLE, DICTIONARY_COMPRESSION))
            .isEqualTo(PARTITIONS * ROWS_PER_PARTITION);
        }
    }

    @Test
    void testReadSSTableCompressedWithoutDictionary() throws IOException
    {
        try (TemporaryDirectory directory = new TemporaryDirectory())
        {
            writeSSTable(directory.path(), PLAIN_TABLE, PLAIN_COMPRESSION, null);

            // CompressionDictionary.deserialize returns null at end of file, which is both an SSTable that
            // holds no dictionary and every SSTable that an earlier Cassandra version wrote
            try (CompressionMetadata metadata = openCompressionMetadata(directory.path()))
            {
                assertThat(metadata.dictionary()).isNull();
                assertThat(metadata.compressor()).isNotInstanceOf(ZstdDictionaryCompressor.class);
            }

            assertThat(readRows(directory.path(), PLAIN_TABLE, PLAIN_COMPRESSION))
            .isEqualTo(PARTITIONS * ROWS_PER_PARTITION);
        }
    }

    /**
     * A node keeps a compression dictionary alive through {@code CompressionDictionaryManager}, which an offline
     * reader has none of. {@link CompressionMetadata} takes that part: it holds one instance per dictionary id,
     * owns the primary reference of each, and gives every reader a reference of its own. This test walks the
     * whole sequence, because a reference released too early makes the native zstd tables invalid mid-read, and
     * a reference never released makes {@code Ref} log LEAK DETECTED.
     */
    @Test
    void testDictionaryReferenceLifecycle() throws IOException
    {
        try (TemporaryDirectory directory = new TemporaryDirectory())
        {
            writeSSTable(directory.path(), LIFECYCLE_TABLE, DICTIONARY_COMPRESSION,
                         trainDictionary(LIFECYCLE_DICTIONARY_ID));

            CompressionMetadata first = openCompressionMetadata(directory.path());
            CompressionMetadata second = openCompressionMetadata(directory.path());
            // Two readers of one SSTable deserialize two dictionaries and share one instance
            assertThat(second.dictionary()).isSameAs(first.dictionary());

            CompressionDictionary dictionary = first.dictionary();
            Ref<? extends CompressionDictionary> readerRef = first.acquireDictionaryRef();
            assertThat(readerRef).isNotNull();

            first.close();
            // close() is idempotent, so a double release throws nothing
            first.close();
            second.close();

            // The primary reference outlives both metadata instances, so a later reader still gets a reference
            Ref<? extends CompressionDictionary> probe = dictionary.tryRef();
            assertThat(probe).isNotNull();
            probe.close();

            // The reader's reference outlives the primary one, so a removal mid-read frees nothing
            CompressionMetadata.evictDictionaries();
            probe = dictionary.tryRef();
            assertThat(probe).isNotNull();
            probe.close();

            // The last reference frees the native zstd tables, and tryRef reports the dictionary as released
            readerRef.close();
            assertThat(dictionary.tryRef()).isNull();
        }
    }

    private static String createStatement(String table, String compression)
    {
        return String.format("CREATE TABLE %s.%s (a int, b int, c text, PRIMARY KEY(a, b)) WITH compression = %s",
                             KEYSPACE, table, compression);
    }

    /**
     * Train a zstd dictionary over the values that {@link #writeSSTable} writes, then wrap it in the
     * Cassandra 6.0 dictionary that carries the kind, the id and the checksum through the CompressionInfo
     * component.
     */
    private static CompressionDictionary trainDictionary(long dictionaryId)
    {
        // ZstdDictTrainer needs at least 11 samples, and it caps the dictionary at the second argument
        ZstdDictTrainer trainer = new ZstdDictTrainer(1024 * 1024, 16 * 1024);
        for (int partition = 0; partition < PARTITIONS; partition++)
        {
            for (int row = 0; row < ROWS_PER_PARTITION; row++)
            {
                trainer.addSample(value(partition, row).getBytes(StandardCharsets.UTF_8));
            }
        }
        byte[] raw = trainer.trainSamples();
        assertThat(raw).isNotEmpty();
        CompressionDictionary.Kind kind = CompressionDictionary.Kind.ZSTD;
        int checksum = CompressionDictionary.calculateChecksum((byte) kind.ordinal(), dictionaryId, raw);
        return kind.createDictionary(new CompressionDictionary.DictId(kind, dictionaryId), raw, checksum);
    }

    private static void writeSSTable(Path directory,
                                     String table,
                                     String compression,
                                     @Nullable CompressionDictionary dictionary) throws IOException
    {
        CQLSSTableWriter.Builder builder =
        CQLSSTableWriter.builder()
                        .inDirectory(directory.toAbsolutePath().toString())
                        .forTable(createStatement(table, compression))
                        .using(String.format("INSERT INTO %s.%s (a, b, c) VALUES (?, ?, ?)", KEYSPACE, table))
                        .withPartitioner(Murmur3Partitioner.instance);
        if (dictionary != null)
        {
            builder.withCompressionDictionary(dictionary);
        }

        try (CQLSSTableWriter writer = builder.build())
        {
            for (int partition = 0; partition < PARTITIONS; partition++)
            {
                for (int row = 0; row < ROWS_PER_PARTITION; row++)
                {
                    writer.addRow(partition, row, value(partition, row));
                }
            }
        }
        catch (Exception exception)
        {
            throw new IOException("Failed to write the SSTable", exception);
        }

        // CQLSSTableWriter creates a keyspace directory when it builds the ColumnFamilyStore that owns the
        // dictionary, and removes it again, so the SSTable is the only content of the directory
        assertThat(TestSSTable.countIn(directory)).isEqualTo(1);
    }

    private static CompressionMetadata openCompressionMetadata(Path directory) throws IOException
    {
        Path compressionInfo = TestSSTable.firstIn(directory, FileType.COMPRESSION_INFO);
        try (InputStream in = new BufferedInputStream(Files.newInputStream(compressionInfo)))
        {
            return CompressionMetadata.fromInputStream(in, true, 1.0);
        }
    }

    /**
     * Read every row through the analytics {@code SSTableReader} and verify both the clustering value and the
     * text column, which is the value that the dictionary compresses.
     */
    private static int readRows(Path directory, String table, String compression) throws IOException
    {
        TableMetadata metadata = new SchemaBuilder(createStatement(table, compression),
                                                  KEYSPACE,
                                                  new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                        ImmutableMap.of("replication_factor", 1)),
                                                  Partitioner.Murmur3Partitioner).tableMetaData();
        SSTable ssTable = TestSSTable.firstIn(directory);
        SSTableReader reader = SSTableReader.builder(metadata, ssTable)
                                            .withReadIndexOffset(true)
                                            .withStats(Stats.DoNothingStats.INSTANCE)
                                            .build();

        Set<String> seen = new HashSet<>();
        try (ISSTableScanner scanner = reader.scanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    int a = partition.partitionKey().getKey().getInt(0);
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        assertThat(unfiltered.isRow()).isTrue();
                        Row row = (Row) unfiltered;
                        int b = row.clustering().bufferAt(0).getInt(0);
                        for (ColumnData data : row)
                        {
                            Cell<?> cell = (Cell<?>) data;
                            String c = StandardCharsets.UTF_8.decode(cell.buffer().duplicate()).toString();
                            assertThat(c).isEqualTo(value(a, b));
                            seen.add(a + ":" + b);
                        }
                    }
                }
            }
        }
        return seen.size();
    }

    /**
     * A value with enough repetition for a trained dictionary to be worth attaching, and enough variation for
     * every row to be distinguishable.
     */
    private static String value(int partition, int row)
    {
        StringBuilder builder = new StringBuilder();
        for (int repeat = 0; repeat < 16; repeat++)
        {
            builder.append("the quick brown fox jumps over the lazy dog ");
        }
        return builder.append(partition).append(':').append(row).toString();
    }
}
