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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.spark.TestUtils.BIG_FORMAT;
import static org.apache.cassandra.spark.TestUtils.BTI_FORMAT;
import static org.apache.cassandra.spark.TestUtils.SSTABLE_FORMATS;
import static org.apache.cassandra.spark.reader.SSTableReaderTests.tableMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class ReaderUtilsTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    private static final int ROWS = 50;
    private static final int COLUMNS = 25;

    @Test
    public void testReadStatsMetaData()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    long nowMicros = System.currentTimeMillis() * 1000;
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    String dataFile = TestSSTable.firstIn(directory.path()).getDataFileName();
                    Descriptor descriptor = Descriptor.fromFile(
                    new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile));
                    Path statsFile = TestSSTable.firstIn(directory.path(), FileType.STATISTICS);

                    // Deserialize stats meta data and verify components match expected values
                    Map<MetadataType, MetadataComponent> componentMap;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(statsFile)))
                    {
                        componentMap = ReaderUtils.deserializeStatsMetadata(in, EnumSet.allOf(MetadataType.class), descriptor);
                    }
                    assertThat(componentMap).isNotNull();
                    assertThat(componentMap.isEmpty()).isFalse();

                    ValidationMetadata validationMetadata = (ValidationMetadata) componentMap.get(MetadataType.VALIDATION);
                    assertThat(validationMetadata.partitioner).isEqualTo("org.apache.cassandra.dht." + partitioner.name());

                    CompactionMetadata compactionMetadata = (CompactionMetadata) componentMap.get(MetadataType.COMPACTION);
                    assertThat(compactionMetadata).isNotNull();

                    StatsMetadata statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);
                    assertThat(statsMetadata.totalRows).isEqualTo(ROWS * COLUMNS);
                    assertThat(statsMetadata.repairedAt).isEqualTo(0L);
                    // Want to avoid test flakiness but timestamps should be in same ballpark
                    long tolerance = TimeUnit.MICROSECONDS.convert(10, TimeUnit.SECONDS);
                    assertThat(Math.abs(statsMetadata.maxTimestamp - nowMicros) < tolerance).isTrue();
                    assertThat(Math.abs(statsMetadata.minTimestamp - nowMicros) < tolerance).isTrue();

                    SerializationHeader.Component header = (SerializationHeader.Component) componentMap.get(MetadataType.HEADER);
                    assertThat(header).isNotNull();
                    assertThat(header.getKeyType().toString()).isEqualTo("org.apache.cassandra.db.marshal.Int32Type");
                    List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                    assertThat(clusteringTypes.size()).isEqualTo(1);
                    assertThat(clusteringTypes.get(0).toString()).isEqualTo("org.apache.cassandra.db.marshal.Int32Type");
                    assertThat(header.getStaticColumns().isEmpty()).isTrue();
                    List<AbstractType<?>> regulars = new ArrayList<>(header.getRegularColumns().values());
                    assertThat(regulars.size()).isEqualTo(1);
                    assertThat(regulars.get(0).toString()).isEqualTo("org.apache.cassandra.db.marshal.Int32Type");
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testReadFirstLastPartitionKeyBigFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(BIG_FORMAT);
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    // Read Summary.db file for first and last partition keys from Summary.db
                    Path summaryFile = TestSSTable.firstIn(directory.path(), FileType.SUMMARY);
                    SummaryDbUtils.Summary summaryKeys;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
                    {
                        summaryKeys = SummaryDbUtils.readSummary(in, Murmur3Partitioner.instance, 128, 2048);
                    }
                    assertThat(summaryKeys).isNotNull();
                    assertThat(summaryKeys.first()).isNotNull();
                    assertThat(summaryKeys.last()).isNotNull();

                    // Read Primary Index.db file for first and last partition keys from Summary.db
                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    Pair<DecoratedKey, DecoratedKey> indexKeys;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(indexFile)))
                    {
                        Pair<ByteBuffer, ByteBuffer> keys = ReaderUtils.primaryIndexReadFirstAndLastKey(in);
                        indexKeys = Pair.of(Murmur3Partitioner.instance.decorateKey(keys.left),
                                            Murmur3Partitioner.instance.decorateKey(keys.right));
                    }
                    assertThat(indexKeys).isNotNull();
                    assertThat(summaryKeys.first()).isEqualTo(indexKeys.left);
                    assertThat(summaryKeys.last()).isEqualTo(indexKeys.right);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testReadFirstLastPartitionKeyBtiFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(BTI_FORMAT);
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    // Read Partition Index file for first and last partition keys
                    SSTable ssTable = TestSSTable.firstIn(directory.path());
                    Pair<DecoratedKey, DecoratedKey> indexKeys = ReaderUtils.keysFromIndex(Murmur3Partitioner.instance, ssTable);
                    assertThat(indexKeys).isNotNull();
                    assertThat(indexKeys.left).isNotNull();
                    assertThat(indexKeys.right).isNotNull();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSearchInBloomFilter()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), arbitrary().pick(SSTABLE_FORMATS))
            .checkAssert((partitioner, format) -> {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    ByteBuffer key1 = Int32Type.instance.fromString("1");
                    BigInteger token1 = BRIDGE.hash(partitioner, key1);
                    PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);

                    // Read Filter.db file
                    Path filterFile = TestSSTable.firstIn(directory.path(), FileType.FILTER);
                    Descriptor descriptor = Descriptor.fromFileWithComponent(new File(filterFile.toFile()), false).left;
                    IPartitioner iPartitioner;
                    switch (partitioner)
                    {
                        case Murmur3Partitioner:
                            iPartitioner = Murmur3Partitioner.instance;
                            break;
                        case RandomPartitioner:
                            iPartitioner = RandomPartitioner.instance;
                            break;
                        default:
                            throw new RuntimeException("Unexpected partitioner: " + partitioner);
                    }

                    try (InputStream indexStream = new FileInputStream(filterFile.toString()))
                    {
                        SSTable ssTable = mock(SSTable.class);
                        when(ssTable.openFilterStream()).thenReturn(indexStream);
                        List<PartitionKeyFilter> filters = ReaderUtils.filterKeyInBloomFilter(ssTable,
                                                                                              iPartitioner,
                                                                                              descriptor,
                                                                                              Collections.singletonList(keyInSSTable));
                        assertThat(filters.size()).isEqualTo(1);
                        assertThat(filters.get(0)).isEqualTo(keyInSSTable);
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSearchInIndexEmptyFilters()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), arbitrary().pick(SSTABLE_FORMATS))
            .checkAssert((partitioner, format) -> {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                TestSchema schema = TestSchema.basic(BRIDGE);
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    Path dataFile = TestSSTable.firstIn(directory.path(), FileType.DATA);
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable ssTable = TestSSTable.at(dataFile);

                    Descriptor descriptor = ReaderUtils.constructDescriptor(metadata.keyspace, metadata.name, ssTable);

                    assertThat(ReaderUtils.anyFilterKeyInIndex(ssTable, metadata, descriptor, Collections.emptyList())).isFalse();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSearchInIndexKeyNotFound()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), arbitrary().pick(SSTABLE_FORMATS))
            .checkAssert((partitioner, format) -> {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    ByteBuffer key = Int32Type.instance.fromString("51");
                    BigInteger token = BRIDGE.hash(partitioner, key);
                    PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key, token);

                    Path dataFile = TestSSTable.firstIn(directory.path(), FileType.DATA);
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable ssTable = TestSSTable.at(dataFile);

                    Descriptor descriptor = ReaderUtils.constructDescriptor(metadata.keyspace, metadata.name, ssTable);

                    assertThat(ReaderUtils.anyFilterKeyInIndex(ssTable, metadata, descriptor, Collections.singletonList(keyNotInSSTable))).isFalse();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testSearchInIndexKeyFound()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), arbitrary().pick(SSTABLE_FORMATS))
            .checkAssert((partitioner, format) -> {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int row = 0; row < ROWS; row++)
                        {
                            for (int column = 0; column < COLUMNS; column++)
                            {
                                writer.write(row, column, row + column);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    ByteBuffer key = Int32Type.instance.fromString("19");
                    BigInteger token = BRIDGE.hash(partitioner, key);
                    PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key, token);

                    Path dataFile = TestSSTable.firstIn(directory.path(), FileType.DATA);
                    TableMetadata metadata = tableMetadata(schema, partitioner);
                    SSTable ssTable = TestSSTable.at(dataFile);

                    Descriptor descriptor = ReaderUtils.constructDescriptor(metadata.keyspace, metadata.name, ssTable);

                    assertThat(ReaderUtils.anyFilterKeyInIndex(ssTable, metadata, descriptor, Collections.singletonList(keyInSSTable))).isTrue();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
