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
import java.io.File;
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

import org.junit.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
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
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    String dataFile = TestSSTable.firstIn(directory.path()).getDataFileName();
                    Descriptor descriptor = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile));
                    Path statsFile = TestSSTable.firstIn(directory.path(), FileType.STATISTICS);

                    // Deserialize stats meta data and verify components match expected values
                    Map<MetadataType, MetadataComponent> componentMap;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(statsFile)))
                    {
                        componentMap = ReaderUtils.deserializeStatsMetadata(in, EnumSet.allOf(MetadataType.class), descriptor);
                    }
                    assertNotNull(componentMap);
                    assertFalse(componentMap.isEmpty());

                    ValidationMetadata validationMetadata = (ValidationMetadata) componentMap.get(MetadataType.VALIDATION);
                    assertEquals("org.apache.cassandra.dht." + partitioner.name(), validationMetadata.partitioner);

                    CompactionMetadata compactionMetadata = (CompactionMetadata) componentMap.get(MetadataType.COMPACTION);
                    assertNotNull(compactionMetadata);

                    StatsMetadata statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);
                    assertEquals(ROWS * COLUMNS, statsMetadata.totalRows);
                    assertEquals(0L, statsMetadata.repairedAt);
                    // Want to avoid test flakiness but timestamps should be in same ballpark
                    long tolerance = TimeUnit.MICROSECONDS.convert(10, TimeUnit.SECONDS);
                    assertTrue(Math.abs(statsMetadata.maxTimestamp - nowMicros) < tolerance);
                    assertTrue(Math.abs(statsMetadata.minTimestamp - nowMicros) < tolerance);

                    SerializationHeader.Component header = (SerializationHeader.Component) componentMap.get(MetadataType.HEADER);
                    assertNotNull(header);
                    assertEquals("org.apache.cassandra.db.marshal.Int32Type", header.getKeyType().toString());
                    List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                    assertEquals(1, clusteringTypes.size());
                    assertEquals("org.apache.cassandra.db.marshal.Int32Type", clusteringTypes.get(0).toString());
                    assertTrue(header.getStaticColumns().isEmpty());
                    List<AbstractType<?>> regulars = new ArrayList<>(header.getRegularColumns().values());
                    assertEquals(1, regulars.size());
                    assertEquals("org.apache.cassandra.db.marshal.Int32Type", regulars.get(0).toString());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @Test
    public void testReadFirstLastPartitionKey()
    {
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    // Read Summary.db file for first and last partition keys from Summary.db
                    Path summaryFile = TestSSTable.firstIn(directory.path(), FileType.SUMMARY);
                    SummaryDbUtils.Summary summaryKeys;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
                    {
                        summaryKeys = SummaryDbUtils.readSummary(in, Murmur3Partitioner.instance, 128, 2048);
                    }
                    assertNotNull(summaryKeys);
                    assertNotNull(summaryKeys.first());
                    assertNotNull(summaryKeys.last());

                    // Read Primary Index.db file for first and last partition keys from Summary.db
                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    Pair<DecoratedKey, DecoratedKey> indexKeys;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(indexFile)))
                    {
                        Pair<ByteBuffer, ByteBuffer> keys = ReaderUtils.readPrimaryIndex(in, true, Collections.emptyList());
                        indexKeys = Pair.create(Murmur3Partitioner.instance.decorateKey(keys.left),
                                                Murmur3Partitioner.instance.decorateKey(keys.right));
                    }
                    assertNotNull(indexKeys);
                    assertEquals(indexKeys.left, summaryKeys.first());
                    assertEquals(indexKeys.right, summaryKeys.last());
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    ByteBuffer key1 = Int32Type.instance.fromString("1");
                    BigInteger token1 = BRIDGE.hash(partitioner, key1);
                    PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);

                    // Read Filter.db file
                    Path filterFile = TestSSTable.firstIn(directory.path(), FileType.FILTER);
                    Descriptor descriptor = Descriptor.fromFilename(filterFile.toFile());
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
                        assertEquals(1, filters.size());
                        assertEquals(keyInSSTable, filters.get(0));
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        SSTable ssTable = mock(SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertFalse(ReaderUtils.anyFilterKeyInIndex(ssTable, Collections.emptyList()));
                    }
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    ByteBuffer key = Int32Type.instance.fromString("51");
                    BigInteger token = BRIDGE.hash(partitioner, key);
                    PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key, token);

                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        SSTable ssTable = mock(SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertFalse(ReaderUtils.anyFilterKeyInIndex(ssTable, Collections.singletonList(keyNotInSSTable)));
                    }
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
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    ByteBuffer key = Int32Type.instance.fromString("19");
                    BigInteger token = BRIDGE.hash(partitioner, key);
                    PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key, token);

                    Path indexFile = TestSSTable.firstIn(directory.path(), FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        SSTable ssTable = mock(SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertTrue(ReaderUtils.anyFilterKeyInIndex(ssTable, Collections.singletonList(keyInSSTable)));
                    }
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
