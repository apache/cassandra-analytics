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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.utils.BloomFilter;

import static org.apache.cassandra.spark.TestUtils.SSTABLE_FORMATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class SSTableCacheTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    // CHECKSTYLE IGNORE: Long method
    @Test
    public void testCache()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), arbitrary().pick(SSTABLE_FORMATS))
            .checkAssert((partitioner, format) -> {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable
                    TestSchema schema = TestSchema.basic(BRIDGE);
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer ->
                            IntStream.range(0, 10).forEach(index -> writer.write(index, 0, index)));
                    schema.writeSSTable(directory, BRIDGE, partitioner, writer ->
                            IntStream.range(20, 100).forEach(index -> writer.write(index, 1, index)));
                    List<SSTable> ssTables = TestSSTable.allIn(directory.path());
                    String dataFile0 = ssTables.get(0).getDataFileName();
                    String dataFile1 = ssTables.get(1).getDataFileName();
                    TableMetadata metadata = new SchemaBuilder(schema.createStatement,
                                                               schema.keyspace,
                                                               new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                                     ImmutableMap.of("replication_factor", 1)),
                                                               partitioner).tableMetaData();
                    SSTable ssTable0 = ssTables.get(0);
                    assertFalse(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));

                    SummaryDbUtils.Summary key1 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable0);
                    if (ssTable0.isBigFormat())
                    {
                        assertNotNull(key1);
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                        assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable0));
                        assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
                        assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));
                        assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));
                    }

                    Pair<DecoratedKey, DecoratedKey> key2 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable0);
                    if (ssTable0.isBigFormat())
                    {
                        assertEquals(key1.first(), key2.left);
                        assertEquals(key1.last(), key2.right);
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    }
                    else
                    {
                        assertNotNull(key2);
                    }
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));

                    Descriptor descriptor0 = Descriptor.fromFile(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile0));
                    Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0);
                    assertNotNull(componentMap);
                    if (ssTable0.isBigFormat())
                    {
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    }
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));
                    assertEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0));

                    BloomFilter filter = SSTableCache.INSTANCE.bloomFilter(ssTable0, descriptor0);
                    if (ssTable0.isBigFormat())
                    {
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    }
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));
                    assertTrue(filter.isPresent(key2.left));
                    assertTrue(filter.isPresent(key2.right));

                    CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(ssTable0,
                                                                                                        descriptor0.version.hasMaxCompressedLength(),
                                                                                                        metadata.params.crcCheckChance);
                    assertNotNull(compressionMetadata);
                    if (ssTable0.isBigFormat())
                    {
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    }
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0));

                    SSTable ssTable1 = ssTables.get(1);
                    Descriptor descriptor1 = Descriptor.fromFile(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile1));
                    if (ssTable1.isBigFormat())
                    {
                        assertFalse(SSTableCache.INSTANCE.containsSummary(ssTable1));
                    }
                    assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable1));
                    if (ssTable1.isBigFormat())
                    {
                        SummaryDbUtils.Summary key3 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1);
                        assertNotEquals(key1.first(), key3.first());
                        assertNotEquals(key1.last(), key3.last());
                        assertEquals(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).first(),
                                     SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).left);
                        assertEquals(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).last(),
                                     SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).right);
                    }
                    Pair<DecoratedKey, DecoratedKey> key4 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertNotEquals(key2.left, key4.left);
                    assertNotEquals(key2.right, key4.right);
                    assertNotEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable1, descriptor1));
                    Pair<DecoratedKey, DecoratedKey> key5 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertTrue(SSTableCache.INSTANCE.bloomFilter(ssTable1, descriptor1).isPresent(key5.left));
                    if (ssTable1.isBigFormat())
                    {
                        assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable1));
                    }
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable1));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable1));
                    assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable1));
                    SSTableCache.INSTANCE.compressionMetadata(ssTable1, descriptor1.version.hasMaxCompressedLength(), metadata.params.crcCheckChance);
                    assertTrue(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable1));
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
