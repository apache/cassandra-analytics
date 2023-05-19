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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class SSTableCacheTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testCache()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
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

                    SummaryDbUtils.Summary key1 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable0);
                    assertNotNull(key1);
                    assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));

                    Pair<DecoratedKey, DecoratedKey> key2 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable0);
                    assertEquals(key1.first(), key2.left);
                    assertEquals(key1.last(), key2.right);
                    assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));

                    Descriptor descriptor0 = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile0));
                    Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0);
                    assertNotNull(componentMap);
                    assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0));

                    BloomFilter filter = SSTableCache.INSTANCE.bloomFilter(ssTable0, descriptor0);
                    assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
                    assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable0));
                    assertTrue(filter.isPresent(key1.first()));
                    assertTrue(filter.isPresent(key1.last()));

                    SSTable ssTable1 = ssTables.get(1);
                    Descriptor descriptor1 = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile1));
                    assertFalse(SSTableCache.INSTANCE.containsSummary(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsStats(ssTable1));
                    assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable1));
                    SummaryDbUtils.Summary key3 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1);
                    assertNotEquals(key1.first(), key3.first());
                    assertNotEquals(key1.last(), key3.last());
                    Pair<DecoratedKey, DecoratedKey> key4 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertNotEquals(key1.first(), key4.left);
                    assertNotEquals(key1.last(), key4.right);
                    assertEquals(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).first(),
                                 SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).left);
                    assertEquals(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).last(),
                                 SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).right);
                    assertNotEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable1, descriptor1));
                    Pair<DecoratedKey, DecoratedKey> key5 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertTrue(SSTableCache.INSTANCE.bloomFilter(ssTable1, descriptor1).isPresent(key5.left));
                    assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable1));
                    assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable1));
                    assertTrue(SSTableCache.INSTANCE.containsStats(ssTable1));
                    assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable1));
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
