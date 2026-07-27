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
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.utils.BloomFilter;

import static org.assertj.core.api.Assertions.assertThat;
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
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isFalse();

                    IndexSummaryComponent key1 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable0);
                    assertThat(key1).isNotNull();
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isFalse();

                    Pair<DecoratedKey, DecoratedKey> key2 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable0);
                    assertThat(key2.left).isEqualTo(key1.first());
                    assertThat(key2.right).isEqualTo(key1.last());
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isFalse();

                    Descriptor descriptor0 = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile0));
                    Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0);
                    assertThat(componentMap).isNotNull();
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isFalse();
                    assertThat(SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0)).isEqualTo(componentMap);

                    BloomFilter filter = SSTableCache.INSTANCE.bloomFilter(ssTable0, descriptor0);
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isFalse();
                    assertThat(filter.isPresent(key1.first())).isTrue();
                    assertThat(filter.isPresent(key1.last())).isTrue();

                    CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(ssTable0,
                                                                                                        descriptor0.version.hasMaxCompressedLength(),
                                                                                                        metadata.params.crcCheckChance);
                    assertThat(compressionMetadata).isNotNull();
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable0)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable0)).isTrue();

                    SSTable ssTable1 = ssTables.get(1);
                    Descriptor descriptor1 = Descriptor.fromFilename(
                            new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile1));
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable1)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable1)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable1)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable1)).isFalse();
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable1)).isFalse();
                    IndexSummaryComponent key3 = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1);
                    assertThat(key3.first()).isNotEqualTo(key1.first());
                    assertThat(key3.last()).isNotEqualTo(key1.last());
                    Pair<DecoratedKey, DecoratedKey> key4 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertThat(key4.left).isNotEqualTo(key1.first());
                    assertThat(key4.right).isNotEqualTo(key1.last());
                    assertThat(SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).left)
                                 .isEqualTo(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).first());
                    assertThat(SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1).right)
                                 .isEqualTo(SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable1).last());
                    assertThat(SSTableCache.INSTANCE.componentMapFromStats(ssTable1, descriptor1)).isNotEqualTo(componentMap);
                    Pair<DecoratedKey, DecoratedKey> key5 = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable1);
                    assertThat(SSTableCache.INSTANCE.bloomFilter(ssTable1, descriptor1).isPresent(key5.left)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsSummary(ssTable1)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsIndex(ssTable1)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsStats(ssTable1)).isTrue();
                    assertThat(SSTableCache.INSTANCE.containsFilter(ssTable1)).isTrue();
                    SSTableCache.INSTANCE.compressionMetadata(ssTable1, descriptor1.version.hasMaxCompressedLength(), metadata.params.crcCheckChance);
                    assertThat(SSTableCache.INSTANCE.containsCompressionMetadata(ssTable1)).isTrue();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
