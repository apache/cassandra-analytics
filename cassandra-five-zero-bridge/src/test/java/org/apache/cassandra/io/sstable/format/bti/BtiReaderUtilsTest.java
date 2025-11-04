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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.spark.reader.SSTableReaderTests.tableMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class BtiReaderUtilsTest
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    private static final int ROWS = 50;
    private static final int COLUMNS = 25;

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testPartitionIndexTokenRange(boolean compression)
    {
        // Only test BTI format for Cassandra 5.0+
        assumeThat(CassandraVersion.sstableFormat()).isEqualTo("bti");

        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable with BTI format
                    TestSchema schema = TestSchema.basic(BRIDGE, builder -> builder.withCompression(compression));
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

                    SSTable sstable = TestSSTable.firstIn(directory.path());
                    TableMetadata tableMetadata = tableMetadata(schema, partitioner);

                    // Test token range calculation
                    TokenRange tokenRange = ReaderUtils.tokenRangeFromIndex(tableMetadata, sstable);
                    assertThat(tokenRange).isNotNull();
                    assertThat(tokenRange.lowerEndpoint()).isNotNull();
                    assertThat(tokenRange.upperEndpoint()).isNotNull();
                    // Token range should be valid (upper >= lower for non-wrapping ranges)
                    assertThat(tokenRange.upperEndpoint().compareTo(tokenRange.lowerEndpoint())).isGreaterThanOrEqualTo(0);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testPrimaryIndexContainsAnyKey(boolean compression)
    {
        // Only test BTI format for Cassandra 5.0+
        assumeThat(CassandraVersion.sstableFormat()).isEqualTo("bti");

        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable with BTI format
                    TestSchema schema = TestSchema.basic(BRIDGE, builder -> builder.withCompression(compression));
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

                    SSTable sstable = TestSSTable.firstIn(directory.path());
                    TableMetadata tableMetadata = tableMetadata(schema, partitioner);
                    Descriptor descriptor = ReaderUtils.constructDescriptor(schema.keyspace, schema.table, sstable);

                    // Test with empty filters
                    boolean foundEmpty = BtiReaderUtils.primaryIndexContainsAnyKey(sstable, tableMetadata, descriptor, Collections.emptyList());
                    assertThat(foundEmpty).isFalse();

                    // Test with existing key
                    ByteBuffer existingKey = Int32Type.instance.fromString("19");
                    BigInteger token = BRIDGE.hash(partitioner, existingKey);
                    PartitionKeyFilter existingFilter = PartitionKeyFilter.create(existingKey, token);
                    boolean foundExisting = BtiReaderUtils.primaryIndexContainsAnyKey(sstable,
                                                                                      tableMetadata,
                                                                                      descriptor,
                                                                                      Collections.singletonList(existingFilter));
                    assertThat(foundExisting).isTrue();

                    // Test with non-existing key
                    ByteBuffer nonExistingKey = Int32Type.instance.fromString("99");
                    BigInteger nonExistingToken = BRIDGE.hash(partitioner, nonExistingKey);
                    PartitionKeyFilter nonExistingFilter = PartitionKeyFilter.create(nonExistingKey, nonExistingToken);
                    boolean foundNonExisting = BtiReaderUtils.primaryIndexContainsAnyKey(sstable,
                                                                                         tableMetadata,
                                                                                         descriptor,
                                                                                         Collections.singletonList(nonExistingFilter));
                    assertThat(foundNonExisting).isFalse();
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testReadPrimaryIndex(boolean compression)
    {
        // Only test BTI format for Cassandra 5.0+
        assumeThat(CassandraVersion.sstableFormat()).isEqualTo("bti");

        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable with BTI format
                    TestSchema schema = TestSchema.basic(BRIDGE, builder -> builder.withCompression(compression));
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

                    SSTable sstable = TestSSTable.firstIn(directory.path());
                    Descriptor descriptor = ReaderUtils.constructDescriptor(schema.keyspace, schema.table, sstable);

                    // Test reading primary index
                    AtomicInteger keyCount = new AtomicInteger(0);
                    AtomicBoolean foundTarget = new AtomicBoolean(false);
                    BtiReaderUtils.readPrimaryIndex(sstable, BRIDGE.getPartitioner(partitioner), descriptor, 1.0, key -> {
                        keyCount.incrementAndGet();
                        // Look for a specific key we know exists
                        if (Int32Type.instance.getString(key).equals("19"))
                        {
                            foundTarget.set(true);
                        }
                        return false; // Continue iteration
                    });

                    assertThat(keyCount.get()).isGreaterThan(0);
                    assertThat(foundTarget.get()).isTrue();

                    // Test early exit
                    AtomicInteger earlyExitCount = new AtomicInteger(0);
                    BtiReaderUtils.readPrimaryIndex(sstable, BRIDGE.getPartitioner(partitioner), descriptor, 1.0, key -> {
                        earlyExitCount.incrementAndGet();
                        return true; // Early exit on first key
                    });

                    assertThat(earlyExitCount.get()).isEqualTo(1);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testConsumePrimaryIndex(boolean compression)
    {
        // Only test BTI format for Cassandra 5.0+
        assumeThat(CassandraVersion.sstableFormat()).isEqualTo("bti");

        qt().forAll(arbitrary().enumValues(Partitioner.class))
            .checkAssert(partitioner -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    // Write an SSTable with BTI format
                    TestSchema schema = TestSchema.basic(BRIDGE, builder -> builder.withCompression(compression));
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

                    SSTable sstable = TestSSTable.firstIn(directory.path());
                    TableMetadata tableMetadata = tableMetadata(schema, partitioner);
                    Descriptor descriptor = ReaderUtils.constructDescriptor(schema.keyspace, schema.table, sstable);

                    // Test consuming index entries
                    AtomicInteger entryCount = new AtomicInteger(0);
                    IndexConsumer consumer = new IndexConsumer()
                    {
                        @Override
                        public void accept(org.apache.cassandra.spark.reader.IndexEntry entry)
                        {
                            entryCount.incrementAndGet();
                            assertThat(entry.getPartitionKey()).isNotNull();
                            assertThat(entry.getToken()).isNotNull();
                            assertThat(entry.getUncompressed()).isGreaterThanOrEqualTo(0);
                            assertThat(entry.getCompressed()).isGreaterThanOrEqualTo(0);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            // No additional action needed for this test
                        }

                        @Override
                        public void onFinished(long runtimeNanos)
                        {
                            // No additional action needed for this test
                        }
                    };

                    BtiReaderUtils.consumePrimaryIndex(sstable, tableMetadata, descriptor, null, consumer);
                    assertThat(entryCount.get()).isGreaterThan(0);
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
