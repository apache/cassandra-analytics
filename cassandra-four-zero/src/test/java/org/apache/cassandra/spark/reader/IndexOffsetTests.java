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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;

public class IndexOffsetTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexOffsetTests.class);
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    @SuppressWarnings("unchecked")
    private static final Multimap<Partitioner, TokenRange> RANGES = new ImmutableMultimap.Builder<Partitioner, TokenRange>()
        .putAll(Partitioner.RandomPartitioner,  TokenRange.closedOpen(BigInteger.ZERO,
                                                                      BigInteger.ZERO),
                                                TokenRange.closedOpen(BigInteger.ONE,
                                                                      new BigInteger("56713727820156410577229101238628035242")),
                                                TokenRange.closedOpen(new BigInteger("56713727820156410577229101238628035243"),
                                                                      new BigInteger("113427455640312821154458202477256070484")),
                                                TokenRange.closedOpen(new BigInteger("113427455640312821154458202477256070485"),
                                                                      new BigInteger("170141183460469231731687303715884105727")))
        .putAll(Partitioner.Murmur3Partitioner, TokenRange.closed(new BigInteger("-9223372036854775808"),
                                                                  new BigInteger("-9223372036854775808")),
                                                TokenRange.closed(new BigInteger("-9223372036854775807"),
                                                                  new BigInteger("-3074457345618258603")),
                                                TokenRange.closed(new BigInteger("-3074457345618258602"),
                                                                  new BigInteger("3074457345618258602")),
                                                TokenRange.closed(new BigInteger("3074457345618258603"),
                                                                  new BigInteger("9223372036854775807")))
        .build();

    @Test
    @SuppressWarnings("static-access")
    public void testReadIndexOffsets()
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), booleans().all())
            .checkAssert((partitioner, enableCompression) -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    int numKeys = 100000;
                    TestSchema schema = TestSchema.basicBuilder(BRIDGE)
                                                  .withCompression(enableCompression)
                                                  .build();

                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int index = 0; index < numKeys; index++)
                        {
                            writer.write(index, 0, index);
                        }
                    });
                    assertEquals(1, TestSSTable.countIn(directory.path()));

                    TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
                    assertNotNull("Could not find table metadata", metadata);

                    SSTable ssTable = TestSSTable.firstIn(directory.path());
                    assertNotNull("Could not find SSTable", ssTable);

                    Collection<TokenRange> ranges = RANGES.get(partitioner);
                    assertNotNull("Unknown paritioner", ranges);

                    LOGGER.info("Testing index offsets numKeys={} sparkPartitions={} partitioner={} enableCompression={}",
                                numKeys, ranges.size(), partitioner.name(), enableCompression);

                    MutableInt skipped = new MutableInt(0);
                    int[] counts = new int[numKeys];
                    for (TokenRange range : ranges)
                    {
                        SSTableReader reader = SSTableReader.builder(metadata, ssTable)
                                                            .withSparkRangeFilter(SparkRangeFilter.create(range))
                                                            .withStats(new Stats()
                                                            {
                                                                public void skippedPartition(ByteBuffer key, BigInteger token)
                                                                {
                                                                    skipped.add(1);
                                                                }
                                                            })
                                                            .build();
                        if (reader.ignore())
                        {
                            // We can skip this range entirely, it doesn't overlap with SSTable
                            continue;
                        }

                        // Iterate through SSTable partitions,
                        // each scanner should only read tokens within its own token range
                        try (ISSTableScanner scanner = reader.scanner())
                        {
                            while (scanner.hasNext())
                            {
                                UnfilteredRowIterator rowIterator = scanner.next();
                                int key = rowIterator.partitionKey().getKey().getInt();
                                // Count how many times we read a key across all 'spark' token partitions
                                counts[key]++;
                                while (rowIterator.hasNext())
                                {
                                    rowIterator.next();
                                }
                            }
                        }
                    }

                    // Verify we read each key exactly once across all Spark partitions
                    assertEquals(counts.length, numKeys);
                    int index = 0;
                    for (int count : counts)
                    {
                        if (count == 0)
                        {
                            LOGGER.error("Missing key key={} token={} partitioner={}",
                                         index,
                                         // Cast to ByteBuffer required when compiling with Java 8
                                         ReaderUtils.tokenToBigInteger(BRIDGE
                                                .getPartitioner(partitioner)
                                                .decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(index).flip())
                                                .getToken()),
                                         partitioner.name());
                        }
                        else if (count > 1)
                        {
                            LOGGER.error("Key read by more than 1 Spark partition key={} token={} partitioner={}",
                                         index,
                                         // Cast to ByteBuffer required when compiling with Java 8
                                         ReaderUtils.tokenToBigInteger(BRIDGE
                                                .getPartitioner(partitioner)
                                                .decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(index).flip())
                                                .getToken()),
                                         partitioner.name());
                        }
                        assertEquals(count > 0 ? "Key " + index + " read " + count + " times"
                                               : "Key not found: " + index, 1, count);
                        index++;
                    }

                    LOGGER.info("Success skippedKeys={} partitioner={}",
                                skipped.intValue(), partitioner.name());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }
}
