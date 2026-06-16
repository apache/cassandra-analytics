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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;

public class IndexOffsetTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexOffsetTests.class);
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    @SuppressWarnings("unchecked")
    private static final Multimap<Partitioner, TokenRange> RANGES =
    new ImmutableMultimap.Builder<Partitioner, TokenRange>()
    .putAll(Partitioner.RandomPartitioner,
            TokenRange.openClosed(BigInteger.ZERO,
                                  BigInteger.ONE),
            TokenRange.openClosed(BigInteger.ONE,
                                  new BigInteger("56713727820156410577229101238628035242")),
            TokenRange.openClosed(new BigInteger("56713727820156410577229101238628035243"),
                                  new BigInteger("113427455640312821154458202477256070484")),
            TokenRange.openClosed(new BigInteger("113427455640312821154458202477256070485"),
                                  new BigInteger("170141183460469231731687303715884105727")))
    .putAll(Partitioner.Murmur3Partitioner,
            TokenRange.openClosed(new BigInteger("-9223372036854775808"),
                                  new BigInteger("-9223372036854775807")),
            TokenRange.openClosed(new BigInteger("-9223372036854775807"),
                                  new BigInteger("-3074457345618258603")),
            TokenRange.openClosed(new BigInteger("-3074457345618258602"),
                                  new BigInteger("3074457345618258602")),
            TokenRange.openClosed(new BigInteger("3074457345618258603"),
                                  new BigInteger("9223372036854775807")))
    .build();

    @SuppressWarnings("static-access")
    @ParameterizedTest
    @MethodSource("partitionSizeProvider")
    public void testReadIndexOffsets(int numPartitions, int numRowsPerPartition)
    {
        qt().forAll(arbitrary().enumValues(Partitioner.class), booleans().all())
            .checkAssert((partitioner, enableCompression) -> {
                try (TemporaryDirectory directory = new TemporaryDirectory())
                {
                    int numKeys = numPartitions * numRowsPerPartition;
                    TestSchema schema = TestSchema.basicBuilder(BRIDGE)
                                                  .withCompression(enableCompression)
                                                  .build();

                    schema.writeSSTable(directory, BRIDGE, partitioner, writer -> {
                        for (int pk = 0; pk < numPartitions; pk++)
                        {
                            for (int ck = 0; ck < numRowsPerPartition; ck++)
                            {
                                writer.write(pk, ck, pk);
                            }
                        }
                    });
                    assertThat(TestSSTable.countIn(directory.path())).isEqualTo(1);

                    TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
                    assertThat(metadata).as("Could not find table metadata").isNotNull();

                    SSTable ssTable = TestSSTable.firstIn(directory.path());
                    assertThat(ssTable).as("Could not find SSTable").isNotNull();

                    Collection<TokenRange> ranges = RANGES.get(partitioner);
                    assertThat(ranges).as("Unknown paritioner").isNotNull();

                    LOGGER.info("Testing index offsets numKeys={} sparkPartitions={} partitioner={} enableCompression={}",
                                numKeys, ranges.size(), partitioner.name(), enableCompression);

                    AtomicInteger skippedPartitions = new AtomicInteger(0);
                    AtomicLong skippedDataOffsets = new AtomicLong(0);
                    int[][] counts = new int[numPartitions][numRowsPerPartition];
                    for (TokenRange range : ranges)
                    {
                        SSTableReader reader = SSTableReader.builder(metadata, ssTable)
                                                            .withSparkRangeFilter(SparkRangeFilter.create(range))
                                                            .withStats(new Stats()
                                                            {
                                                                public void skippedPartition(ByteBuffer key, BigInteger token)
                                                                {
                                                                    skippedPartitions.addAndGet(1);
                                                                }

                                                                public void skippedDataDbStartOffset(long length)
                                                                {
                                                                    skippedDataOffsets.addAndGet(length);
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
                                int pk = rowIterator.partitionKey().getKey().getInt();
                                while (rowIterator.hasNext())
                                {
                                    Unfiltered unfiltered = rowIterator.next();
                                    int ck = unfiltered.clustering().bufferAt(0).asIntBuffer().get();
                                    // Count how many times we read a key across all 'spark' token partitions
                                    counts[pk][ck]++;
                                }
                            }
                        }
                    }

                    // Verify we read each key exactly once across all Spark partitions
                    assertThat(counts.length).isEqualTo(numPartitions);
                    for (int partitionNum = 0; partitionNum < counts.length; partitionNum++)
                    {
                        for (int rowNumInPartition = 0; rowNumInPartition < counts[partitionNum].length; rowNumInPartition++)
                        {
                            String key = partitionNum + "/" + rowNumInPartition;
                            int count = counts[partitionNum][rowNumInPartition];
                            if (count == 0)
                            {
                                LOGGER.error("Missing key key={} token={} partitioner={}",
                                             key,
                                             toToken(partitioner, partitionNum),
                                             partitioner.name());
                            }
                            else if (count > 1)
                            {
                                LOGGER.error("Key read by more than 1 Spark partition key={} token={} partitioner={}",
                                             key,
                                             toToken(partitioner, partitionNum),
                                             partitioner.name());
                            }
                            assertThat(count).as(count > 0 ? "Key " + key + " read " + count + " times"
                                                           : "Key not found: " + key).isEqualTo(1);
                        }
                    }

                    assertThat(skippedDataOffsets.longValue()).isGreaterThan(0);

                    LOGGER.info("Success skippedKeys={} partitioner={}",
                                skippedPartitions.intValue(), partitioner.name());
                }
                catch (IOException exception)
                {
                    throw new RuntimeException(exception);
                }
            });
    }

    static Stream<Arguments> partitionSizeProvider()
    {
        return Stream.of(
        Arguments.of(100000, 1),
        Arguments.of(1000, 100),
        Arguments.of(100, 1000)
        );
    }

    private BigInteger toToken(Partitioner partitioner, int index)
    {
        // Cast to ByteBuffer required when compiling with Java 8
        return ReaderUtils.tokenToBigInteger(BRIDGE
                                             .getPartitioner(partitioner)
                                             .decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(index).flip())
                                             .getToken());
    }
}
