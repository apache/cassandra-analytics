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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.bridge.BloomFilter;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.spark.Tester.DEFAULT_NUM_ROWS;
import static org.apache.cassandra.spark.utils.ByteBufferUtils.readShortLengthCompositeTypeString;
import static org.apache.cassandra.spark.utils.RandomUtils.randomAlphanumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class CassandraBridgeUtilTests
{
    @TempDir
    private static Path tempPath;

    @Test
    public void testLastRepairTime()
    {
        runTest((partitioner, bridge, schema, testDir) -> {
            writeSSTable(partitioner, bridge, schema, testDir,
                         (writer) -> IntStream.range(0, DEFAULT_NUM_ROWS)
                                              .forEach(i -> writer.write(randomAlphanumeric(), randomAlphanumeric(), randomAlphanumeric()))
            );

            try
            {
                assertEquals(0, bridge.lastRepairTime("ks", "tb", TestSSTable.firstIn(testDir)));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBuildPartitionKey()
    {
        runTest((partitioner, bridge, schema, testDir) -> {
            ByteBuffer pk1 = bridge.encodePartitionKey(partitioner, "ks", schema.createStatement, Collections.singletonList("a"));
            assertEquals("a", ByteBufferUtils.string(pk1));
            String str1 = randomAlphanumeric();
            ByteBuffer pk2 = bridge.encodePartitionKey(partitioner, "ks", schema.createStatement, Collections.singletonList(str1));
            assertEquals(str1, ByteBufferUtils.string(pk2));
            List<String> keys = Arrays.asList(randomAlphanumeric(), randomAlphanumeric(), randomAlphanumeric());

            TestSchema threePkSchema = TestSchema.builder(bridge)
                                                 .withPartitionKey("a", bridge.text())
                                                 .withPartitionKey("b", bridge.text())
                                                 .withPartitionKey("c", bridge.text())
                                                 .withColumn("d", bridge.bigint())
                                                 .withColumn("e", bridge.aInt())
                                                 .build();
            ByteBuffer pk3 = bridge.encodePartitionKey(partitioner, "ks", threePkSchema.createStatement, keys);
            assertEquals(keys.get(0), readShortLengthCompositeTypeString(pk3));
            assertEquals(keys.get(1), readShortLengthCompositeTypeString(pk3));
            assertEquals(keys.get(2), readShortLengthCompositeTypeString(pk3));
        });
    }

    @Test
    public void testOverlaps()
    {
        runTest((partitioner, bridge, schema, testDir) -> {
            // write SSTable
            List<String> keys = IntStream.range(0, 3).mapToObj(i -> randomAlphanumeric()).collect(Collectors.toList());
            List<ByteBuffer> buffers = bridge.encodePartitionKeys(
            partitioner,
            schema.keyspace,
            schema.createStatement,
            keys.stream()
                .map(Collections::singletonList)
                .collect(Collectors.toList())
            );
            List<BigInteger> sortedTokens = buffers
                                            .stream()
                                            .map(partitioner::hash)
                                            .sorted()
                                            .collect(Collectors.toList());
            assertTrue(TestSSTable.allIn(testDir).isEmpty());
            writeSSTable(partitioner, bridge, schema, testDir,
                         (writer) -> keys.forEach(key -> writer.write(key, randomAlphanumeric(), randomAlphanumeric())));
            assertEquals(1, TestSSTable.allIn(testDir).size());

            TestSSTable ssTable = (TestSSTable) TestSSTable.firstIn(testDir);
            List<TokenRange> testRanges = Arrays.asList(
            TokenRange.openClosed(sortedTokens.get(0), sortedTokens.get(0)),
            TokenRange.closed(sortedTokens.get(0), sortedTokens.get(1)),
            TokenRange.closed(sortedTokens.get(0), sortedTokens.get(2)),
            TokenRange.openClosed(sortedTokens.get(1), sortedTokens.get(1)),
            TokenRange.closed(sortedTokens.get(1), sortedTokens.get(2)),
            TokenRange.openClosed(sortedTokens.get(2), sortedTokens.get(2).add(BigInteger.ONE)) // out of range
            );

            Path summaryDbFile = ssTable.fileComponentPath(FileType.SUMMARY);
            for (int j = 0; j < 2; j++) // loop around after deleting Summary.db file to verify we can check using Index.db file
            {
                SSTableSummary summary = bridge.getSSTableSummary(partitioner, ssTable, 128, 256);
                assertEquals(sortedTokens.get(0), summary.firstToken);
                assertEquals(sortedTokens.get(2), summary.lastToken);
                TokenRange sstableRange = TokenRange.closed(summary.firstToken, summary.lastToken);
                for (BigInteger token : sortedTokens)
                {
                    assertTrue(sstableRange.contains(token));
                }
                List<Boolean> result = bridge.overlaps(ssTable, partitioner, 128, 256, testRanges);

                assertEquals(testRanges.size(), result.size());
                for (int i = 0; i < result.size(); i++)
                {
                    assertEquals(i < result.size() - 1, result.get(i));
                }

                // delete Summary.db file and check we can read the Index.db file too
                if (summaryDbFile != null)
                {
                    Files.deleteIfExists(summaryDbFile);
                }
            }
        });
    }

    @Test
    public void testContains()
    {
        runTest((partitioner, bridge, schema, testDir) -> {
            // write SSTable
            Set<String> keys = IntStream.range(0, 25).mapToObj(i -> randomAlphanumeric()).collect(Collectors.toSet());
            List<ByteBuffer> buffers = bridge.encodePartitionKeys(
            partitioner,
            schema.keyspace,
            schema.createStatement,
            keys.stream()
                .map(Collections::singletonList)
                .collect(Collectors.toList())
            );
            assertTrue(TestSSTable.allIn(testDir).isEmpty());
            writeSSTable(partitioner, bridge, schema, testDir,
                         (writer) -> keys.forEach(key -> writer.write(key, randomAlphanumeric(), randomAlphanumeric())));
            assertEquals(1, TestSSTable.allIn(testDir).size());

            TestSSTable ssTable = (TestSSTable) TestSSTable.firstIn(testDir);

            // should return all positives for the keys contained in the SSTable
            BloomFilter filter = bridge.openBloomFilter(partitioner, schema.keyspace, schema.table, ssTable);
            List<Boolean> result = buffers.stream().map(filter::mightContain).collect(Collectors.toList());
            assertEquals(result.size(), buffers.size());
            assertTrue(result.stream().allMatch(boolValue -> boolValue));

            // random keys should return some negatives for keys not contained in the SSTable
            List<String> otherKeys = IntStream.range(0, DEFAULT_NUM_ROWS).mapToObj(i -> randomAlphanumeric(keys)).collect(Collectors.toList());
            List<ByteBuffer> randomBuffers = bridge.encodePartitionKeys(
            partitioner,
            schema.keyspace,
            schema.createStatement,
            otherKeys.stream()
                     .map(Collections::singletonList)
                     .collect(Collectors.toList())
            );
            BloomFilter randomBloomFilter = bridge.openBloomFilter(partitioner, schema.keyspace, schema.table, ssTable);
            List<Boolean> randomResult = randomBuffers.stream().map(randomBloomFilter::mightContain).collect(Collectors.toList());
            assertEquals(randomResult.size(), otherKeys.size());
            assertTrue(randomResult.stream().anyMatch(boolValue -> !boolValue));

            // perform exact contains query to confirm expected keys exist and random keys don't
            assertTrue(bridge.contains(partitioner, schema.keyspace, schema.table, ssTable, buffers).stream().allMatch(aBoolean -> aBoolean));
            assertTrue(bridge.contains(partitioner, schema.keyspace, schema.table, ssTable, randomBuffers).stream().noneMatch(aBoolean -> aBoolean));
            List<ByteBuffer> allKeys = new ArrayList<>();
            allKeys.addAll(buffers);
            allKeys.addAll(randomBuffers);
            List<Boolean> exactResult = bridge.contains(partitioner, schema.keyspace, schema.table, ssTable, allKeys);
            assertEquals(allKeys.size(), exactResult.size());
            for (int i = 0; i < exactResult.size(); i++)
            {
                boolean contains = exactResult.get(i);
                assertEquals(i < buffers.size(), contains);
                if (filter.doesNotContain(allKeys.get(i)))
                {
                    // if bloom filter returns true for `doesNotContain` then contains should always be false
                    assertFalse(contains);
                }
                if (contains)
                {
                    // bloom filter should always return true if SSTable contains key
                    assertTrue(filter.mightContain(allKeys.get(i)));
                }
            }
        });
    }

    @Test
    public void testReadKeys()
    {
        runTest(
        (partitioner, bridge, schema, testDir) -> {
            Map<String, String> expected = IntStream.range(0, DEFAULT_NUM_ROWS)
                                                    .mapToObj(i -> randomAlphanumeric(32))
                                                    .collect(Collectors.toMap(Function.identity(), i -> randomAlphanumeric(32)));
            writeSSTable(partitioner, bridge, schema, testDir, (writer) -> expected.forEach((key, value) -> writer.write(key, value, value)));

            List<SSTable> all = TestSSTable.allIn(testDir);
            Set<SSTable> ssTables = new HashSet<>(all);
            Map<String, Map<String, Object>> actual = new HashMap<>(DEFAULT_NUM_ROWS);
            bridge.readStringPartitionKeys(partitioner,
                                           schema.keyspace,
                                           schema.createStatement,
                                           ssTables,
                                           null,
                                           expected.keySet().stream().map(Collections::singletonList).collect(Collectors.toList()),
                                           null,
                                           (row) -> actual.put(row.get("a").toString(), row)
            );
            assertEquals(expected.size(), actual.size());
            for (Map.Entry<String, String> entry : expected.entrySet())
            {
                Map<String, Object> actualRow = actual.get(entry.getKey());
                assertNotNull(actualRow);
                assertEquals(actualRow.get("b"), entry.getValue());
                assertEquals(actualRow.get("c"), entry.getValue());
            }
        }
        );
    }

    /* test utils */

    private void writeSSTable(Partitioner partitioner, CassandraBridge bridge, TestSchema schema, Path testDir, Consumer<CassandraBridge.Writer> writer)
    {
        bridge.writeSSTable(partitioner,
                            schema.keyspace,
                            schema.table,
                            testDir,
                            schema.createStatement,
                            schema.insertStatement,
                            writer);
    }

    private interface TestRunable
    {
        void run(Partitioner partitioner, CassandraBridge bridge, TestSchema schema, Path testDir) throws IOException;
    }

    private static void runTest(TestRunable test)
    {
        qt().forAll(TestUtils.partitioners(), TestUtils.bridges())
            .checkAssert(
            ((partitioner, bridge) -> {
                TestSchema schema = TestSchema.builder(bridge)
                                              .withPartitionKey("a", bridge.text())
                                              .withColumn("b", bridge.text())
                                              .withColumn("c", bridge.text())
                                              .build();

                String testId = UUID.randomUUID().toString();
                Path testDir = tempPath.resolve(testId);
                try
                {
                    TestUtils.createDirectory(testDir);
                    test.run(partitioner, bridge, schema, testDir);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    try
                    {
                        FileUtils.deleteDirectory(testDir.toFile());
                    }
                    catch (IOException ignore)
                    {
                    }
                }
            }
            )
            );
    }
}
