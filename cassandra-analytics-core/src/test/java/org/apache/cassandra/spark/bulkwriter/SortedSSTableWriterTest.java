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

package org.apache.cassandra.spark.bulkwriter;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.BloomFilter;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class SortedSSTableWriterTest
{
    private static String previousMbeanState;

    public static Iterable<Object[]> supportedVersions()
    {
        return Arrays.stream(CassandraVersion.supportedVersions())
                     .map(version -> new Object[]{version})
                     .collect(Collectors.toList());
    }

    @NotNull
    private final TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 12);

    @BeforeAll
    public static void setProps()
    {
        previousMbeanState = System.getProperty("org.apache.cassandra.disable_mbean_registration");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    @AfterAll
    public static void restoreProps()
    {
        if (previousMbeanState != null)
        {
            System.setProperty("org.apache.cassandra.disable_mbean_registration", previousMbeanState);
        }
        else
        {
            System.clearProperty("org.apache.cassandra.disable_mbean_registration");
        }
    }

    @TempDir
    private Path tmpDir;

    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void canCreateWriterForVersion(String version) throws IOException
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);
        SortedSSTableWriter tw = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        List<SSTableDescriptor> allSSTables = new ArrayList<>();
        tw.setSSTablesProducedListener(allSSTables::addAll);
        tw.addRow(BigInteger.ONE, ImmutableMap.of("id", 1, "date", 1, "course", "foo", "marks", 1));
        tw.close(writerContext);
        assertThat(allSSTables).hasSize(1);
        String baseFileName = allSSTables.get(0).baseFilename;
        CassandraVersionFeatures cvf = CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version);
        switch (cvf.getMajorVersion())
        {
            case 40:
            case 41:
                // Format is "nb-<generation>-big"
                assertThat(baseFileName).matches("nb-\\d+-big");
                break;
            case 50:
                // Format is "oa-<generation>-big" or "da-<generation>-bti"
                if ("big".equals(CassandraVersion.configuredSSTableFormat()))
                {
                    assertThat(baseFileName).matches("oa-\\d+-big");
                }
                else
                {
                    assertThat(baseFileName).matches("da-\\d+-bti");
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported version: " + version);
        }
        Set<Path> dataFilePaths = new HashSet<>();
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(tw.getOutDir(), "*Data.db"))
        {
            dataFileStream.forEach(dataFilePath -> {
                dataFilePaths.add(dataFilePath);
                int sstableVersion = SSTables.cassandraVersionFromTable(dataFilePath).getMajorVersion();
                int expectedVersion = CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version).getMajorVersion();
                // 4.0 and 4.1 share the same SSTable format (nb), so the SSTable-level version is always 40
                if (expectedVersion == 41)
                {
                    expectedVersion = 40;
                }
                assertThat(sstableVersion).isEqualTo(expectedVersion);
            });
        }
        // no exception should be thrown from both the validate methods
        tw.validateSSTables(writerContext);
        tw.validateSSTables(writerContext, tw.getOutDir(), dataFilePaths);
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void testBloomFilterRebuild(String version) throws IOException
    {
        int rowCount = 50_000;
        CassandraBridge bridge = CassandraBridgeFactory.get(version);
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);
        Partitioner partitioner = writerContext.getPartitioner();
        CqlTable cqlTable = bridge.buildSchema(writerContext.schema().getTableSchema().createStatement,
                                               writerContext.qualifiedTableName().keyspace(),
                                               new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                     ImmutableMap.of("replication_factor", 1)),
                                               partitioner,
                                               Collections.emptySet());
        SortedMap<BigInteger, List<String>> sortedKeys = new TreeMap<>();
        for (int i = 0; i < rowCount; ++i)
        {
            List<String> keys = ImmutableList.of(String.valueOf(i), "1");
            AbstractMap.SimpleEntry<ByteBuffer, BigInteger> partitionKey = bridge.getPartitionKey(cqlTable, partitioner, keys);
            sortedKeys.put(partitionKey.getValue(), keys);
        }

        SortedSSTableWriter tw = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        List<SSTableDescriptor> allSSTables = new ArrayList<>();
        tw.setSSTablesProducedListener(allSSTables::addAll);
        for (BigInteger token : sortedKeys.keySet())
        {
            List<String> partitionKey = sortedKeys.get(token);
            tw.addRow(token,
                      ImmutableMap.of("id", Integer.parseInt(partitionKey.get(0)),
                                      "date", Integer.parseInt(partitionKey.get(1)),
                                      "course", "foo", "marks", 1));
        }
        tw.close(writerContext);

        assertThat(allSSTables).hasSize(1);

        Set<Path> filterFilePaths = new HashSet<>();
        try (DirectoryStream<Path> filterFileStream = Files.newDirectoryStream(tw.getOutDir(), "*-Filter.db"))
        {
            filterFileStream.forEach(filterFilePaths::add);
        }

        assertThat(filterFilePaths).hasSize(1);

        Path filterFile = filterFilePaths.iterator().next();
        String dataFileName = filterFile.toFile().getName().replace("-Filter", "-Data");
        Path dataFilePath = filterFile.getParent().resolve(dataFileName);
        FileSystemSSTable ssTable = new FileSystemSSTable(dataFilePath, false, BufferingInputStreamStats::doNothingStats);

        BloomFilter bloomFilter = bridge.openBloomFilter(partitioner,
                                                         writerContext.qualifiedTableName().keyspace(),
                                                         writerContext.qualifiedTableName().table(),
                                                         ssTable);

        // second column is always set to 1 when inserting data
        List<ByteBuffer> searchKeys = bridge.encodePartitionKeys(partitioner,
                                                                 writerContext.qualifiedTableName().keyspace(),
                                                                 writerContext.schema().getTableSchema().createStatement,
                                                                 ImmutableList.of(ImmutableList.of("1", "1"), ImmutableList.of("7", "2")));

        assertThat(bloomFilter.mightContain(searchKeys.get(0))).isTrue();
        // Flaky assertion: bloom filters can answer false positive, but since we are using limited data set,
        // it is unlikely to happen.
        assertThat(bloomFilter.doesNotContain(searchKeys.get(1))).isTrue();
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void testBloomFilterRebuildErrorHandling(String version) throws IOException
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);
        SortedSSTableWriter tw = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1)
        {
            protected void rebuildFilterComponents(@NotNull BulkWriterContext writerContext,
                                                   @NotNull DirectoryStream.Filter<Path> filter) throws IOException
            {
                // temporarily move index file to simulate error in bloom filter rebuild process
                try (DirectoryStream<Path> indexFileStream = Files.newDirectoryStream(getOutDir(), "*.db"))
                {
                    indexFileStream.forEach(indexFilePath -> {
                        for (String indexSuffix : Arrays.asList("Partitions.db", "Index.db"))
                        {
                            if (indexFilePath.toFile().getName().endsWith(indexSuffix))
                            {
                                File indexFile = indexFilePath.toFile();
                                boolean moved = indexFile.renameTo(new File(indexFile.getAbsolutePath() + "_hidden"));
                                assertThat(moved).isTrue();
                            }
                        }
                    });
                }
                super.rebuildFilterComponents(writerContext, filter);
                // move the index files back
                try (DirectoryStream<Path> hiddenFileStream = Files.newDirectoryStream(getOutDir(), "*_hidden"))
                {
                    hiddenFileStream.forEach(hiddenFilePath -> {
                        File hiddenFile = hiddenFilePath.toFile();
                        boolean moved = hiddenFile.renameTo(new File(hiddenFile.getParent(), hiddenFile.getName().replace("_hidden", "")));
                        assertThat(moved).isTrue();
                    });
                }
            }
        };
        List<SSTableDescriptor> allSSTables = new ArrayList<>();
        tw.setSSTablesProducedListener(allSSTables::addAll);
        tw.addRow(BigInteger.ONE, ImmutableMap.of("id", 1, "date", 1, "course", "foo", "marks", 1));
        tw.close(writerContext);
        assertThat(allSSTables).hasSize(1);
        // verify that bloom filter was not created
        try (DirectoryStream<Path> filterFileStream = Files.newDirectoryStream(tw.getOutDir(), "*Filter.db"))
        {
            assertThat(filterFileStream.iterator().hasNext()).isFalse();
        }
    }

    /**
     * Tests the race condition fix between prepareSStablesToSend (called from background threads)
     * and close (called from the main thread). This test exercises CASSANALYTICS-107.
     *
     * This test focuses on verifying thread safety when:
     * 1. prepareSStablesToSend is called repeatedly from a background thread
     * 2. close is called from the main thread
     * 3. Both methods access shared state concurrently
     */
    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void testConcurrentPrepareSStablesToSendAndClose(String version) throws Exception
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);

        // First, create real SSTables that will be used to simulate the race
        // These SSTables will be in tmpDir and represent the "intermediate flush" scenario
        List<SSTableDescriptor> existingSSTables = mockSSTableProduced(writerContext);

        // Verify we have SSTables to work with
        assertThat(existingSSTables).as("Should have produced SSTables").isNotEmpty();

        // Now create the writer that will be tested for the race condition
        // It will share the same directory (tmpDir) where the SSTables already exist, i.e. already have sstables produced
        SortedSSTableWriter testWriter = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        testWriter.setSSTablesProducedListener(x -> {});

        // Add a row to the test writer (this will produce another SSTable on close)
        testWriter.addRow(BigInteger.valueOf(100), ImmutableMap.of("id", 2, "date", 2, "course", "test2", "marks", 200));

        // Simulate the race: prepareSStablesToSend in background thread processing existing SSTables,
        // while close is called in main thread
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(2);

            // Background thread: repeatedly call prepareSStablesToSend with the existing SSTables
            // This simulates DirectStreamSession#onSSTablesProduced processing an intermediate flush
            Future<?> prepareFuture = executor.submit(() -> {
                Uninterruptibles.awaitUninterruptibly(startLatch);
                try
                {
                    // Repeatedly call prepareSStablesToSend to increase chance of race
                    for (int i = 0; i < 50; i++)
                    {
                        try
                        {
                            // Use the existing SSTables to simulate real scenario
                            testWriter.prepareSStablesToSend(writerContext, new HashSet<>(existingSSTables));
                            Thread.yield();
                        }
                        catch (IOException e)
                        {
                            // IOException is acceptable (e.g., files already processed)
                            // But ConcurrentModificationException would indicate a threading bug
                            String message = e.getMessage();
                            if (message != null && message.toLowerCase().contains("concurrent"))
                            {
                                throw new RuntimeException("Thread safety violation detected", e);
                            }
                        }
                    }
                }
                finally
                {
                    completionLatch.countDown();
                }
            });

            // Main thread: call close
            Future<?> closeFuture = executor.submit(() -> {
                Uninterruptibles.awaitUninterruptibly(startLatch);
                try
                {
                    // Small delay to let prepareSStablesToSend start
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
                    testWriter.close(writerContext);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("close failed", e);
                }
                finally
                {
                    completionLatch.countDown();
                }
            });

            // Start both operations concurrently
            startLatch.countDown();

            // Wait for both to complete
            assertThat(completionLatch.await(30, TimeUnit.SECONDS))
            .as("Both operations should complete within timeout")
            .isTrue();

            // Verify neither future threw an exception
            prepareFuture.get(5, TimeUnit.SECONDS);
            closeFuture.get(5, TimeUnit.SECONDS);

            // Verify the writer is in a consistent state
            assertThat(testWriter.sstableCount()).isEqualTo(2);
            assertThat(testWriter.bytesWritten()).isGreaterThan(0);

            // Verify file digests map is not corrupted
            assertThat(testWriter.fileDigestMap()).isNotEmpty();

            // Verify SSTables can still be validated (no data corruption)
            testWriter.validateSSTables(writerContext);
        }
        finally
        {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS))
            .as("Executor should terminate cleanly")
            .isTrue();
        }
    }

    /**
     * Tests the scenario where prepareSStablesToSend is called with produced SSTables,
     * those files are then deleted (simulating DirectStreamSession behavior), and then
     * close() is called. This verifies that close() only calculates bytesWritten for
     * newly produced files, not the already-processed (and deleted) ones.
     * This test exercises CASSANALYTICS-107.
     */
    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void testBytesWrittenWithDeletedFiles(String version) throws Exception
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);

        // Create initial SSTables to simulate intermediate flush
        List<SSTableDescriptor> existingSSTables = mockSSTableProduced(writerContext);
        assertThat(existingSSTables).as("Should have produced SSTables").isNotEmpty();

        // Create a new writer that will process the existing SSTables
        SortedSSTableWriter writer = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        writer.setSSTablesProducedListener(x -> {});

        // Add a row to produce another SSTable on close (use higher token to maintain order)
        writer.addRow(BigInteger.valueOf(100), ImmutableMap.of("id", 2, "date", 2, "course", "test2", "marks", 200));

        // Call prepareSStablesToSend with the existing SSTables
        SortedSSTableWriter.PreparedSSTables processedFiles = writer.prepareSStablesToSend(writerContext, new HashSet<>(existingSSTables));
        assertThat(processedFiles.sstables()).as("Should have processed existing SSTables").isNotEmpty();

        long bytesAfterPrepare = writer.bytesWritten();

        // Delete the files that were processed (simulating DirectStreamSession behavior)
        for (Path path : processedFiles.files())
        {
            Files.deleteIfExists(path);
        }

        // Now close - this should only count NEW files, not trying to re-count deleted ones
        // No NoSuchFileException should be thrown
        assertThatNoException().isThrownBy(() -> writer.close(writerContext));

        // Verify bytesWritten increased (from close processing new files)
        assertThat(writer.bytesWritten())
        .as("bytesWritten should have increased after close")
        .isGreaterThanOrEqualTo(bytesAfterPrepare);

        assertThat(writer.sstableCount()).as("Should have correct sstable count").isEqualTo(2);
    }

    /**
     * Tests that prepareSStablesToSend returns an empty map when called after close().
     * This verifies that the method properly guards against being called after the writer is closed,
     * which would otherwise cause double-counting of SSTables and bytes.
     */
    @ParameterizedTest
    @MethodSource("supportedVersions")
    public void testPrepareSStablesToSendAfterClose(String version) throws Exception
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);

        // Create a writer and add a row
        SortedSSTableWriter writer = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        writer.setSSTablesProducedListener(x -> {});

        writer.addRow(BigInteger.valueOf(100), ImmutableMap.of("id", 2, "date", 2, "course", "test2", "marks", 200));

        // Close the writer first
        writer.close(writerContext);

        // Record the state after close
        int sstableCountAfterClose = writer.sstableCount();
        long bytesWrittenAfterClose = writer.bytesWritten();
        int fileDigestCountAfterClose = writer.fileDigestMap().size();

        // Try to call prepareSStablesToSend after close - it should return empty map
        SortedSSTableWriter.PreparedSSTables result = writer.prepareSStablesToSend(writerContext, new HashSet<>());

        // Verify it returned an empty map
        assertThat(result.sstables())
        .as("prepareSStablesToSend should return empty map when called after close")
        .isEmpty();

        // Verify that counters were NOT incremented (no double-counting)
        assertThat(writer.sstableCount())
        .as("sstableCount should not change when prepareSStablesToSend is called after close")
        .isEqualTo(sstableCountAfterClose);

        assertThat(writer.bytesWritten())
        .as("bytesWritten should not change when prepareSStablesToSend is called after close")
        .isEqualTo(bytesWrittenAfterClose);

        assertThat(writer.fileDigestMap().size())
        .as("fileDigestMap size should not change when prepareSStablesToSend is called after close")
        .isEqualTo(fileDigestCountAfterClose);
    }

    /**
     * Helper method to create initial SSTables for testing.
     * This simulates the scenario where SSTables are produced during intermediate flushes, i.e. prepareSStablesToSend
     */
    private List<SSTableDescriptor> mockSSTableProduced(MockBulkWriterContext writerContext) throws IOException
    {
        SortedSSTableWriter initialWriter = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        List<SSTableDescriptor> producedSSTables = new ArrayList<>();
        initialWriter.setSSTablesProducedListener(producedSSTables::addAll);

        // Write a row to produce real SSTables
        // Use token BigInteger.ONE for consistency
        initialWriter.addRow(BigInteger.ONE, ImmutableMap.of("id", 1, "date", 1, "course", "test1", "marks", 100));
        initialWriter.close(writerContext);

        return producedSSTables;
    }
}
