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

package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.IncompleteSSTableException;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SingleReplicaTests
{
    public static final ExecutorService EXECUTOR =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("replicas-tests-%d")
                                                                        .setDaemon(true)
                                                                        .build());

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testOpenSSTables(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        runTest(dataFileName, false);  // Missing no files
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testMissingNonEssentialFiles(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        runTest(dataFileName, false, FileType.FILTER);  // Missing non-essential SSTable file component
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testMissingOnlySummaryFile(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        // Summary.db can be missing if we can use Index.db
        runTest(dataFileName, false, FileType.SUMMARY);
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testMissingOnlyIndexFile(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        // Index.db can be missing if we can use Summary.db
        runTest(dataFileName, false, FileType.INDEX);
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testMissingDataFile(String dataFileName)
    {
        assertThrows(IOException.class,
                     () -> runTest(dataFileName, true, FileType.DATA)
        );
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testMissingStatisticsFile(String dataFileName)
    {        assertThrows(IOException.class,
                          () -> runTest(dataFileName, true, FileType.STATISTICS)
    );
    }

    @Test()
    public void testMissingSummaryPrimaryIndex()
    {
        assertThrows(IOException.class,
                     () -> runTest("na-1-big-Data.db", true, FileType.SUMMARY, FileType.INDEX)
        );
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testFailOpenReader(String dataFileName)
    {
        assertThrows(IOException.class,
                     () -> runTest(dataFileName,
                                   true,
                                   (ssTable, isRepairPrimary) -> {
                         throw new IOException("Couldn't open Summary.db file");
                         },
                Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)))
        );
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testFilterOverlap(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(dataFileName,
                false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(50), BigInteger.valueOf(150L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testFilterInnerlap(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(dataFileName,
                false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(25), BigInteger.valueOf(75L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @ParameterizedTest
    @MethodSource("dataFileNames")
    public void testFilterBoundary(String dataFileName) throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(dataFileName,
                false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(100L), BigInteger.valueOf(102L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    private static void runTest(
            String dataFileName,
            boolean shouldThrowIOException,
            FileType... missingFileTypes) throws ExecutionException, InterruptedException, IOException
    {
        runTest(dataFileName,
                shouldThrowIOException,
                (ssTable, isRepairPrimary) -> new Reader(ssTable),
                Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)),
                missingFileTypes);
    }

    private static void runTest(
            String dataFileName,
            boolean shouldThrowIOException,
            SSTablesSupplier.ReaderOpener<Reader> readerOpener,
            Range<BigInteger> range,
            FileType... missingFileTypes) throws InterruptedException, IOException, ExecutionException
    {
        PartitionedDataLayer dataLayer = mock(PartitionedDataLayer.class);
        CassandraInstance instance = new CassandraInstance("-9223372036854775808", "local1-i1", "DC1");

        SSTable ssTable1 = mockSSTable();
        SSTable ssTable2 = mockSSTable();
        SSTable ssTable3 = mockSSTable();
        for (FileType fileType : missingFileTypes)
        {
            // verify() should throw IncompleteSSTableException when missing Statistic.db file
            when(ssTable3.isMissing(eq(fileType))).thenReturn(true);
            doCallRealMethod().when(ssTable3).isBigFormat();
            doCallRealMethod().when(ssTable3).isBtiFormat();
            when(ssTable3.getDataFileName()).thenReturn(dataFileName);
        }

        Stream<SSTable> sstables = Stream.of(ssTable1, ssTable2, ssTable3);
        when(dataLayer.listInstance(eq(0), eq(range), eq(instance)))
                .thenReturn(CompletableFuture.completedFuture(sstables));

        SingleReplica replica = new SingleReplica(instance, dataLayer, range, 0, EXECUTOR, true);
        Set<Reader> readers;
        try
        {
            readers = replica.openReplicaAsync(readerOpener).get();
        }
        catch (ExecutionException exception)
        {
            // Extract IOException and rethrow if wrapped in SSTableStreamException
            IOException io = SSTableStreamException.getIOException(exception);
            if (io != null)
            {
                throw io;
            }
            throw exception;
        }
        if (shouldThrowIOException)
        {
            fail("Should throw IOException because an SSTable is corrupt");
        }
        assertEquals(3, readers.size());
    }

    static SSTable mockSSTable() throws IncompleteSSTableException
    {
        SSTable ssTable = mock(SSTable.class);
        when(ssTable.isMissing(any(FileType.class))).thenReturn(false);
        doCallRealMethod().when(ssTable).verify();
        return ssTable;
    }

    public static class Reader implements SparkSSTableReader
    {
        BigInteger firstToken;
        BigInteger lastToken;
        SSTable ssTable;

        Reader(SSTable ssTable)
        {
            this(ssTable, BigInteger.valueOf(0L), BigInteger.valueOf(1L));
        }

        Reader(SSTable ssTable, @Nullable BigInteger firstToken, @Nullable BigInteger lastToken)
        {
            this.ssTable = ssTable;
            this.firstToken = firstToken;
            this.lastToken = lastToken;
        }

        @Override
        public BigInteger firstToken()
        {
            return firstToken;
        }

        @Override
        public BigInteger lastToken()
        {
            return lastToken;
        }

        public boolean ignore()
        {
            return false;
        }
    }

    public static List<Named<String>> dataFileNames()
    {
        return Arrays.asList(Named.of("BIG", "na-1-big-Data.db"),
                             Named.of("BTI", "na-1-bti-Data.db"));
    }
}
