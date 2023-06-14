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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testOpenSSTables() throws ExecutionException, InterruptedException, IOException
    {
        runTest(false);  // Missing no files
    }

    @Test
    public void testMissingNonEssentialFiles() throws ExecutionException, InterruptedException, IOException
    {
        runTest(false, FileType.FILTER);  // Missing non-essential SSTable file component
    }

    @Test
    public void testMissingOnlySummaryFile() throws ExecutionException, InterruptedException, IOException
    {
        // Summary.db can be missing if we can use Index.db
        runTest(false, FileType.SUMMARY);
    }

    @Test
    public void testMissingOnlyIndexFile() throws ExecutionException, InterruptedException, IOException
    {
        // Index.db can be missing if we can use Summary.db
        runTest(false, FileType.INDEX);
    }

    @Test()
    public void testMissingDataFile()
    {
        assertThrows(IOException.class,
                     () -> runTest(true, FileType.DATA)
        );
    }

    @Test()
    public void testMissingStatisticsFile()
    {        assertThrows(IOException.class,
                          () -> runTest(true, FileType.STATISTICS)
    );
    }

    @Test()
    public void testMissingSummaryPrimaryIndex()
    {
        assertThrows(IOException.class,
                     () -> runTest(true, FileType.SUMMARY, FileType.INDEX)
        );
    }

    @Test()
    public void testFailOpenReader()
    {
        assertThrows(IOException.class,
                     () -> runTest(true,
                                   (ssTable, isRepairPrimary) -> {
                         throw new IOException("Couldn't open Summary.db file");
                         },
                Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)))
        );
    }

    @Test
    public void testFilterOverlap() throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(50), BigInteger.valueOf(150L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @Test
    public void testFilterInnerlap() throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(25), BigInteger.valueOf(75L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @Test
    public void testFilterBoundary() throws ExecutionException, InterruptedException, IOException
    {
        // Should not filter out SSTables overlapping with token range
        runTest(false,
                (ssTable, isRepairPrimary) -> new Reader(ssTable, BigInteger.valueOf(100L), BigInteger.valueOf(102L)),
                Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    private static void runTest(
            boolean shouldThrowIOException,
            FileType... missingFileTypes) throws ExecutionException, InterruptedException, IOException
    {
        runTest(shouldThrowIOException,
                (ssTable, isRepairPrimary) -> new Reader(ssTable),
                Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)),
                missingFileTypes);
    }

    private static void runTest(
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
}
