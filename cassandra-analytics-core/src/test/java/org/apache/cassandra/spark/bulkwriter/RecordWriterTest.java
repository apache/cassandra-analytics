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

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.mockito.Mockito;
import scala.Tuple2;

import static org.apache.cassandra.spark.bulkwriter.MockBulkWriterContext.DEFAULT_CASSANDRA_VERSION;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

class RecordWriterTest
{
    private static final int REPLICA_COUNT = 3;
    private static final int FILES_PER_SSTABLE = 8;
    // writing 270 rows with sstable size cap of 1 MB should produce 2 sstable
    private static final int UPLOADED_SSTABLES = 2;
    private static final int ROWS_COUNT = 270;
    private static final String[] COLUMN_NAMES = {
    "id", "date", "course", "marks"
    };

    @TempDir
    private Path folder;

    private TokenRangeMapping<RingInstance> tokenRangeMapping;
    private RecordWriter rw;
    private MockTableWriter tw;
    private Tokenizer tokenizer;
    private Range<BigInteger> range;
    private MockBulkWriterContext writerContext;
    private TestTaskContext tc;
    private DigestAlgorithm digestAlgorithm;

    @BeforeEach
    public void setUp()
    {
        digestAlgorithm = new XXHash32DigestAlgorithm();
        tw = new MockTableWriter(folder.getRoot());
        tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(100000, ImmutableMap.of("DC1", 3), 12);
        writerContext = new MockBulkWriterContext(tokenRangeMapping);
        writerContext.setSstableDataSizeInMB(1); // defaults to the minimum sstable data size allowed to set
        tc = new TestTaskContext();
        range = writerContext.job().getTokenPartitioner().getTokenRange(tc.partitionId());
        tokenizer = new Tokenizer(writerContext);
    }

    @Test
    void symmetricDifferenceTest()
    {
        Set<Integer> s1 = new HashSet<>(Arrays.asList(1, 2, 3));
        Set<Integer> s2 = new HashSet<>(Arrays.asList(2, 3, 4));
        Set<Integer> s3 = new HashSet<>(Arrays.asList(5, 6, 7));
        Set<Integer> s4 = new HashSet<>(Arrays.asList(5, 6, 7));
        Set<Integer> s5 = new HashSet<>();
        assertThat(RecordWriter.symmetricDifference(s1, s2), is(new HashSet<>(Arrays.asList(1, 4))));
        assertThat(RecordWriter.symmetricDifference(s2, s3), is(new HashSet<>(Arrays.asList(2, 3, 4, 5, 6, 7))));
        assertThat(RecordWriter.symmetricDifference(s3, s4), empty());
        assertThat(RecordWriter.symmetricDifference(s4, s5), is(s4));
    }

    @Test
    void testWriteFailWhenTopologyChangeWithinTask()
    {
        // Generate token range mapping to simulate node movement of the first node by assigning it a different token
        // within the same partition
        int moveTargetToken = 50000;
        TokenRangeMapping<RingInstance> testMapping =
        TokenRangeMappingUtils.buildTokenRangeMapping(100000,
                                                      ImmutableMap.of("DC1", 3),
                                                      12,
                                                      true,
                                                      moveTargetToken);

        MockBulkWriterContext m = Mockito.spy(writerContext);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);

        when(m.getTokenRangeMapping(false)).thenCallRealMethod().thenReturn(testMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertThat(ex.getMessage(), containsString("Token range mappings have changed since the task started"));
    }

    @Test
    void testWriteWithExclusions()
    {
        TokenRangeMapping<RingInstance> testMapping =
        TokenRangeMappingUtils.buildTokenRangeMappingWithFailures(100000,
                                                      ImmutableMap.of("DC1", 3),
                                                      12);

        MockBulkWriterContext m = Mockito.spy(writerContext);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);

        when(m.getTokenRangeMapping(anyBoolean())).thenReturn(testMapping);
        when(m.getInstanceAvailability()).thenCallRealMethod();
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
    }

    @Test
    void testSuccessfulWrite() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES);
    }

    @Test
    void testWriteWithMixedCaseColumnNames() throws InterruptedException
    {
        boolean quoteIdentifiers = true;
        String[] pk = {"ID", "date"};
        String[] columnNames = {"ID", "date", "course", "limit"};

        Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair = TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
        columnNames,
        new DataType[]{DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType, DataTypes.IntegerType},
        new CqlField.CqlType[]{mockCqlType(INT), mockCqlType(DATE), mockCqlType(VARCHAR), mockCqlType(INT)});

        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping,
                                                                        DEFAULT_CASSANDRA_VERSION,
                                                                        ConsistencyLevel.CL.LOCAL_QUORUM,
                                                                        validPair,
                                                                        pk,
                                                                        pk,
                                                                        quoteIdentifiers);
        writerContext.setSstableDataSizeInMB(1);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        validateSuccessfulWrite(writerContext, data, columnNames);
    }

    @Test
    void testSuccessfulWriteCheckUploads()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_SSTABLES));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.digest);
        }
    }

    @Test
    void testWriteWithConstantTTL() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(false, false);
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES);
    }

    @Test
    void testWriteWithTTLColumn() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(true, false);
        String[] columnNamesWithTtl =
        {
        "id", "date", "course", "marks", "ttl"
        };
        validateSuccessfulWrite(writerContext, data, columnNamesWithTtl);
    }

    @Test
    void testWriteWithConstantTimestamp() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(false, false);
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES);
    }

    @Test
    void testWriteWithTimestampColumn() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(false, true);
        String[] columnNamesWithTimestamp =
        {
        "id", "date", "course", "marks", "timestamp"
        };
        validateSuccessfulWrite(writerContext, data, columnNamesWithTimestamp);
    }

    @Test
    void testWriteWithTimestampAndTTLColumn() throws InterruptedException
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(true, true);
        String[] columnNames =
        {
        "id", "date", "course", "marks", "ttl", "timestamp"
        };
        validateSuccessfulWrite(writerContext, data, columnNames);
    }

    @Test
    void testWriteWithSubRanges()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);

        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.openClosed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);

        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateDataWithFakeToken(ROWS_COUNT, range);
        List<StreamResult> res = rw.write(data).streamResults();
        assertEquals(1, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        assertEquals(range, res.get(0).tokenRange);
        Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals(REPLICA_COUNT, uploads.keySet().size());
        assertEquals(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_SSTABLES, uploads.values().stream().mapToInt(List::size).sum());
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.digest);
        }
    }

    @Test
    void testWriteWithDataInMultipleSubRanges()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);
        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.openClosed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000));
        Range<BigInteger> firstSubrange = Range.openClosed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(100000));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);

        // Generate rows that belong to the first sub-range
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateDataWithFakeToken(ROWS_COUNT, firstSubrange);
        List<StreamResult> res = rw.write(data).streamResults();
        assertEquals(1, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        assertEquals(firstSubrange, res.get(0).tokenRange);
        Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals(REPLICA_COUNT, uploads.keySet().size());
        assertEquals(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_SSTABLES,
                     uploads.values().stream().mapToInt(List::size).sum());
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.digest);
        }
    }

    @Test
    void testWriteWithTokensAcrossSubRanges()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        m.setSstableDataSizeInMB(1);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);
        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.openClosed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000L));
        Range<BigInteger> firstSubrange = Range.openClosed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(100000L));
        Range<BigInteger> secondSubrange = Range.openClosed(BigInteger.valueOf(100000L), BigInteger.valueOf(200000L));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);
        int numRows = 1; // generate 1 row in each range
        Iterator<Tuple2<DecoratedKey, Object[]>> firstRangeData = generateDataWithFakeToken(numRows, firstSubrange);
        Iterator<Tuple2<DecoratedKey, Object[]>> secondRangeData = generateDataWithFakeToken(numRows, secondSubrange);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = Iterators.concat(firstRangeData, secondRangeData);
        List<StreamResult> res = rw.write(data).streamResults();
        // We expect 2 streams since rows belong to different sub-ranges
        assertEquals(2, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        final Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals((REPLICA_COUNT + 1), uploads.keySet().size());

        // There are a total of 2 SSTable files - One for each sub-range
        // Although the replica-sets for each file were different they will still be 3 for each subrange
        assertEquals(REPLICA_COUNT * FILES_PER_SSTABLE * 2, uploads.values().stream().mapToInt(List::size).sum());
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.digest);
        }
    }

    @Test
    void testCorruptSSTable()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc,
                              (wc, path, dp, pid) -> new SortedSSTableWriter(tw.setOutDir(path), path, digestAlgorithm, pid));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        // TODO: Add better error handling with human-readable exception messages in SSTableReader::new
        // That way we can assert on the exception thrown here
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
    }

    @Test
    void testWriteWithOutOfRangeTokenFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc,
                              (wc, path, dp, pid) -> new SortedSSTableWriter(tw, folder, digestAlgorithm, pid));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, Range.all(), false, false, false);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        String expectedErr = "java.lang.IllegalStateException: Received Token " +
                             "5765203080415074583 outside the expected ranges [(-9223372036854775808‥100000]]";
        assertEquals(expectedErr, ex.getMessage());
    }

    @Test
    void testAddRowThrowingFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc,
                              (wc, path, dp, pid) -> new SortedSSTableWriter(tw, folder, digestAlgorithm, pid));
        tw.setAddRowThrows(true);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertEquals("java.lang.RuntimeException: Failed to write because addRow throws", ex.getMessage());
    }

    @Test
    void testBadTimeSkewFails()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just outside the limits
        long sixtyOneMinutesInMillis = TimeUnit.MINUTES.toMillis(61);
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc,
                              (wc, path, dp, pid) -> new SortedSSTableWriter(tw, folder, digestAlgorithm, pid));
        writerContext.setTimeProvider(() -> System.currentTimeMillis() - sixtyOneMinutesInMillis);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertThat(ex.getMessage(), startsWith("Time skew between Spark and Cassandra is too large. Allowable skew is 60 minutes. Spark executor time is "));
    }

    @Test
    void testTimeSkewWithinLimitsSucceeds()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just inside the limits
        long fiftyNineMinutesInMillis = TimeUnit.MINUTES.toMillis(59);
        long remoteTime = System.currentTimeMillis() - fiftyNineMinutesInMillis;
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, SortedSSTableWriter::new);
        writerContext.setTimeProvider(() -> remoteTime);  // Return a very low "current time" to make sure we fail if skew is too bad
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData();
        rw.write(data);
    }

    private void validateSuccessfulWrite(MockBulkWriterContext writerContext,
                                         Iterator<Tuple2<DecoratedKey, Object[]>> data,
                                         String[] columnNames) throws InterruptedException
    {
        validateSuccessfulWrite(writerContext,
                                data,
                                columnNames,
                                REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_SSTABLES,
                                new CountDownLatch(0));
    }

    private void validateSuccessfulWrite(MockBulkWriterContext writerContext,
                                         Iterator<Tuple2<DecoratedKey, Object[]>> data,
                                         String[] columnNames,
                                         int expectedUploads,
                                         CountDownLatch uploadsLatch) throws InterruptedException
    {
        RecordWriter rw = new RecordWriter(writerContext, columnNames, () -> tc, SortedSSTableWriter::new);
        rw.write(data);

        uploadsLatch.await(1, TimeUnit.SECONDS);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(expectedUploads));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.digest);
        }
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData()
    {
        return generateData(false, false);
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(boolean withTTL, boolean withTimestamp)
    {
        return generateData(ROWS_COUNT, range, false, withTTL, withTimestamp);
    }

    // generate data with fake tokens assigend. The fake tokens are provided by the input range.
    // Although the data generated have fake tokens, the actual tokens computed from each tuple
    // are still ordered ascendingly.
    private Iterator<Tuple2<DecoratedKey, Object[]>> generateDataWithFakeToken(int numValues, Range<BigInteger> range)
    {
        return generateData(numValues, range, /* fake tokens */ true, /* ttl */ false, /* timestamp */ false);
    }

    /**
     * Generate test data
     * @param rowsCount number of rows to generate; the rows are sorted
     * @param tokenRange token range that the generated rows belong to
     * @param fakeTokens whether to fake tokens of each row. The acutal tokens of each row are still sorted
     * @param withTTL wthether to add the TTL
     * @param withTimestamp whether to add the timestamp
     * @return iterator of the test rows
     */
    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int rowsCount,
                                                                  Range<BigInteger> tokenRange,
                                                                  boolean fakeTokens,
                                                                  boolean withTTL, boolean withTimestamp)
    {
        String courseString = IntStream.range(0, 100000).boxed().map(i -> "Long long string").collect(Collectors.joining());
        Stream<Tuple2<DecoratedKey, Object[]>> source = IntStream.iterate(0, integer -> integer + 1).mapToObj(index -> {
            Object[] columns;
            if (withTTL && withTimestamp)
            {
                columns = new Object[]
                          {
                          index, index, courseString, index, index * 100, System.currentTimeMillis() * 1000
                          };
            }
            else if (withTimestamp)
            {
                columns = new Object[]
                          {
                          index, index, courseString, index, System.currentTimeMillis() * 1000
                          };
            }
            else if (withTTL)
            {
                columns = new Object[]
                          {
                          index, index, courseString, index, index * 100
                          };
            }
            else
            {
                columns = new Object[]
                          {
                          index, index, courseString, index
                          };
            }
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        });

        // filter based on the actual token if the test does not want to fake tokens
        if (!fakeTokens)
        {
            source = source.filter(val -> tokenRange.contains(val._1.getToken()));
        }

        Stream<Tuple2<DecoratedKey, Object[]>> limitedStream = source.limit(rowsCount);

        List<Tuple2<DecoratedKey, Object[]>> sortedData = limitedStream.sorted(Comparator.comparing(tuple -> tuple._1().getToken()))
                                                                       .collect(Collectors.toList());

        if (fakeTokens)
        {
            // update each tuple with fake token assigned
            // bytebuffer keys in the list are still ordered
            BigInteger currentToken = tokenRange.lowerEndpoint().add(BigInteger.ONE); // lower end is open; thus plus 1
            for (int i = 0; i < sortedData.size(); i++, currentToken = currentToken.add(BigInteger.ONE))
            {
                Tuple2<DecoratedKey, Object[]> item = sortedData.get(i);
                DecoratedKey key = item._1();
                item = Tuple2.apply(new DecoratedKey(currentToken, key.getKey()), item._2());
                sortedData.set(i, item);
            }
        }

        return sortedData.iterator();
    }
}
