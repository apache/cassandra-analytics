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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import scala.Tuple2;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RecordWriterTest
{
    public static final int REPLICA_COUNT = 3;
    public static final int FILES_PER_SSTABLE = 8;
    public static final int UPLOADED_TABLES = 3;
    private static final String[] COLUMN_NAMES = {"id", "date", "course", "marks"};
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private CassandraRing<RingInstance> ring;
    private RecordWriter rw;
    private MockTableWriter tw;
    private Tokenizer tokenizer;
    private Range<BigInteger> range;
    private MockBulkWriterContext writerContext;
    private TestTaskContext tc;

    @Before
    public void setUp()
    {
        tw = new MockTableWriter(folder.getRoot().toPath());
        ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test", 12);
        writerContext = new MockBulkWriterContext(ring);
        tc = new TestTaskContext();
        range = writerContext.job().getTokenPartitioner().getTokenRange(tc.partitionId());
        tokenizer = new Tokenizer(writerContext);
    }

    @Test
    public void testSuccessfulWrite()
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES);
    }

    @Test
    public void testWriteWithConstantTTL()
    {
        MockBulkWriterContext bulkWriterContext =  new MockBulkWriterContext(ring);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, false);
        validateSuccessfulWrite(bulkWriterContext, data, COLUMN_NAMES);
    }

    @Test
    public void testWriteWithTTLColumn()
    {
        MockBulkWriterContext bulkWriterContext =  new MockBulkWriterContext(ring);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, true, false);
        String[] columnNamesWithTtl = {"id", "date", "course", "marks", "ttl"};
        validateSuccessfulWrite(bulkWriterContext, data, columnNamesWithTtl);
    }

    @Test
    public void testWriteWithConstantTimestamp()
    {
        MockBulkWriterContext bulkWriterContext =  new MockBulkWriterContext(ring);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, false);
        validateSuccessfulWrite(bulkWriterContext, data, COLUMN_NAMES);
    }

    @Test
    public void testWriteWithTimestampColumn()
    {
        MockBulkWriterContext bulkWriterContext =  new MockBulkWriterContext(ring);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, true);
        String[] columnNamesWithTimestamp = {"id", "date", "course", "marks", "timestamp"};
        validateSuccessfulWrite(bulkWriterContext, data, columnNamesWithTimestamp);
    }

    @Test
    public void testWriteWithTimestampAndTTLColumn()
    {
        MockBulkWriterContext bulkWriterContext =  new MockBulkWriterContext(ring);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, true, true);
        String[] columnNames = {"id", "date", "course", "marks", "ttl", "timestamp"};
        validateSuccessfulWrite(bulkWriterContext, data, columnNames);
    }

    @Test
    public void testCorruptSSTable()
    {
        // TODO: Add better error handling with human-readable exception messages in SSTableReader::new
        exception.expect(RuntimeException.class);
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) ->  new SSTableWriter(tw.setOutDir(path), path));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_TABLES));
    }

    @Test
    public void testWriteWithOutOfRangeTokenFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder.getRoot().toPath()));
        exception.expectMessage("Received Token 5765203080415074583 outside of expected range [-9223372036854775808‥0]");
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, false);
        rw.write(data);
    }

    @Test
    public void testAddRowThrowingFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder.getRoot().toPath()));
        tw.setAddRowThrows(true);
        exception.expectMessage("java.lang.RuntimeException: Failed to write because addRow throws");
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
    }

    @Test
    public void testBadTimeSkewFails()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just outside the limits
        long sixtyOneMinutesInMillis = TimeUnit.MINUTES.toMillis(61);
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder.getRoot().toPath()));
        writerContext.setTimeProvider(() -> System.currentTimeMillis() - sixtyOneMinutesInMillis);
        exception.expectMessage("Time skew between Spark and Cassandra is too large. Allowable skew is 60 minutes. Spark executor time is ");
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
    }

    @Test
    public void testTimeSkewWithinLimitsSucceeds()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just inside the limits
        long fiftyNineMinutesInMillis = TimeUnit.MINUTES.toMillis(59);
        long remoteTime = System.currentTimeMillis() - fiftyNineMinutesInMillis;
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        writerContext.setTimeProvider(() -> remoteTime);  // Return a very low "current time" to make sure we fail if skew is too bad
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
    }

    private void validateSuccessfulWrite(MockBulkWriterContext writerContext,
                                         Iterator<Tuple2<DecoratedKey, Object[]>> data,
                                         String[] columnNames)
    {
        RecordWriter rw = new RecordWriter(writerContext, columnNames, () -> tc, SSTableWriter::new);
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_TABLES));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur: requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int numValues, boolean onlyInRange)
    {
        return generateData(numValues, onlyInRange, false, false);
    }
    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int numValues, boolean onlyInRange, boolean withTTL, boolean withTimestamp)
    {
        Stream<Tuple2<DecoratedKey, Object[]>> source = IntStream.iterate(0, integer -> integer + 1).mapToObj(index -> {
            Object[] columns;
            if (withTTL && withTimestamp)
            {
                columns = new Object[]{index, index, "foo" + index, index, index * 100, System.currentTimeMillis() * 1000};
            }
            else if (withTimestamp)
            {
                columns = new Object[]{index, index, "foo" + index, index, System.currentTimeMillis() * 1000};
            }
            else if (withTTL)
            {
                columns = new Object[]{index, index, "foo" + index, index, index * 100};
            }
            else
            {
                columns = new Object[]{index, index, "foo" + index, index};
            }
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        });
        if (onlyInRange)
        {
            source = source.filter(val -> range.contains(val._1.getToken()));
        }
        return source.limit(numValues).iterator();
    }
}
