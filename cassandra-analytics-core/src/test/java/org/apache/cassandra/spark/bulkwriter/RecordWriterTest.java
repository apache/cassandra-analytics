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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import scala.Tuple2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordWriterTest
{
    public static final int REPLICA_COUNT = 3;
    public static final int FILES_PER_SSTABLE = 8;
    public static final int UPLOADED_TABLES = 3;

    @TempDir
    public Path folder; // CHECKSTYLE IGNORE: Public mutable field for parameterized testing

    private CassandraRing<RingInstance> ring;
    private RecordWriter rw;
    private MockTableWriter tw;
    private Tokenizer tokenizer;
    private Range<BigInteger> range;
    private MockBulkWriterContext writerContext;
    private TestTaskContext tc;

    @BeforeEach
    public void setUp()
    {
        tw = new MockTableWriter(folder);
        ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test", 12);
        writerContext = new MockBulkWriterContext(ring);
        tc = new TestTaskContext();
        range = writerContext.job().getTokenPartitioner().getTokenRange(tc.partitionId());
        tokenizer = new Tokenizer(writerContext);
    }

    @Test
    public void testSuccessfulWrite()
    {
        rw = new RecordWriter(writerContext, () -> tc, SSTableWriter::new);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
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

    @Test
    public void testCorruptSSTable()
    {
        rw = new RecordWriter(writerContext, () -> tc, (wc, path) ->  new SSTableWriter(tw.setOutDir(path), path));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        // TODO: Add better error handling with human-readable exception messages in SSTableReader::new
        // That way we can assert on the exception thrown here
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
    }

    @Test
    public void testWriteWithOutOfRangeTokenFails()
    {
        rw = new RecordWriter(writerContext, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, false);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertThat(ex.getMessage(),
                   matchesPattern("java.lang.IllegalStateException: Received Token "
                              + "5765203080415074583 outside of expected range \\[-9223372036854775808(‥|..)0]"));
    }

    @Test
    public void testAddRowThrowingFails()
    {
        rw = new RecordWriter(writerContext, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        tw.setAddRowThrows(true);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertEquals(ex.getMessage(), "java.lang.RuntimeException: Failed to write because addRow throws");
    }

    @Test
    public void testBadTimeSkewFails()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just outside the limits
        long sixtyOneMinutesInMillis = TimeUnit.MINUTES.toMillis(61);
        rw = new RecordWriter(writerContext, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        writerContext.setTimeProvider(() -> System.currentTimeMillis() - sixtyOneMinutesInMillis);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertThat(ex.getMessage(), startsWith("Time skew between Spark and Cassandra is too large. Allowable skew is 60 minutes. Spark executor time is "));
    }

    @Test
    public void testTimeSkewWithinLimitsSucceeds()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just inside the limits
        long fiftyNineMinutesInMillis = TimeUnit.MINUTES.toMillis(59);
        long remoteTime = System.currentTimeMillis() - fiftyNineMinutesInMillis;
        rw = new RecordWriter(writerContext, () -> tc, SSTableWriter::new);
        writerContext.setTimeProvider(() -> remoteTime);  // Return a very low "current time" to make sure we fail if skew is too bad
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int numValues, boolean onlyInRange)
    {
        Stream<Tuple2<DecoratedKey, Object[]>> source = IntStream.iterate(0, integer -> integer + 1).mapToObj(index -> {
            Object[] columns = {index, index, "foo" + index, index};
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        });
        if (onlyInRange)
        {
            source = source.filter(val -> range.contains(val._1.getToken()));
        }
        return source.limit(numValues).iterator();
    }
}
