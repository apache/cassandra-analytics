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

package org.apache.cassandra.spark.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.backup.BackupReaderConfig;
import org.apache.cassandra.spark.data.backup.BackupReaderFactory;
import org.apache.cassandra.spark.data.backup.FakeBackupReader;

import software.amazon.awssdk.core.exception.SdkException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableTokenIndexTest
{
    @Test
    void missingIndexEntryFailsOpen()
    {
        SSTableTokenIndex index = SSTableTokenIndex.fromShards(Collections.emptyList());

        assertTrue(index.include(key("node1", "1"), range(0L, 100L)));
    }

    @Test
    void includeUsesConnectedRangeSemantics()
    {
        SSTableKey key = key("node1", "1");
        SSTableTokenIndex index = indexFor(key, new SSTableTokenBounds(100L, 200L));

        assertTrue(index.include(key, range(50L, 100L)));
        assertFalse(index.include(key, range(201L, 300L)));
    }

    @Test
    void invertedBoundsAreNormalized()
    {
        SSTableKey key = key("node1", "1");
        SSTableTokenIndex index = indexFor(key, new SSTableTokenBounds(200L, 100L));

        assertTrue(index.include(key, range(150L, 160L)));
    }

    @Test
    void lookupSurvivesSerializationByValue() throws Exception
    {
        SSTableKey original = key("node1", "1");
        SSTableTokenIndex index = indexFor(original, new SSTableTokenBounds(100L, 200L));

        SSTableTokenIndex roundTripped = roundTrip(index);

        assertFalse(roundTripped.include(key("node1", "1"), range(201L, 300L)));
        assertTrue(roundTripped.include(key("node1", "2"), range(201L, 300L)));
    }

    @Test
    void indexMergesShardCounts()
    {
        SSTableKey key1 = key("node1", "1");
        SSTableKey key2 = key("node1", "2");
        Map<SSTableIndexKey, SSTableTokenBounds> shard1Bounds = new HashMap<>();
        shard1Bounds.put(SSTableIndexKey.from(key1), new SSTableTokenBounds(0L, 100L));
        Map<SSTableIndexKey, SSTableTokenBounds> shard2Bounds = new HashMap<>();
        shard2Bounds.put(SSTableIndexKey.from(key2), new SSTableTokenBounds(200L, 300L));

        SSTableTokenIndex index = SSTableTokenIndex.fromShards(Arrays.asList(new TokenIndexShard(shard1Bounds, 2, 3),
                                                                             new TokenIndexShard(shard2Bounds, 5, 7)));

        assertEquals(2, index.size());
        assertEquals(2, index.successCount());
        assertEquals(7, index.missingCount());
        assertEquals(10, index.errorCount());
        assertFalse(index.include(key1, range(101L, 199L)));
        assertTrue(index.include(key2, range(250L, 260L)));
    }

    @Test
    void workItemAndShardAreImmutableAndSerializable() throws Exception
    {
        SSTableKey key = key("node1", "1");
        Map<FileType, Long> componentSizes = new HashMap<>();
        componentSizes.put(FileType.DATA, 10L);
        SSTableSummaryWorkItem workItem = new SSTableSummaryWorkItem(key, "0", componentSizes);
        componentSizes.put(FileType.SUMMARY, 20L);

        assertFalse(workItem.componentSizes().containsKey(FileType.SUMMARY));
        assertThrows(UnsupportedOperationException.class,
                     () -> workItem.componentSizes().put(FileType.SUMMARY, 20L));
        assertEquals(workItem.indexKey(), roundTrip(workItem).indexKey());

        Map<SSTableIndexKey, SSTableTokenBounds> shardBounds = new HashMap<>();
        shardBounds.put(SSTableIndexKey.from(key), new SSTableTokenBounds(0L, 10L));
        TokenIndexShard shard = new TokenIndexShard(shardBounds, 1, 2);
        shardBounds.clear();

        assertEquals(1, shard.successCount());
        assertEquals(1, shard.boundsBySSTable().size());
        assertThrows(UnsupportedOperationException.class, () -> shard.boundsBySSTable().clear());
        assertEquals(1, roundTrip(shard).boundsBySSTable().size());
    }

    @Test
    void builderMarksMissingSummaryAsFailOpen()
    {
        SSTableKey key = key("node1", "1");
        SSTableSummaryWorkItem workItem = new SSTableSummaryWorkItem(key,
                                                                     "0",
                                                                     Collections.singletonMap(FileType.DATA, 10L));

        S3ClientConfig s3Config = S3ClientConfig.create("us-west-2", "bucket", null, null, null);
        BackupReaderFactory factory = config -> new FakeBackupReader(config.s3Config(), config.s3Config().s3Bucket());
        TokenIndexShard shard = SSTableTokenIndexBuilder.buildShard(Collections.singletonList(workItem).iterator(),
                                                                    factory,
                                                                    BackupReaderConfig.of(s3Config),
                                                                    "cluster",
                                                                    "DC1",
                                                                    CassandraVersion.FOURZERO,
                                                                    0);
        SSTableTokenIndex index = SSTableTokenIndex.fromShards(Collections.singletonList(shard));

        assertEquals(0, shard.successCount());
        assertEquals(1, shard.missingCount());
        assertEquals(0, shard.errorCount());
        assertTrue(index.include(key, range(1_000L, 2_000L)));
    }

    @Test
    void summaryRetryRetriesNetworkIoFailures() throws Exception
    {
        AtomicInteger attempts = new AtomicInteger();

        String result = SSTableTokenIndexBuilder.executeWithRetry(() -> {
            if (attempts.incrementAndGet() < 3)
            {
                throw new SocketTimeoutException("transient Summary.db read failure");
            }
            return "ok";
        }, 3, 0L, 0L);

        assertEquals("ok", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void summaryRetryRetriesOuterRetryableSdkFailures() throws Exception
    {
        AtomicInteger attempts = new AtomicInteger();

        String result = SSTableTokenIndexBuilder.executeWithRetry(() -> {
            if (attempts.incrementAndGet() < 3)
            {
                throw new RetryableSdkException("transient AWS SDK failure",
                                                new IllegalStateException("non-socket inner failure"));
            }
            return "ok";
        }, 3, 0L, 0L);

        assertEquals("ok", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void summaryRetryDoesNotRetryPlainIoFailures()
    {
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(IOException.class,
                     () -> SSTableTokenIndexBuilder.executeWithRetry(() -> {
                         attempts.incrementAndGet();
                         throw new IOException("deterministic Summary.db read failure");
                     }, 3, 0L, 0L));

        assertEquals(1, attempts.get());
    }

    @Test
    void summaryRetryDoesNotRetryNonIoRuntimeFailures()
    {
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(IllegalArgumentException.class,
                     () -> SSTableTokenIndexBuilder.executeWithRetry(() -> {
                         attempts.incrementAndGet();
                         throw new IllegalArgumentException("bad summary metadata");
                     }, 3, 0L, 0L));

        assertEquals(1, attempts.get());
    }

    @Test
    void summaryRetryDoesNotRetryEofFailures()
    {
        // EOFException from Summary.db parsing is deterministic for the same byte range, so
        // it must not consume the retry budget.
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(EOFException.class,
                     () -> SSTableTokenIndexBuilder.executeWithRetry(() -> {
                         attempts.incrementAndGet();
                         throw new EOFException("truncated Summary.db");
                     }, 3, 0L, 0L));

        assertEquals(1, attempts.get());
    }

    @Test
    void summaryRetryDoesNotRetryEofWrappedInIoException()
    {
        // The bridge wraps the underlying parser failure; the unwrapping logic should still
        // detect the deterministic EOF and skip retries.
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(IOException.class,
                     () -> SSTableTokenIndexBuilder.executeWithRetry(() -> {
                         attempts.incrementAndGet();
                         throw new IOException("failed to read Summary.db", new EOFException("eof"));
                     }, 3, 0L, 0L));

        assertEquals(1, attempts.get());
    }

    private static final class RetryableSdkException extends SdkException
    {
        private RetryableSdkException(String message, Throwable cause)
        {
            super(SdkException.builder()
                              .message(message)
                              .cause(cause));
        }

        @Override
        public boolean retryable()
        {
            return true;
        }
    }

    private static SSTableTokenIndex indexFor(SSTableKey key, SSTableTokenBounds bounds)
    {
        Map<SSTableIndexKey, SSTableTokenBounds> map = new HashMap<>();
        map.put(SSTableIndexKey.from(key), bounds);
        return SSTableTokenIndex.fromShards(Collections.singletonList(new TokenIndexShard(map, 0, 0)));
    }

    private static SSTableKey key(String nodeId, String generationId)
    {
        return new SSTableKey(nodeId, "ks", "tbl", "table_id", generationId, "crc", "ma-" + generationId + "-big");
    }

    private static TokenRange range(long lower, long upper)
    {
        return TokenRange.closed(BigInteger.valueOf(lower), BigInteger.valueOf(upper));
    }

    private static <T extends Serializable> T roundTrip(T value) throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(value);
        }

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            @SuppressWarnings("unchecked")
            T roundTripped = (T) in.readObject();
            return roundTripped;
        }
    }
}
