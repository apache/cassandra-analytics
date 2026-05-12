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
import java.io.EOFException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.data.backup.BackupReader;
import org.apache.cassandra.spark.data.backup.BackupReaderConfig;
import org.apache.cassandra.spark.data.backup.BackupReaderFactory;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.exception.SdkException;

public final class SSTableTokenIndexBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableTokenIndexBuilder.class);
    private static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
    private static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;
    // Retry budget for Summary.db prebuild reads. Tuned for S3/KMS throttling, which is the
    // dominant failure mode under high prebuild fan-out: KMS rate-limit windows tend to recover
    // on the order of seconds, so an initial 500 ms (jittered) and a 5 s cap give two meaningful
    // re-reads beyond the SDK's own retries without pinning per-task threads for too long.
    private static final int SUMMARY_READ_MAX_ATTEMPTS = 3;
    private static final long SUMMARY_READ_INITIAL_BACKOFF_MILLIS = 500L;
    private static final long SUMMARY_READ_MAX_BACKOFF_MILLIS = 5_000L;
    private static final int DETAILED_FAILURE_LOG_LIMIT = 5;
    private static final int FAILURE_LOG_INTERVAL = 10_000;

    private SSTableTokenIndexBuilder()
    {
    }

    public static TokenIndexShard buildShard(Iterator<SSTableSummaryWorkItem> workItems,
                                             BackupReaderFactory backupReaderFactory,
                                             BackupReaderConfig backupReaderConfig,
                                             String clusterName,
                                             String datacenter,
                                             CassandraVersion cassandraVersion,
                                             int concurrency)
    {
        BackupReader reader = backupReaderFactory.create(backupReaderConfig);
        CassandraBridge bridge = CassandraBridgeFactory.get(cassandraVersion);
        int maxInFlight = Math.max(1, concurrency);
        ExecutorService executor = Executors.newFixedThreadPool(maxInFlight);
        ExecutorCompletionService<TokenIndexRecord> completionService = new ExecutorCompletionService<>(executor);
        int inFlight = 0;
        FailureReporter failureReporter = new FailureReporter(clusterName, datacenter);
        try
        {
            Map<SSTableIndexKey, SSTableTokenBounds> boundsBySSTable = new HashMap<>();
            int missingCount = 0;
            int errorCount = 0;
            while (workItems.hasNext() || inFlight > 0)
            {
                while (workItems.hasNext() && inFlight < maxInFlight)
                {
                    SSTableSummaryWorkItem workItem = workItems.next();
                    completionService.submit(scanWorkItem(reader, bridge, clusterName, datacenter, workItem, failureReporter));
                    inFlight++;
                }

                TokenIndexRecord record = take(completionService, failureReporter);
                inFlight--;
                if (record.bounds == null)
                {
                    if (record.missing)
                    {
                        missingCount++;
                    }
                    else
                    {
                        errorCount++;
                    }
                }
                else
                {
                    boundsBySSTable.put(record.key, record.bounds);
                }
            }
            failureReporter.logShardSummary(boundsBySSTable.size(), missingCount, errorCount);
            return new TokenIndexShard(boundsBySSTable, missingCount, errorCount);
        }
        finally
        {
            executor.shutdownNow();
            reader.close();
        }
    }

    private static Callable<TokenIndexRecord> scanWorkItem(BackupReader reader,
                                                           CassandraBridge bridge,
                                                           String clusterName,
                                                           String datacenter,
                                                           SSTableSummaryWorkItem workItem,
                                                           FailureReporter failureReporter)
    {
        return () -> {
            if (!workItem.componentSizes().containsKey(FileType.SUMMARY))
            {
                return TokenIndexRecord.missing(workItem.indexKey());
            }
            try
            {
                SummaryOnlySSTable ssTable = new SummaryOnlySSTable(reader, clusterName, datacenter, workItem);
                SSTableSummary summary = executeWithRetry(() -> bridge.getSSTableSummary(Partitioner.Murmur3Partitioner,
                                                                                        ssTable,
                                                                                        DEFAULT_MIN_INDEX_INTERVAL,
                                                                                        DEFAULT_MAX_INDEX_INTERVAL),
                                                          SUMMARY_READ_MAX_ATTEMPTS,
                                                          SUMMARY_READ_INITIAL_BACKOFF_MILLIS,
                                                          SUMMARY_READ_MAX_BACKOFF_MILLIS);
                return TokenIndexRecord.success(workItem.indexKey(),
                                                new SSTableTokenBounds(toLong(summary.firstToken),
                                                                       toLong(summary.lastToken)));
            }
            catch (Exception exception)
            {
                failureReporter.record(workItem, exception);
                return TokenIndexRecord.error(workItem.indexKey());
            }
        };
    }

    private static TokenIndexRecord take(ExecutorCompletionService<TokenIndexRecord> completionService,
                                         FailureReporter failureReporter)
    {
        try
        {
            Future<TokenIndexRecord> future = completionService.take();
            return future.get();
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while building SSTable token index shard", exception);
        }
        catch (ExecutionException exception)
        {
            failureReporter.record(null, exception);
            return TokenIndexRecord.error(null);
        }
    }

    /**
     * Retry helper used by the prebuild path. Doubles an unjittered "schedule" on each failure
     * and sleeps for a full-jitter sample uniformly in {@code [0, schedule]}. Doing the
     * doubling on the unjittered base preserves an exponential ceiling; sleeping on the
     * jittered sample spreads the herd of in-flight workers across the throttling window.
     * See AWS SDK v2 {@code BackoffStrategy.fullJitter} for the same approach.
     */
    static <T> T executeWithRetry(Callable<T> action,
                                  int maxAttempts,
                                  long initialBackoffMillis,
                                  long maxBackoffMillis) throws Exception
    {
        int attempts = Math.max(1, maxAttempts);
        long scheduleMillis = Math.max(0L, initialBackoffMillis);
        long ceilingMillis = Math.max(scheduleMillis, Math.max(0L, maxBackoffMillis));
        Exception lastException = null;
        for (int attempt = 1; attempt <= attempts; attempt++)
        {
            try
            {
                return action.call();
            }
            catch (Exception exception)
            {
                lastException = exception;
                if (attempt == attempts || !isRetryableSummaryFailure(exception))
                {
                    throw exception;
                }
                try
                {
                    sleep(jitter(scheduleMillis));
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    interrupted.addSuppressed(exception);
                    throw interrupted;
                }
                // Advance the unjittered schedule so the next sleep window keeps growing
                // even when this iteration sampled near zero.
                scheduleMillis = Math.min(Math.max(scheduleMillis * 2L, 1L), ceilingMillis);
            }
        }
        // Math.max(1, maxAttempts) above guarantees at least one iteration that assigns
        // lastException before throwing, so this branch is unreachable in practice.
        throw lastException == null ? new IllegalStateException("executeWithRetry exited without throwing or returning")
                                    : lastException;
    }

    private static long jitter(long scheduleMillis)
    {
        if (scheduleMillis <= 0L)
        {
            return 0L;
        }
        // Full jitter: uniform in [0, scheduleMillis]. Spreads simultaneous failures across
        // the throttling recovery window instead of issuing all retries at the same instant.
        return ThreadLocalRandom.current().nextLong(scheduleMillis + 1L);
    }

    private static void sleep(long backoffMillis) throws InterruptedException
    {
        if (backoffMillis > 0L)
        {
            Thread.sleep(backoffMillis);
        }
    }

    private static boolean isRetryableSummaryFailure(Throwable throwable)
    {
        // Keep this allow-list narrow. Summary parser failures (including EOF) are deterministic
        // for a resolved object/range and retrying them only adds S3/KMS load. Let the AWS SDK mark
        // service-side failures retryable, and only add local network socket failures here.
        if (containsCause(throwable, EOFException.class))
        {
            return false;
        }
        return containsRetryableSdkException(throwable)
               || containsCause(throwable, SocketException.class)
               || containsCause(throwable, SocketTimeoutException.class);
    }

    private static boolean containsRetryableSdkException(Throwable throwable)
    {
        Throwable current = unwrapCompletion(throwable);
        while (current.getCause() != null && current.getCause() != current)
        {
            if (current instanceof SdkException && ((SdkException) current).retryable())
            {
                return true;
            }
            current = unwrapCompletion(current.getCause());
        }
        return current instanceof SdkException && ((SdkException) current).retryable();
    }

    private static boolean containsCause(Throwable throwable, Class<? extends Throwable> causeClass)
    {
        Throwable current = unwrapCompletion(throwable);
        while (current.getCause() != null && current.getCause() != current)
        {
            if (causeClass.isInstance(current))
            {
                return true;
            }
            current = unwrapCompletion(current.getCause());
        }
        return causeClass.isInstance(current);
    }

    private static Throwable rootCause(Throwable throwable)
    {
        Throwable current = unwrapCompletion(throwable);
        while (current.getCause() != null && current.getCause() != current)
        {
            current = unwrapCompletion(current.getCause());
        }
        return current;
    }

    private static Throwable unwrapCompletion(Throwable throwable)
    {
        if ((throwable instanceof CompletionException || throwable instanceof ExecutionException) && throwable.getCause() != null)
        {
            return throwable.getCause();
        }
        return throwable;
    }

    private static long toLong(BigInteger token)
    {
        return token.longValue();
    }

    private static final class FailureReporter
    {
        private final String clusterName;
        private final String datacenter;
        private final AtomicInteger failureCount = new AtomicInteger();
        private final ConcurrentMap<String, AtomicInteger> failuresByException = new ConcurrentHashMap<>();

        private FailureReporter(String clusterName, String datacenter)
        {
            this.clusterName = clusterName;
            this.datacenter = datacenter;
        }

        private void record(@Nullable SSTableSummaryWorkItem workItem, Throwable throwable)
        {
            Throwable rootCause = rootCause(throwable);
            String exceptionClass = rootCause.getClass().getName();
            failuresByException.computeIfAbsent(exceptionClass, key -> new AtomicInteger()).incrementAndGet();

            int count = failureCount.incrementAndGet();
            if (count <= DETAILED_FAILURE_LOG_LIMIT)
            {
                LOGGER.warn("Failed to prebuild SSTable token index from Summary.db "
                            + "cluster={} datacenter={} failureCount={} sstableKey={} token={} "
                            + "summarySizeBytes={} exceptionClass={} message={}",
                            clusterName,
                            datacenter,
                            count,
                            workItem == null ? null : workItem.sstableKey(),
                            workItem == null ? null : workItem.token(),
                            workItem == null ? null : workItem.componentSizes().get(FileType.SUMMARY),
                            exceptionClass,
                            rootCause.getMessage(),
                            rootCause);
            }
            else if (count % FAILURE_LOG_INTERVAL == 0)
            {
                LOGGER.warn("Failed to prebuild SSTable token index from Summary.db "
                            + "cluster={} datacenter={} failureCount={} sstableKey={} token={} "
                            + "summarySizeBytes={} exceptionClass={} message={}",
                            clusterName,
                            datacenter,
                            count,
                            workItem == null ? null : workItem.sstableKey(),
                            workItem == null ? null : workItem.token(),
                            workItem == null ? null : workItem.componentSizes().get(FileType.SUMMARY),
                            exceptionClass,
                            rootCause.getMessage());
            }
        }

        private void logShardSummary(int successCount, int missingCount, int errorCount)
        {
            if (errorCount > 0)
            {
                LOGGER.warn("Completed SSTable token index shard with fail-open Summary.db errors "
                            + "cluster={} datacenter={} indexed={} missing={} errors={} failureTypes={}",
                            clusterName,
                            datacenter,
                            successCount,
                            missingCount,
                            errorCount,
                            failureCounts());
            }
        }

        private Map<String, Integer> failureCounts()
        {
            Map<String, Integer> counts = new HashMap<>();
            failuresByException.forEach((exceptionClass, count) -> counts.put(exceptionClass, count.get()));
            return Collections.unmodifiableMap(counts);
        }
    }

    private static final class TokenIndexRecord
    {
        @Nullable
        private final SSTableIndexKey key;
        @Nullable
        private final SSTableTokenBounds bounds;
        private final boolean missing;

        private TokenIndexRecord(@Nullable SSTableIndexKey key, @Nullable SSTableTokenBounds bounds, boolean missing)
        {
            this.key = key;
            this.bounds = bounds;
            this.missing = missing;
        }

        private static TokenIndexRecord success(SSTableIndexKey key, SSTableTokenBounds bounds)
        {
            return new TokenIndexRecord(key, bounds, false);
        }

        private static TokenIndexRecord missing(SSTableIndexKey key)
        {
            return new TokenIndexRecord(key, null, true);
        }

        private static TokenIndexRecord error(@Nullable SSTableIndexKey key)
        {
            return new TokenIndexRecord(key, null, false);
        }
    }

    private static final class SummaryOnlySSTable extends SSTable
    {
        private final BackupReader reader;
        private final String clusterName;
        private final String datacenter;
        private final SSTableSummaryWorkItem workItem;
        private final ConcurrentMap<FileType, Long> actualSizes = new ConcurrentHashMap<>();

        private SummaryOnlySSTable(BackupReader reader,
                                   String clusterName,
                                   String datacenter,
                                   SSTableSummaryWorkItem workItem)
        {
            this.reader = reader;
            this.clusterName = clusterName;
            this.datacenter = datacenter;
            this.workItem = workItem;
        }

        @Nullable
        @Override
        protected InputStream openInputStream(FileType fileType)
        {
            Long size = workItem.componentSizes().get(fileType);
            if (size == null || size <= 0)
            {
                return null;
            }
            byte[] bytes = reader.readMutableMetadataAsync(clusterName,
                                                           datacenter,
                                                           workItem.token(),
                                                           workItem.sstableKey(),
                                                           fileType,
                                                           size,
                                                           Stats.DoNothingStats.INSTANCE).join();
            actualSizes.put(fileType, (long) bytes.length);
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public long length(FileType fileType)
        {
            Long actualSize = actualSizes.get(fileType);
            if (actualSize != null)
            {
                return actualSize;
            }
            Long size = workItem.componentSizes().get(fileType);
            if (size == null)
            {
                throw new IncompleteSSTableException(fileType);
            }
            return size;
        }

        @Override
        public boolean isMissing(FileType fileType)
        {
            return !workItem.componentSizes().containsKey(fileType);
        }

        @Override
        public String getDataFileName()
        {
            return workItem.sstableKey().getDataFileName();
        }
    }
}
