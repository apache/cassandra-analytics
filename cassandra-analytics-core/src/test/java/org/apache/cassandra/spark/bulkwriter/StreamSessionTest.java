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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.jetbrains.annotations.NotNull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamSessionTest
{
    public static final String LOAD_RANGE_ERROR_PREFIX = "Failed to load 1 ranges with LOCAL_QUORUM";
    private static final Map<String, Object> COLUMN_BOUND_VALUES = ImmutableMap.of("id", 0, "date", 1, "course", "course", "marks", 2);
    @TempDir
    public Path folder; // CHECKSTYLE IGNORE: Public mutable field for testing
    private static final int FILES_PER_SSTABLE = 8;
    private static final int RF = 3;

    private StreamSession ss;
    private MockBulkWriterContext writerContext;
    private List<String> expectedInstances;
    private TokenRangeMapping<RingInstance> tokenRangeMapping;
    private MockScheduledExecutorService executor;
    private MockTableWriter tableWriter;
    private Range<BigInteger> range;

    @BeforeEach
    public void setup()
    {
        range = Range.range(BigInteger.valueOf(101L), BoundType.CLOSED, BigInteger.valueOf(199L), BoundType.CLOSED);
        tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 12);
        writerContext = getBulkWriterContext();
        tableWriter = new MockTableWriter(folder);
        executor = new MockScheduledExecutorService();
        ss = new StreamSession(writerContext, "sessionId", range, executor, new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
        expectedInstances = Lists.newArrayList("DC1-i1", "DC1-i2", "DC1-i3");
    }

    @Test
    public void testGetReplicasReturnsCorrectData()
    {
        List<RingInstance> replicas = ss.getReplicas();
        assertNotNull(replicas);
        List<String> actualInstances = replicas.stream().map(RingInstance::getNodeName).collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    public void testScheduleStreamSendsCorrectFilesToCorrectInstances() throws IOException, ExecutionException, InterruptedException
    {
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        tr.close(writerContext, 1);
        ss.scheduleStream(tr);
        ss.close();  // Force "execution" of futures
        executor.assertFuturesCalled();
        assertThat(executor.futures.size(), equalTo(1));  // We only scheduled one SSTable
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        final List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::getNodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    public void testMismatchedTokenRangeFails() throws IOException
    {
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(9999L), COLUMN_BOUND_VALUES);
        tr.close(writerContext, 1);
        IllegalStateException illegalStateException = assertThrows(IllegalStateException.class,
                                                                   () -> ss.scheduleStream(tr));
        assertThat(illegalStateException.getMessage(), matchesPattern(
        "SSTable range \\[9999(‥|..)9999] should be enclosed in the partition range \\[101(‥|..)199]"));
    }

    @Test
    public void testUploadFailureCallsClean() throws IOException, ExecutionException, InterruptedException
    {
        runFailedUpload();

        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::getNodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    public void testUploadFailureSkipsCleanWhenConfigured() throws IOException, ExecutionException, InterruptedException
    {
        writerContext.setSkipCleanOnFailures(true);
        runFailedUpload();

        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::getNodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, iterableWithSize(0));
        final List<UploadRequest> uploads = writerContext.getUploads().values()
                                                         .stream()
                                                         .flatMap(Collection::stream)
                                                         .collect(Collectors.toList());
        assertTrue(uploads.size() > 0);
        assertTrue(uploads.stream().noneMatch(u -> u.uploadSucceeded));
    }

    @Test
    public void testUploadFailureRefreshesClusterInfo() throws IOException, ExecutionException, InterruptedException
    {
        runFailedUpload();
        assertThat(writerContext.refreshClusterInfoCallCount(), equalTo(3));
    }

    @Test
    public void testOutDirCreationFailureCleansAllReplicas()
    {
        assertThrows(RuntimeException.class, () -> {
            SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, tableWriter.getOutDir());
            tr.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
            tr.close(writerContext, 1);
            tableWriter.removeOutDir();
            ss.scheduleStream(tr);
            ss.close();
        });

        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::getNodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    public void streamWithNoWritersReturnsEmptyStreamResult() throws ExecutionException, InterruptedException
    {
        writerContext.setInstancesAreAvailable(false);
        ss = new StreamSession(writerContext, "sessionId", range, executor, new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
        StreamResult result = ss.close();
        assertThat(result.failures.size(), equalTo(0));
        assertThat(result.passed.size(), equalTo(0));
        assertThat(result.sessionID, equalTo("sessionId"));
        assertThat(result.tokenRange, equalTo(range));
    }

    @Test
    public void failedCleanDoesNotThrow() throws IOException, ExecutionException, InterruptedException
    {
        writerContext.setCleanShouldThrow(true);
        runFailedUpload();
    }

    @Test
    public void testLocalQuorumSucceedsWhenSingleCommitFails(
    ) throws IOException, ExecutionException, InterruptedException
    {
        ss = new StreamSession(writerContext, "sessionId", range, executor, new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
        AtomicBoolean success = new AtomicBoolean(true);
        writerContext.setCommitResultSupplier((uuids, dc) -> {
            // Return failed result for 1st result, success for the rest
            if (success.getAndSet(false))
            {
                return new DataTransferApi.RemoteCommitResult(false, uuids, null, "");
            }
            else
            {
                return new DataTransferApi.RemoteCommitResult(true, null, uuids, "");
            }
        });
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        tr.close(writerContext, 1);
        ss.scheduleStream(tr);
        ss.close();  // Force "execution" of futures
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        final List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::getNodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    public void testLocalQuorumFailsWhenCommitsFail() throws IOException, ExecutionException, InterruptedException
    {
        ss = new StreamSession(writerContext, "sessionId", range, executor, new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
        AtomicBoolean success = new AtomicBoolean(true);
        // Return successful result for 1st result, failed for the rest
        writerContext.setCommitResultSupplier((uuids, dc) -> {
            if (success.getAndSet(false))
            {
                return new DataTransferApi.RemoteCommitResult(true, null, uuids, "");
            }
            else
            {
                return new DataTransferApi.RemoteCommitResult(false, uuids, null, "");
            }
        });

        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        tr.close(writerContext, 1);
        ss.scheduleStream(tr);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> ss.close());  // Force "execution" of futures
        assertEquals("Failed to load 1 ranges with LOCAL_QUORUM for job " + writerContext.job().getId()
                     + " in phase UploadAndCommit.", exception.getMessage());
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::getNodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    private void runFailedUpload() throws IOException, ExecutionException, InterruptedException
    {
        writerContext.setUploadSupplier(instance -> false);
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        tr.close(writerContext, 1);
        ss.scheduleStream(tr);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> ss.close());
        assertThat(ex.getMessage(), startsWith(LOAD_RANGE_ERROR_PREFIX));
    }

    @NotNull
    private MockBulkWriterContext getBulkWriterContext()
    {
        return new MockBulkWriterContext(tokenRangeMapping);
    }
}
