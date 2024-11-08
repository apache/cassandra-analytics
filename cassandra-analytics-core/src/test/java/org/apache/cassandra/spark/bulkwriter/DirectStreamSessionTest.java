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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectStreamSessionTest
{
    public static final String LOAD_RANGE_ERROR_PREFIX = "Failed to write 1 ranges with LOCAL_QUORUM";
    private static final Map<String, Object> COLUMN_BOUND_VALUES = ImmutableMap.of("id", 0, "date", 1, "course", "course", "marks", 2);
    @TempDir
    private Path folder;
    private static final int FILES_PER_SSTABLE = 8;
    private static final int RF = 3;

    private MockBulkWriterContext writerContext;
    private TransportContext.DirectDataBulkWriterContext transportContext;
    private List<String> expectedInstances;
    private TokenRangeMapping<RingInstance> tokenRangeMapping;
    private MockScheduledExecutorService executor;
    private MockTableWriter tableWriter;
    private Range<BigInteger> range;
    private DigestAlgorithm digestAlgorithm;

    @BeforeEach
    public void setup()
    {
        digestAlgorithm = new XXHash32DigestAlgorithm();
        range = Range.range(BigInteger.valueOf(101L), BoundType.CLOSED, BigInteger.valueOf(199L), BoundType.CLOSED);
        ImmutableMap<String, Integer> rfOptions = ImmutableMap.of("DC1", 3);
        ReplicationFactor rf = new ReplicationFactor(NetworkTopologyStrategy, rfOptions);
        tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfOptions, 12);
        writerContext = getBulkWriterContext();
        writerContext.setReplicationFactor(rf);
        tableWriter = new MockTableWriter(folder);
        transportContext = (TransportContext.DirectDataBulkWriterContext) writerContext.transportContext();
        executor = new MockScheduledExecutorService();
        expectedInstances = Lists.newArrayList("DC1-i2", "DC1-i3", "DC1-i4");
    }

    @Test
    void testGetReplicasReturnsCorrectData()
    {
        StreamSession<?> streamSession = createStreamSession(SortedSSTableWriter::new);
        List<RingInstance> replicas = streamSession.getReplicas();
        assertNotNull(replicas);
        List<String> actualInstances = replicas.stream().map(RingInstance::nodeName).collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    void testScheduleStreamSendsCorrectFilesToCorrectInstances() throws IOException, ExecutionException, InterruptedException
    {
        StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        assertThat(ss.rowCount(), is(1L));
        StreamResult streamResult = ss.finalizeStreamAsync().get();
        assertThat(streamResult.rowCount, is(1L));
        executor.assertFuturesCalled();
        assertThat(executor.futures.size(), equalTo(1));  // We only scheduled one SSTable
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        final List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::nodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    void testEmptyTokenRangeFails()
    {
        Exception exception = assertThrows(IllegalStateException.class,
                                           () -> new DirectStreamSession(
                                           writerContext,
                                           new NonValidatingTestSortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
                                           transportContext,
                                           "sessionId",
                                           Range.range(BigInteger.valueOf(0L), BoundType.OPEN, BigInteger.valueOf(0L), BoundType.CLOSED),
                                           replicaAwareFailureHandler(), null)
                                           );
        assertThat(exception.getMessage(), is("No replicas found for range (0‥0]"));
    }

    @Test
    void testMismatchedTokenRangeFails() throws IOException
    {
        StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
        ss.addRow(BigInteger.valueOf(9999L), COLUMN_BOUND_VALUES);
        IllegalStateException illegalStateException = assertThrows(IllegalStateException.class,
                                                                   ss::finalizeStreamAsync);
        assertThat(illegalStateException.getMessage(), matchesPattern(
        "SSTable range \\[9999(‥|..)9999] should be enclosed in the partition range \\[101(‥|..)199]"));
    }

    @Test
    void testUploadFailureCallsClean() throws IOException, ExecutionException, InterruptedException
    {
        runFailedUpload();

        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::nodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    void testUploadFailureSkipsCleanWhenConfigured() throws IOException, ExecutionException, InterruptedException
    {
        writerContext.setSkipCleanOnFailures(true);
        runFailedUpload();

        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::nodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, iterableWithSize(0));
        final List<UploadRequest> uploads = writerContext.getUploads().values()
                                                         .stream()
                                                         .flatMap(Collection::stream)
                                                         .collect(Collectors.toList());
        assertFalse(uploads.isEmpty());
        assertTrue(uploads.stream().noneMatch(u -> u.uploadSucceeded));
    }

    @Test
    void testUploadFailureRefreshesClusterInfo() throws IOException, ExecutionException, InterruptedException
    {
        runFailedUpload();
        assertThat(writerContext.refreshClusterInfoCallCount(), equalTo(3));
    }

    @Test
    void testOutDirCreationFailureCleansAllReplicas()
    {
        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
            ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
            Future<?> fut = ss.finalizeStreamAsync();
            tableWriter.removeOutDir();
            fut.get();
        });

        Assertions.assertThat(ex).hasRootCauseInstanceOf(NoSuchFileException.class);
        List<String> actualInstances = writerContext.getCleanedInstances().stream()
                                                    .map(CassandraInstance::nodeName)
                                                    .collect(Collectors.toList());
        assertThat(actualInstances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    void failedCleanDoesNotThrow() throws IOException
    {
        writerContext.setCleanShouldThrow(true);
        runFailedUpload();
    }

    @Test
    void testLocalQuorumSucceedsWhenSingleCommitFails() throws IOException, ExecutionException, InterruptedException
    {
        StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
        AtomicBoolean success = new AtomicBoolean(true);
        writerContext.setCommitResultSupplier((uuids, dc) -> {
            // Return failed result for 1st result, success for the rest
            if (success.getAndSet(false))
            {
                return new DirectDataTransferApi.RemoteCommitResult(false, uuids, null, "");
            }
            else
            {
                return new DirectDataTransferApi.RemoteCommitResult(true, null, uuids, "");
            }
        });
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        ss.finalizeStreamAsync().get();
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        final List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::nodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    @Test
    void testLocalQuorumFailsWhenCommitsFail() throws IOException
    {
        StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
        AtomicBoolean success = new AtomicBoolean(true);
        // Return successful result for 1st result, failed for the rest
        writerContext.setCommitResultSupplier((uuids, dc) -> {
            if (success.getAndSet(false))
            {
                return new DirectDataTransferApi.RemoteCommitResult(true, null, uuids, "");
            }
            else
            {
                return new DirectDataTransferApi.RemoteCommitResult(false, uuids, null, "");
            }
        });
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        ExecutionException exception = assertThrows(ExecutionException.class,
                                                    () -> ss.finalizeStreamAsync().get());
        Assertions.assertThat(exception)
                  .hasCauseExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
                  .hasMessageContaining("Failed to write 1 ranges with LOCAL_QUORUM for job " + writerContext.job().getId()
                                        + " in phase UploadAndCommit.");
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum(), equalTo(RF * FILES_PER_SSTABLE));
        List<String> instances = writerContext.getUploads().keySet().stream().map(CassandraInstance::nodeName).collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(expectedInstances.toArray()));
    }

    private void runFailedUpload() throws IOException
    {
        writerContext.setUploadSupplier(instance -> false);
        StreamSession<?> ss = createStreamSession(NonValidatingTestSortedSSTableWriter::new);
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        ExecutionException ex = assertThrows(ExecutionException.class,
                                             () -> ss.finalizeStreamAsync().get());
        assertThat(ex.getCause().getMessage(), startsWith(LOAD_RANGE_ERROR_PREFIX));
    }

    @NotNull
    private MockBulkWriterContext getBulkWriterContext()
    {
        return new MockBulkWriterContext(tokenRangeMapping);
    }

    @NotNull
    private ReplicaAwareFailureHandler<RingInstance> replicaAwareFailureHandler()
    {
        return new MultiClusterReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
    }

    private DirectStreamSession createStreamSession(MockTableWriter.Creator writerCreator)
    {
        return new DirectStreamSession(writerContext,
                                       writerCreator.create(tableWriter, folder, digestAlgorithm, 1),
                                       transportContext,
                                       "sessionId",
                                       range,
                                       replicaAwareFailureHandler(),
                                       executor);
    }
}
