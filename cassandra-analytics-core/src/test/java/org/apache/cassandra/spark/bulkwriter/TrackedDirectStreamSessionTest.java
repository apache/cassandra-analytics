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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link TrackedDirectStreamSession}, which streams to a single coordinator replica per token range
 * instead of every write replica (as {@link DirectStreamSession} does for untracked keyspaces).
 */
public class TrackedDirectStreamSessionTest
{
    private static final Map<String, Object> COLUMN_BOUND_VALUES = ImmutableMap.of("id", 0, "date", 1, "course", "course", "marks", 2);
    private static final int FILES_PER_SSTABLE = 8;

    @TempDir
    private Path folder;

    private MockBulkWriterContext writerContext;
    private TransportContext.DirectDataBulkWriterContext transportContext;
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
        writerContext = new MockBulkWriterContext(tokenRangeMapping);
        writerContext.setReplicationFactor(rf);
        writerContext.setTrackedKeyspace(true);
        tableWriter = new MockTableWriter(folder);
        transportContext = (TransportContext.DirectDataBulkWriterContext) writerContext.transportContext();
        executor = new MockScheduledExecutorService();
    }

    @Test
    void testGetReplicasReturnsExactlyOneCandidate()
    {
        StreamSession<?> streamSession = createStreamSession();
        List<RingInstance> replicas = streamSession.getReplicas();
        assertThat(replicas)
        .describedAs("Tracked stream session should pick a single coordinator, not every write replica")
        .hasSize(1);
        assertThat(replicas.get(0).nodeName())
        .describedAs("Coordinator must be one of the range's write replicas")
        .isIn("DC1-i2", "DC1-i3", "DC1-i4");
    }

    @Test
    void testCoordinatorSelectionIsDeterministicForSameRange()
    {
        RingInstance first = createStreamSession().getReplicas().get(0);
        RingInstance second = createStreamSession().getReplicas().get(0);
        assertThat(first)
        .describedAs("Coordinator pick should be stable across sessions for the same range and failure state")
        .isEqualTo(second);
    }

    @Test
    void testCoordinatorSelectionSpreadsAcrossRanges()
    {
        // Build a larger ring so different sub-ranges are owned by different replica sets,
        // giving the range-based coordinator pick room to land on more than one node.
        ImmutableMap<String, Integer> rfOptions = ImmutableMap.of("DC1", 3);
        TokenRangeMapping<RingInstance> largeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfOptions, 12);
        MockBulkWriterContext largeContext = new MockBulkWriterContext(largeMapping);
        largeContext.setReplicationFactor(new ReplicationFactor(NetworkTopologyStrategy, rfOptions));
        largeContext.setTrackedKeyspace(true);
        TransportContext.DirectDataBulkWriterContext largeTransportContext =
        (TransportContext.DirectDataBulkWriterContext) largeContext.transportContext();

        Set<String> coordinatorsPicked = new HashSet<>();
        for (Range<BigInteger> subRange : largeMapping.getRangeMap().asMapOfRanges().keySet())
        {
            TrackedDirectStreamSession session = new TrackedDirectStreamSession(
            largeContext,
            new SortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
            largeTransportContext,
            "session-" + subRange,
            subRange,
            new MultiClusterReplicaAwareFailureHandler<>(largeContext.cluster().getPartitioner()),
            executor);
            coordinatorsPicked.add(session.getReplicas().get(0).nodeName());
        }

        assertThat(coordinatorsPicked)
        .describedAs("Coordinator pick should be spread across more than one candidate across different ranges")
        .hasSizeGreaterThan(1);
    }

    @Test
    void testGetReplicasExcludesFailedInstances()
    {
        ReplicaAwareFailureHandler<RingInstance> failureHandler = replicaAwareFailureHandler();
        StreamSession<?> streamSession = new TrackedDirectStreamSession(
        writerContext,
        new SortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
        transportContext,
        "sessionId",
        range,
        failureHandler,
        executor);

        RingInstance firstCoordinator = streamSession.getReplicas().get(0);
        failureHandler.addFailure(range, firstCoordinator, "simulated failure");

        StreamSession<?> retrySession = new TrackedDirectStreamSession(
        writerContext,
        new SortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
        transportContext,
        "sessionId-retry",
        range,
        failureHandler,
        executor);

        assertThat(retrySession.getReplicas())
        .describedAs("A failed coordinator must not be picked again on retry")
        .doesNotContain(firstCoordinator);
    }

    @Test
    void testScheduleStreamSendsFilesOnlyToCoordinator() throws IOException, ExecutionException, InterruptedException
    {
        StreamSession<?> ss = createNonValidatingStreamSession();
        RingInstance coordinator = ss.getReplicas().get(0);

        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);
        assertThat(ss.rowCount()).isEqualTo(1L);
        StreamResult streamResult = ss.finalizeStreamAsync().get();

        assertThat(streamResult.rowCount).isEqualTo(1L);
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().keySet())
        .describedAs("Only the coordinator should receive uploads")
        .containsExactly(coordinator);
        assertThat(writerContext.getUploads().values().stream().mapToInt(Collection::size).sum())
        .isEqualTo(FILES_PER_SSTABLE);
    }

    @Test
    void testCommitFailureThrowsConsistencyNotSatisfied() throws IOException
    {
        writerContext.setCommitResultSupplier((uuids, dc) ->
        new DirectDataTransferApi.RemoteCommitResult(false, uuids, null, "commit rejected"));

        StreamSession<?> ss = createNonValidatingStreamSession();
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);

        assertThatThrownBy(() -> ss.finalizeStreamAsync().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseExactlyInstanceOf(ConsistencyNotSatisfiedException.class)
        .hasMessageContaining("Failed to write tracked keyspace range " + range);
    }

    @Test
    void testUploadFailureThrowsConsistencyNotSatisfied() throws IOException
    {
        writerContext.setUploadSupplier(instance -> false);
        StreamSession<?> ss = createNonValidatingStreamSession();
        ss.addRow(BigInteger.valueOf(102L), COLUMN_BOUND_VALUES);

        assertThatThrownBy(() -> ss.finalizeStreamAsync().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(ConsistencyNotSatisfiedException.class);
    }

    @NotNull
    private ReplicaAwareFailureHandler<RingInstance> replicaAwareFailureHandler()
    {
        return new MultiClusterReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
    }

    private TrackedDirectStreamSession createStreamSession()
    {
        return new TrackedDirectStreamSession(writerContext,
                                              new SortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
                                              transportContext,
                                              "sessionId",
                                              range,
                                              replicaAwareFailureHandler(),
                                              executor);
    }

    private TrackedDirectStreamSession createNonValidatingStreamSession()
    {
        return new TrackedDirectStreamSession(writerContext,
                                              new NonValidatingTestSortedSSTableWriter(tableWriter, folder, digestAlgorithm, 1),
                                              transportContext,
                                              "sessionId",
                                              range,
                                              replicaAwareFailureHandler(),
                                              executor);
    }
}
