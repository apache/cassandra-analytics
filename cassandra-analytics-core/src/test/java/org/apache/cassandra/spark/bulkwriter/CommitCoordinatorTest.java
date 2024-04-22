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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link CommitCoordinator}
 */
class CommitCoordinatorTest
{
    TokenRangeMapping<RingInstance> topology;
    MockBulkWriterContext context;
    TransportContext.DirectDataBulkWriterContext transportContext;

    @BeforeEach
    public void setup()
    {
        topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        context = new MockBulkWriterContext(topology);
        transportContext = (TransportContext.DirectDataBulkWriterContext) context.transportContext();
    }

    @Test
    void commitsForEachSuccessfulUpload() throws ExecutionException, InterruptedException
    {
        int successfulUploads = 3;
        DirectStreamResult uploadResult = DirectStreamResultBuilder
                                          .withTopology(topology)
                                          .withSuccessfulUploads(successfulUploads)
                                          .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResult))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            commitResults.forEach(cr -> {
                assertEquals(0, cr.failures.size());
                assertEquals(1, cr.passed.size());
            });
        }
    }

    @Test
    void commitWillNotCommitWhenUploadFailed() throws ExecutionException, InterruptedException
    {
        int successfulUploads = 1;
        int failedUploads = 2;
        DirectStreamResult uploadResult = DirectStreamResultBuilder
                                          .withTopology(topology)
                                          .withSuccessfulUploads(successfulUploads)
                                          .withFailedUploads(failedUploads)
                                          .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResult))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            CommitResult cr = commitResults.get(0);
            assertEquals(0, cr.failures.size());  // Failed uploads should not be committed at all
            assertEquals(successfulUploads, cr.passed.size());
        }
    }

    @Test
    void commitWillNotCommitWhenAlreadyCommitted() throws ExecutionException, InterruptedException
    {

        context.setCommitResultSupplier((uuids, dc) -> {
            throw new RuntimeException("Should not have called commit");
        });

        int successfulUploads = 3;
        int successfulCommits = 3;
        DirectStreamResult uploadResults = DirectStreamResultBuilder
                                           .withTopology(topology)
                                           .withSuccessfulUploads(successfulUploads)
                                           .withSuccessfulCommits(successfulCommits)
                                           .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResults))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            commitResults.forEach(cr -> {
                assertEquals(0, cr.failures.size());
                assertEquals(1, cr.passed.size());
            });
        }
    }

    @Test
    void commitWillReturnFailuresWhenCommitRequestFails() throws ExecutionException, InterruptedException
    {
        context.setCommitResultSupplier((uuids, dc) -> {
            throw new RuntimeException("Intentionally Failing Commit for uuids: " + Arrays.toString(uuids.toArray()));
        });
        int successfulUploads = 3;
        DirectStreamResult uploadResults = DirectStreamResultBuilder
                                           .withTopology(topology)
                                           .withSuccessfulUploads(successfulUploads)
                                           .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResults))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            commitResults.forEach(cr -> {
                assertEquals(1, cr.failures.size());
                assertEquals(0, cr.passed.size());
            });
        }
    }

    @Test
    void commitWillReturnFailuresWhenCommitFailsOnServerWithSpecificUuids() throws ExecutionException, InterruptedException
    {
        context.setCommitResultSupplier((uuids, dc) -> new DirectDataTransferApi.RemoteCommitResult(false, uuids, Collections.emptyList(), "Failed nodetool import"));
        int successfulUploads = 3;
        DirectStreamResult uploadResults = DirectStreamResultBuilder
                                           .withTopology(topology)
                                           .withSuccessfulUploads(successfulUploads)
                                           .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResults))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            commitResults.forEach(cr -> {
                assertEquals(1, cr.failures.size());
                assertEquals(0, cr.passed.size());
            });
        }
    }

    @Test
    void commitWillReturnFailuresWhenCommitFailsOnServerWithNoUuids() throws ExecutionException, InterruptedException
    {
        context.setCommitResultSupplier((uuids, dc) -> new DirectDataTransferApi.RemoteCommitResult(false, Collections.emptyList(), Collections.emptyList(), "Failed nodetool import"));
        int successfulUploads = 3;
        DirectStreamResult uploadResults = DirectStreamResultBuilder
                                           .withTopology(topology)
                                           .withSuccessfulUploads(successfulUploads)
                                           .build();
        try (CommitCoordinator coordinator = CommitCoordinator.commit(context, transportContext, uploadResults))
        {
            List<CommitResult> commitResults = coordinator.get();
            assertEquals(successfulUploads, commitResults.size());
            commitResults.forEach(cr -> {
                assertEquals(1, cr.failures.size());
                assertEquals(0, cr.passed.size());
            });
        }
    }

    static class DirectStreamResultBuilder
    {
        private static final Range<BigInteger> TEST_RANGE = Range.openClosed(BigInteger.valueOf(0), BigInteger.valueOf(200));

        private final TokenRangeMapping<RingInstance> topology;
        private int successfulUploads;
        private int failedUploads;
        private int successfulCommits;
        private int failedCommits;
        private RingInstance[] allInstances;

        DirectStreamResultBuilder(TokenRangeMapping<RingInstance> topology)
        {
            this.topology = topology;
        }

        static DirectStreamResultBuilder withTopology(TokenRangeMapping<RingInstance> topology)
        {
            return new DirectStreamResultBuilder(topology);
        }

        public DirectStreamResultBuilder withSuccessfulUploads(int successfulUploads)
        {
            this.successfulUploads = successfulUploads;
            return this;
        }

        public DirectStreamResultBuilder withFailedUploads(int failedUploads)
        {
            this.failedUploads = failedUploads;
            return this;
        }

        public DirectStreamResultBuilder withSuccessfulCommits(int successfulCommits)
        {
            this.successfulCommits = successfulCommits;
            return this;
        }

        public DirectStreamResultBuilder withFailedCommits(int failedCommits)
        {
            this.failedCommits = failedCommits;
            return this;
        }

        DirectStreamResult build()
        {
            allInstances = this.topology.getTokenRanges().keySet().toArray(new RingInstance[0]);
            DirectStreamResult sr = new DirectStreamResult(UUID.randomUUID().toString(),
                                                           TEST_RANGE,
                                                           buildFailures(),
                                                           buildPassed(), 0, 0);
            if (successfulCommits > 0 || failedCommits > 0)
            {
                List<CommitResult> commitResults = new ArrayList<>();
                for (RingInstance inst : this.topology.getTokenRanges().keySet())
                {
                    CommitResult cr = new CommitResult(
                    sr.sessionID,
                    inst,
                    ImmutableMap.of(sr.sessionID, this.topology.getTokenRanges().get(inst).stream().findFirst().get())
                    );
                    commitResults.add(cr);
                }
                sr.setCommitResults(commitResults);
            }
            return sr;
        }

        private List<RingInstance> buildPassed()
        {
            return new ArrayList<>(Arrays.asList(allInstances).subList(0, successfulUploads));
        }

        private ArrayList<StreamError> buildFailures()
        {
            ArrayList<StreamError> failedInstances = new ArrayList<>();
            for (int i = failedUploads - 1; i >= 0; i--)
            {
                failedInstances.add(new StreamError(TEST_RANGE, allInstances[i], "failed"));
            }
            return failedInstances;
        }
    }
}
