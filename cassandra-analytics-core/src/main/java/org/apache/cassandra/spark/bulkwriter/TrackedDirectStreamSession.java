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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;

/**
 * Stream session for bulk writes to keyspaces with mutation tracking enabled.
 *
 * <p>
 * Unlike bulk write to untracked keyspaces, for tracked keyspaces Cassandra's coordinates SSTable transfer between
 * nodes for bulk import. Import can be triggered on any one of the replica's of a token range and that replica acts as
 * a coordinator. {@link TrackedDirectStreamSession} spreads the coordinator pick across the eligible candidates of a
 * range so that outgoing connections are distributed across the nodes in Cassandra.
 * <p>
 * Write validation now moves to Cassandra in place of analytics checking consistency level. Resiliency to coordinator
 * failure mid-session is not handled here; it is handled at Spark's task-level retry.
 */
public class TrackedDirectStreamSession extends DirectStreamSession
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TrackedDirectStreamSession.class);
    private static final String WRITE_PHASE = "TrackedUploadAndCommit";

    public TrackedDirectStreamSession(BulkWriterContext writerContext,
                                      SortedSSTableWriter sstableWriter,
                                      TransportContext.DirectDataBulkWriterContext transportContext,
                                      String sessionID,
                                      Range<BigInteger> tokenRange,
                                      ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                      ExecutorService executorService)
    {
        super(writerContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler, executorService);
    }

    /**
     * Selects a single replica to act as the coordinator for this token range. The pick varies from range to range, so
     * that coordinator load is spread across replicas.
     */
    @Override
    List<RingInstance> getReplicas()
    {
        Set<RingInstance> failedInstances = failureHandler.getFailedInstances();
        List<RingInstance> candidates = tokenRangeMapping.getSubRanges(tokenRange)
                                                          .asMapOfRanges().values().stream()
                                                          .flatMap(Collection::stream)
                                                          .distinct()
                                                          .filter(instance -> !failedInstances.contains(instance))
                                                          // stable order so the range-based pick below is reproducible
                                                          .sorted(Comparator.comparing(RingInstance::nodeName))
                                                          .collect(Collectors.toList());

        Preconditions.checkState(!candidates.isEmpty(),
                                 "No eligible coordinator candidates found for range %s", tokenRange);

        RingInstance coordinator = pickCoordinator(candidates);
        LOGGER.info("[{}]: Selected {} as coordinator for tracked range {} out of {} candidates",
                    sessionID, coordinator.nodeName(), tokenRange, candidates.size());
        List<RingInstance> replicas = new ArrayList<>();
        replicas.add(coordinator);
        return replicas;
    }

    /**
     * Distributes the coordinator pick across candidates using the range's lower endpoint, so token ranges owned by
     * the same replica set don't all pick the same coordinator.
     */
    private RingInstance pickCoordinator(List<RingInstance> candidates)
    {
        int index = tokenRange.lowerEndpoint().mod(BigInteger.valueOf(candidates.size())).intValue();
        return candidates.get(index);
    }

    @Override
    protected StreamResult doFinalizeStream()
    {
        sendRemainingSSTables();
        DirectStreamResult streamResult = new DirectStreamResult(sessionID,
                                                                 tokenRange,
                                                                 errors,
                                                                 new ArrayList<>(replicas),
                                                                 sstableWriter.rowCount(),
                                                                 sstableWriter.bytesWritten());
        List<CommitResult> commitResults;
        try
        {
            commitResults = commit(streamResult);
        }
        catch (Exception exception)
        {
            if (exception instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(exception);
        }
        streamResult.setCommitResults(commitResults);
        LOGGER.debug("[{}]: Tracked StreamResult: {}", sessionID, streamResult);
        BulkWriteValidator.validateCoordinatorWriteSucceeded(tokenRange, errors, commitResults, LOGGER, WRITE_PHASE, writerContext.job());
        return streamResult;
    }
}
