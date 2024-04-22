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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;

public abstract class StreamSession<T extends TransportContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSession.class);

    protected final BulkWriterContext writerContext;
    protected final T transportContext;
    protected final String sessionID;
    protected final Range<BigInteger> tokenRange;
    protected final List<RingInstance> replicas;
    protected final ArrayList<StreamError> errors = new ArrayList<>();
    protected final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    protected final TokenRangeMapping<RingInstance> tokenRangeMapping;
    protected final SortedSSTableWriter sstableWriter;

    @VisibleForTesting
    protected StreamSession(BulkWriterContext writerContext,
                            SortedSSTableWriter sstableWriter,
                            T transportContext,
                            String sessionID,
                            Range<BigInteger> tokenRange,
                            ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        this.writerContext = writerContext;
        this.sstableWriter = sstableWriter;
        this.transportContext = transportContext;
        this.tokenRangeMapping = writerContext.cluster().getTokenRangeMapping(true);
        this.sessionID = sessionID;
        this.tokenRange = tokenRange;
        this.failureHandler = failureHandler;
        this.replicas = getReplicas();
    }

    public Range<BigInteger> getTokenRange()
    {
        return tokenRange;
    }

    public void addRow(BigInteger token, Map<String, Object> boundValues) throws IOException
    {
        sstableWriter.addRow(token, boundValues);
    }

    public long rowCount()
    {
        return sstableWriter.rowCount();
    }

    public Future<StreamResult> scheduleStreamAsync(int partitionId, ExecutorService executorService) throws IOException
    {
        Preconditions.checkState(!sstableWriter.getTokenRange().isEmpty(), "Trying to stream empty SSTable");
        Preconditions.checkState(tokenRange.encloses(sstableWriter.getTokenRange()),
                                 "SSTable range %s should be enclosed in the partition range %s",
                                 sstableWriter.getTokenRange(), tokenRange);
        sstableWriter.close(writerContext, partitionId);
        return executorService.submit(() -> doScheduleStream(sstableWriter));
    }

    @VisibleForTesting
    List<RingInstance> getReplicas()
    {
        Set<RingInstance> failedInstances = failureHandler.getFailedInstances();
        Set<String> blockedInstances = tokenRangeMapping.getBlockedInstances();
        // Get ranges that intersect with the partition's token range
        Map<Range<BigInteger>, List<RingInstance>> overlappingRanges = tokenRangeMapping.getSubRanges(tokenRange).asMapOfRanges();

        LOGGER.debug("[{}]: Stream session token range: {} overlaps with ring ranges: {}",
                     sessionID, tokenRange, overlappingRanges);

        List<RingInstance> replicasForTokenRange = overlappingRanges.values().stream()
                                                                    .flatMap(Collection::stream)
                                                                    .distinct()
                                                                    .filter(instance -> !isExclusion(instance,
                                                                                                     failedInstances,
                                                                                                     blockedInstances))
                                                                    .collect(Collectors.toList());

        Preconditions.checkState(!replicasForTokenRange.isEmpty(),
                                 "No replicas found for range %s", tokenRange);

        // In order to better utilize replicas, shuffle the replicaList so each session starts writing to a different replica first.
        Collections.shuffle(replicasForTokenRange);
        return replicasForTokenRange;
    }

    /**
     * Evaluates if the given instance should be excluded from writes by checking if it is either blocked or
     * has a failure
     *
     * @param ringInstance the instance being evaluated
     * @param failedInstances set of failed instances
     * @param blockedInstanceIps set of IP addresses of blocked instances
     * @return true if the provided instance is either a failed or blocked instance
     */
    private boolean isExclusion(RingInstance ringInstance, Set<RingInstance> failedInstances, Set<String> blockedInstanceIps)
    {
        return failedInstances.contains(ringInstance)
               || blockedInstanceIps.contains(ringInstance.ipAddress());
    }

    /**
     * Schedule the stream with the produced sstables and return the stream result.
     *
     * @param sstableWriter produces SSTable(s)
     * @return stream result
     */
    protected abstract StreamResult doScheduleStream(SortedSSTableWriter sstableWriter);

    /**
     * Send the SSTable(s) written by SSTableWriter
     * The code runs on a separate thread
     *
     * @param sstableWriter produces SSTable(s)
     */
    protected abstract void sendSSTables(SortedSSTableWriter sstableWriter);
}
