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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;

public class BulkWriteValidator implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkWriteValidator.class);

    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    private final CassandraRingMonitor monitor;
    private final JobInfo job;
    private String phase = "Initializing";

    private final ClusterInfo cluster;

    public BulkWriteValidator(BulkWriterContext bulkWriterContext,
                              Consumer<CancelJobEvent> cancelJobFunc) throws Exception
    {
        cluster = bulkWriterContext.cluster();
        job = bulkWriterContext.job();
        failureHandler = new ReplicaAwareFailureHandler<>(cluster.getRing(true));
        monitor = new CassandraRingMonitor(cluster, cancelJobFunc, 1000, TimeUnit.MILLISECONDS);
    }

    public void setPhase(String phase)
    {
        this.phase = phase;
    }

    public String getPhase()
    {
        return phase;
    }

    public void validateInitialEnvironment()
    {
        validateCLOrFail();
    }

    public void close()
    {
        monitor.stop();
    }

    private void validateCLOrFail()
    {
        updateInstanceAvailability();
        validateClOrFail(failureHandler, LOGGER, phase, job);
    }

    public static void validateClOrFail(ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                        Logger logger,
                                        String phase,
                                        JobInfo job)
    {
        Collection<AbstractMap.SimpleEntry<Range<BigInteger>, Multimap<RingInstance, String>>> failedRanges =
                failureHandler.getFailedEntries(job.getConsistencyLevel(), job.getLocalDC());
        if (failedRanges.isEmpty())
        {
            logger.info("Succeeded {} with {}", phase, job.getConsistencyLevel());
        }
        else
        {
            String message = String.format("Failed to load %s ranges with %s for job %s in phase %s",
                                           failedRanges.size(), job.getConsistencyLevel(), job.getId(), phase);
            logger.error(message);
            failedRanges.forEach(failedRange -> failedRange.getValue().keySet().forEach(instance ->
                    logger.error("Failed {} for {} on {}", phase, failedRange.getKey(), instance.toString())));
            throw new RuntimeException(message);
        }
    }

    public static void updateFailureHandler(CommitResult commitResult,
                                            String phase,
                                            ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        LOGGER.debug("Commit Result: {}", commitResult);
        commitResult.failures.forEach((uuid, err) -> {
            LOGGER.warn("[{}]: {} failed on {} with message {}",
                    uuid, phase, commitResult.instance.getNodeName(), err.errMsg);
            failureHandler.addFailure(err.tokenRange, commitResult.instance, err.errMsg);
        });
    }

    private void updateInstanceAvailability()
    {
        cluster.refreshClusterInfo();
        Map<RingInstance, InstanceAvailability> availability = cluster.getInstanceAvailability();
        availability.forEach(this::checkInstance);
    }

    private void checkInstance(RingInstance instance, InstanceAvailability availability)
    {
        throwIfInvalidState(instance, availability);
        updateFailureHandler(instance, availability);
    }

    private void throwIfInvalidState(RingInstance instance, InstanceAvailability availability)
    {
        if (availability == InstanceAvailability.INVALID_STATE)
        {
            // If we find any nodes in a totally invalid state, just throw as we can't continue
            String message = String.format("Instance (%s) is in an invalid state (%s) during import. "
                                         + "Please rerun import once topology changes are complete.",
                                           instance.getNodeName(), cluster.getInstanceState(instance));
            throw new RuntimeException(message);
        }
    }

    private void updateFailureHandler(RingInstance instX, InstanceAvailability availability)
    {
        if (availability != InstanceAvailability.AVAILABLE)
        {
            addFailedInstance(instX, availability.getMessage());
        }
    }

    private void addFailedInstance(RingInstance instance, String reason)
    {
        Collection<Range<BigInteger>> failedRanges = cluster.getRing(true).getTokenRanges(instance);
        failedRanges.forEach(failedRange -> {
            String nodeDisplayName = instance.getNodeName();
            String message = String.format("%s %s", nodeDisplayName, reason);
            LOGGER.warn("{} failed in phase {} on {} because {}", failedRange, phase, nodeDisplayName, message);
            failureHandler.addFailure(failedRange, instance, message);
        });
    }

    public void failIfRingChanged()
    {
        if (monitor.getRingChanged())
        {
            throw new RuntimeException(String.format("Ring changed during %s stage of import. "
                                                   + "Please rerun import once topology changes are complete.",
                                                     phase));
        }
    }
}
