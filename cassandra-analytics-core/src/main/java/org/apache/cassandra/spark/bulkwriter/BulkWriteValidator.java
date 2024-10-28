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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;

/**
 * A validator for bulk write result against the target cluster(s).
 */
public class BulkWriteValidator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkWriteValidator.class);

    private final ClusterInfo cluster;
    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    private final JobInfo job;
    private String phase = "Initializing";

    public BulkWriteValidator(BulkWriterContext bulkWriterContext,
                              ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        this.cluster = bulkWriterContext.cluster();
        this.job = bulkWriterContext.job();
        this.failureHandler = failureHandler;
    }

    public static void validateClOrFail(TokenRangeMapping<RingInstance> tokenRangeMapping,
                                        ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                        Logger logger,
                                        String phase,
                                        JobInfo job,
                                        ClusterInfo cluster)
    {
        List<ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange> failedRanges =
        failureHandler.getFailedRanges(tokenRangeMapping, job, cluster);

        if (failedRanges.isEmpty())
        {
            logger.info("Succeeded {} with {}", phase, job.getConsistencyLevel());
        }
        else
        {
            String message = String.format("Failed to write %s ranges with %s for job %s in phase %s.",
                                           failedRanges.size(), job.getConsistencyLevel(), job.getId(), phase);
            logger.error(message);
            logFailedRanges(logger, phase, failedRanges);
            throw new ConsistencyNotSatisfiedException(message);
        }
    }

    public String getPhase()
    {
        return phase;
    }

    public void setPhase(String phase)
    {
        LOGGER.info("Updating write phase from {} to {}", this.phase, phase);
        this.phase = phase;
    }

    public synchronized void updateFailureHandler(List<StreamError> streamErrors)
    {
        streamErrors.forEach(err -> {
            LOGGER.info("Populate stream error from tasks. {}", err);
            failureHandler.addFailure(err.failedRange, err.instance, err.errMsg);
        });
    }

    public static void updateFailureHandler(CommitResult commitResult,
                                            String phase,
                                            ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        LOGGER.debug("Commit Result: {}", commitResult);
        commitResult.failures.forEach((uuid, err) -> {
            LOGGER.warn("[{}]: {} failed on {} with message {}",
                        uuid,
                        phase,
                        commitResult.instance.nodeName(),
                        err.errMsg);
            failureHandler.addFailure(err.tokenRange, commitResult.instance, err.errMsg);
        });
    }

    private static void logFailedRanges(Logger logger, String phase,
                                        List<ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange> failedRanges)
    {
        for (ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange failedRange : failedRanges)
        {
            failedRange.failuresPerInstance.forEachInstance((instance, errors) -> {
                logger.error("Failed in phase {} for {} on {}. Failure: {}",
                             phase,
                             failedRange.range,
                             instance.toString(),
                             errors);
            });
        }
    }

    public void updateFailureHandler(Range<BigInteger> failedRange, RingInstance instance, String reason)
    {
        failureHandler.addFailure(failedRange, instance, reason);
    }

    public void validateClOrFail(TokenRangeMapping<RingInstance> tokenRangeMapping)
    {
        validateClOrFail(tokenRangeMapping, true);
    }

    public void validateClOrFail(TokenRangeMapping<RingInstance> tokenRangeMapping, boolean refreshInstanceAvailability)
    {
        if (refreshInstanceAvailability)
        {
            // Updates failures by looking up instance metadata
            updateInstanceAvailability();
        }
        // Fails if the failures violate consistency requirements
        validateClOrFail(tokenRangeMapping, failureHandler, LOGGER, phase, job, cluster);
    }

    private void updateInstanceAvailability()
    {
        cluster.refreshClusterInfo();
        Map<RingInstance, WriteAvailability> availability = cluster.clusterWriteAvailability();
        availability.forEach(this::validateAvailabilityAndUpdateFailures);
    }

    private void validateAvailabilityAndUpdateFailures(RingInstance instance, WriteAvailability availability)
    {
        switch (availability)
        {
            case INVALID_STATE:
                // If we find any nodes in a totally invalid state, just throw as we can't continue
                String errorMessage = String.format("Instance (%s) is in an invalid state (%s) during import. "
                                                    + "Please rerun import once topology changes are complete.",
                                                    instance.nodeName(), instance.nodeState());
                throw new RuntimeException(errorMessage);
            case UNAVAILABLE_DOWN:
                Collection<Range<BigInteger>> unavailableRanges = cluster.getTokenRangeMapping(true)
                                                                         .getTokenRanges()
                                                                         .get(instance);
                unavailableRanges.forEach(failedRange -> {
                    String nodeDisplayName = instance.nodeName();
                    String message = String.format("%s %s", nodeDisplayName, availability.getMessage());
                    LOGGER.warn("{} failed in phase {} on {} because {}", failedRange, phase, nodeDisplayName, message);
                    failureHandler.addFailure(failedRange, instance, message);
                });
                break;

            default:
                // DO NOTHING
                break;
        }
    }
}
