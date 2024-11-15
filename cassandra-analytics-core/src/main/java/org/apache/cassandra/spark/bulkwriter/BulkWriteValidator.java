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
    private static final int ERROR_MESSAGE_MAX_LENGTH = 1024 * 64;

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
            String header = String.format("Failed to write %s ranges with %s for job %s in phase %s. ",
                                           failedRanges.size(), job.getConsistencyLevel(), job.getId(), phase);
            String errorDetails = logEachErrorAndAggregate(logger, phase, failedRanges);
            String message = header + errorDetails;
            logger.error(message);
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

    // aggregate the stream errors in order to provide a better insight on failure
    private static String logEachErrorAndAggregate(Logger logger,
                                                   String phase,
                                                   List<ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange> failedRanges)
    {
        StringBuilder sb = new StringBuilder();
        for (ReplicaAwareFailureHandler<RingInstance>.ConsistencyFailurePerRange failedRange : failedRanges)
        {
            for (Map.Entry<RingInstance, Collection<String>> entry : failedRange.failuresPerInstance.entrySet())
            {
                logger.error("Failed in phase {} for {} on {}. Failure: {}", phase, failedRange.range, entry.getKey(), entry.getValue());
                if (sb.length() >= ERROR_MESSAGE_MAX_LENGTH)
                {
                    sb.setLength(ERROR_MESSAGE_MAX_LENGTH - 3);
                    sb.append("...");
                    continue;
                }
                sb.append("Instance: ").append(entry.getKey()).append(" All failures: ").append(entry.getValue());
            }
        }
        return sb.toString();
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
