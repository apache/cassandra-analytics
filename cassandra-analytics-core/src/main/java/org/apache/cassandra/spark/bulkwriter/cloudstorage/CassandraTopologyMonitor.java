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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.CancelJobEvent;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.util.ThreadUtil;

/**
 * A monitor that check whether the cassandra topology has changed.
 * On topology change, the write produced by the job is no longer accurate. It should fail as soon as change is detected.
 */
public class CassandraTopologyMonitor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTopologyMonitor.class);
    private static final long PERIODIC_CHECK_DELAY_MS = 5000;
    private static final long MAX_CHECK_ATTEMPTS = 10; // Number of attempts to retry the failed check task

    private final ClusterInfo clusterInfo;
    private final TokenRangeMapping<RingInstance> initialTopology;
    private final ScheduledExecutorService executorService;
    private final Consumer<CancelJobEvent> onCancelJob;
    private int retryCount = 0;
    // isStopped is set to true on job cancellation, the scheduled task should do no-op
    private volatile boolean isStopped = false;

    public CassandraTopologyMonitor(ClusterInfo clusterInfo, Consumer<CancelJobEvent> onCancelJob)
    {
        this.clusterInfo = clusterInfo;
        // stop the monitor when job is cancelled
        this.onCancelJob = onCancelJob.andThen(e -> isStopped = true);
        this.initialTopology = clusterInfo.getTokenRangeMapping(false);
        this.executorService = Executors.newSingleThreadScheduledExecutor(ThreadUtil.threadFactory("Cassandra Topology Monitor"));
        executorService.scheduleWithFixedDelay(this::checkTopology, PERIODIC_CHECK_DELAY_MS, PERIODIC_CHECK_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Attempts to stop all tasks; we do not wait here as it is only called on job termination
     */
    public void shutdownNow()
    {
        executorService.shutdownNow();
    }

    /**
     * @return the initial topology retrieved
     */
    public TokenRangeMapping<RingInstance> initialTopology()
    {
        return initialTopology;
    }

    private void checkTopology()
    {
        if (isStopped)
        {
            LOGGER.info("Already stopped. Skip checking topology");
            return;
        }

        LOGGER.debug("Checking topology");
        try
        {
            TokenRangeMapping<RingInstance> currentTopology = clusterInfo.getTokenRangeMapping(false);
            if (!currentTopology.equals(initialTopology))
            {
                onCancelJob.accept(new CancelJobEvent("Topology changed during bulk write"));
                return;
            }
            retryCount = 0;
        }
        catch (Exception exception)
        {
            if (retryCount++ > MAX_CHECK_ATTEMPTS)
            {
                LOGGER.error("Could not retrieve current topology. All hosts exhausted. The retrieval has failed consecutive for {} times", retryCount);
                onCancelJob.accept(new CancelJobEvent("Could not retrieve current cassandra topology. " +
                                                      "All hosts and retries have been exhausted.",
                                                      exception));
            }
            else
            {
                LOGGER.warn("Could not retrieve current topology. Will retry momentarily. Continuing bulk write.", exception);
            }
        }
    }

    @VisibleForTesting
    void checkTopologyOnDemand()
    {
        checkTopology();
    }
}
