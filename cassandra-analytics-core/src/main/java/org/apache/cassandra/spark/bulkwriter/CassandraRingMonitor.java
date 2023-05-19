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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.util.ThreadUtil;
import org.apache.cassandra.spark.common.client.ClientException;

public class CassandraRingMonitor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRingMonitor.class);

    private CassandraRing<RingInstance> initialRing;
    private volatile boolean ringChanged = false;
    private Callable<CassandraRing<RingInstance>> ringSupplier;
    private Consumer<CancelJobEvent> changeFunc;
    private final ScheduledExecutorService executorService;

    public CassandraRingMonitor(ClusterInfo clusterInfo,
                                Consumer<CancelJobEvent> changeFunc,
                                int checkInterval,
                                TimeUnit checkIntervalUnit) throws Exception
    {
        this(() -> clusterInfo.getRing(false),
             changeFunc,
             checkInterval,
             checkIntervalUnit,
             Executors.newSingleThreadScheduledExecutor(ThreadUtil.threadFactory("Cassandra Ring Monitor")));
    }

    @VisibleForTesting
    public CassandraRingMonitor(Callable<CassandraRing<RingInstance>> ringSupplier,
                                Consumer<CancelJobEvent> changeFunc,
                                int checkInterval,
                                TimeUnit checkIntervalUnit,
                                ScheduledExecutorService executorService) throws Exception
    {
        this.ringSupplier = ringSupplier;
        this.initialRing = ringSupplier.call();
        this.changeFunc = changeFunc;
        this.executorService = executorService;
        executorService.scheduleAtFixedRate(this::checkRingChanged, 0, checkInterval, checkIntervalUnit);
    }

    /**
     * Reads the ring changed value from the monitor. This actual ring
     * is retrieved and compared every second - this is just the backing
     * store for the result.
     *
     * @return if the ring has changed or not since this instance was created
     */
    public boolean getRingChanged()
    {
        return ringChanged;
    }

    public void stop()
    {
        executorService.shutdown();
        try
        {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException exception)
        {
            // Do nothing as we're just waiting for the ring call to complete
        }
    }

    private void checkRingChanged()
    {
        LOGGER.debug("Checking ring for changes");
        try
        {
            CassandraRing<RingInstance> currentRing = ringSupplier.call();
            if (checkRingChanged(currentRing))
            {
                ringChanged = true;
                changeFunc.accept(new CancelJobEvent("Ring changed during bulk load"));
                executorService.shutdownNow();
            }
        }
        catch (ClientException exception)
        {
            LOGGER.warn("Could not retrieve current ring. Will retry momentarily. Continuing bulk load.", exception);
        }
        catch (Exception exception)
        {
            String message = "Error while attempting to determine if ring changed. Failing job";
            LOGGER.error(message, exception);
            changeFunc.accept(new CancelJobEvent(message, exception));
            executorService.shutdownNow();
        }
    }

    private boolean checkRingChanged(CassandraRing<RingInstance> currentRing)
    {
        Collection<Range<BigInteger>> currentTokenRanges = currentRing.getTokenRanges().values();
        Collection<Range<BigInteger>> startingTokenRanges = initialRing.getTokenRanges().values();
        LOGGER.debug("Current token range: {}", currentTokenRanges);
        LOGGER.debug("Initial token range: {}", startingTokenRanges);
        return !CollectionUtils.isEqualCollection(currentTokenRanges, startingTokenRanges);
    }
}
