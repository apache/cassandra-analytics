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

package org.apache.cassandra.spark.data;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.common.response.TableStatsResponse;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.analyticsclient.SidecarClient;
import org.apache.cassandra.sidecar.analyticsclient.SidecarInstance;
import org.apache.cassandra.sidecar.analyticsclient.SidecarInstanceImpl;

/**
 * Implementation of {@link TableSizeProvider} that uses Sidecar's client to calculate the table
 * size
 */
public class SidecarTableSizeProvider implements TableSizeProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarTableSizeProvider.class);
    private final SidecarClient sidecarClient;
    private final int sidecarPort;
    private final CompletableFuture<RingResponse> ringFuture;

    public SidecarTableSizeProvider(SidecarClient sidecarClient, int sidecarPort, CompletableFuture<RingResponse> ringFuture)
    {
        this.sidecarClient = sidecarClient;
        this.sidecarPort = sidecarPort;
        this.ringFuture = ringFuture;
    }

    /**
     * Returns the total used space for {@code table} across the datacenter. If only a subset of instances succeeds
     * gathering the table size, the method will normalize the table size to the number of instances. If all
     * the instances fail to retrieve the table size, this method will throw an exception.
     *
     * @param keyspace   the keyspace where the table lives
     * @param table      the table to get the size from
     * @param datacenter the datacenter
     * @return the total used space for the table across the datacenter
     * @throws RuntimeException when all the instances fail to retrieve the table size
     */
    @Override
    public long tableSizeInBytes(String keyspace, String table, String datacenter)
    {
        try
        {
            RingResponse ringResponse = ringFuture.toCompletableFuture().get();
            return tableSizeInBytesInternal(ringResponse, keyspace, table, datacenter);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Error occurred while determining the table size for table '%s.%s'",
                                                     keyspace, table), ex);
        }
    }

    private long tableSizeInBytesInternal(RingResponse ringResponse, String keyspace, String table, String datacenter)
    {
        LongAdder tableSize = new LongAdder();
        LongAdder success = new LongAdder();
        List<RingEntry> instances = ringResponse.stream()
                                                .filter(ringEntry -> datacenter == null || datacenter.equals(ringEntry.datacenter()))
                                                .collect(Collectors.toUnmodifiableList());
        int instancesSize = instances.size();
        CountDownLatch latch = new CountDownLatch(instancesSize);

        for (RingEntry ringEntry : instances)
        {
            SidecarInstance sidecarInstance = new SidecarInstanceImpl(ringEntry.fqdn(), sidecarPort);
            CompletableFuture<TableStatsResponse> tableStatsResponseCompletableFuture = sidecarClient.tableStats(sidecarInstance, keyspace, table);
            tableStatsResponseCompletableFuture
            .whenComplete((response, throwable) -> {
                try
                {
                    if (throwable == null)
                    {
                        tableSize.add(response.totalDiskSpaceUsedBytes());
                        success.increment();
                    }
                    else
                    {
                        LOGGER.warn("Failed to retrieve table statistics for keyspace {} table {} on instance {}",
                                    keyspace, table, sidecarInstance, throwable);
                    }
                }
                finally
                {
                    latch.countDown();
                }
            });
        }

        // wait until all the calls to table stats have completed
        await(latch);

        long successCount = success.sum();
        long tableSizeSum = tableSize.sum();
        if (successCount == 0)
        {
            throw new RuntimeException(String.format("Unable to determine the table size for table '%s.%s'. 0/%d instances available.",
                                                     keyspace, table, instancesSize));
        }
        else if (successCount < instancesSize)
        {
            // normalize table size to the number of instances
            LOGGER.info("{}/{} instances were used to determine the table size {} for table {}.{}",
                        successCount, instancesSize, tableSizeSum, keyspace, table);
            return (tableSizeSum / successCount) * instancesSize;
        }
        return tableSizeSum;
    }

    static void await(CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
