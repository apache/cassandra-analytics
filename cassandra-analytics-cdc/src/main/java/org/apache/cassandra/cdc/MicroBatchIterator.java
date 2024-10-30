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

package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.scanner.CdcScannerBuilder;
import org.apache.cassandra.cdc.scanner.CdcStreamScanner;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.IOUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator for reading a single CDC microbatch.
 * Not thread safe, MicroBatchIterator should be initialized and consumed in a single thread.
 */
public class MicroBatchIterator implements Iterator<CdcEvent>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroBatchIterator.class);

    @Nullable
    protected final TokenRange tokenRange;
    protected final CdcScannerBuilder builder;
    protected final CdcStreamScanner scanner;
    private CdcEvent curr = null;
    private boolean exhausted = false;

    @VisibleForTesting
    public MicroBatchIterator(CdcBridge cdcBridge,
                              CdcState startState,
                              CassandraSource cassandraSource,
                              Supplier<Set<String>> keyspaceSupplier,
                              CdcOptions cdcOptions,
                              AsyncExecutor asyncExecutor,
                              CommitLogProvider commitLogProvider)
    {
        this(cdcBridge,
             0,
             null,
             startState,
             cassandraSource,
             keyspaceSupplier,
             cdcOptions,
             asyncExecutor,
             commitLogProvider,
             ICdcStats.STUB);
    }

    public MicroBatchIterator(CdcBridge cdcBridge,
                              int partitionId,
                              @Nullable TokenRange tokenRange,
                              CdcState startState,
                              CassandraSource cassandraSource,
                              Supplier<Set<String>> keyspaceSupplier,
                              CdcOptions cdcOptions,
                              AsyncExecutor asyncExecutor,
                              CommitLogProvider commitLogProvider,
                              ICdcStats stats) throws NotEnoughReplicasException
    {
        stats.watermarkerSize(startState.size());
        this.tokenRange = tokenRange;
        Map<CassandraInstance, List<CommitLog>> logs = commitLogProvider.logs(this.tokenRange)
                                                                        .collect(Collectors.groupingBy(CommitLog::instance, Collectors.toList()));

        // if insufficient replicas for any keyspace, then skip entirely otherwise we end up reading
        // all mutations (e.g. at RF=1) into the CDC state and storing until another replica comes back up.
        // This could cause state to grow indefinitely, it is better to not proceed and resume from CommitLog offset when enough replicas come back up.
        for (String keyspace : keyspaceSupplier.get())
        {
            int minReplicas = cdcOptions.minimumReplicas(keyspace);
            if (logs.size() < minReplicas)
            {
                //NOTE: this can happen when there are no writes and no commit logs to read
                LOGGER.debug("Insufficient replicas available keyspace={} requiredReplicas={} availableReplicas={}",
                             keyspace, minReplicas, logs.size());
                throw new NotEnoughReplicasException(cdcOptions.consistencyLevel(),
                                                     tokenRange == null ? null : tokenRange.lowerEndpoint(),
                                                     tokenRange == null ? null : tokenRange.upperEndpoint(),
                                                     minReplicas,
                                                     logs.size(),
                                                     cdcOptions.dc());
            }
        }

        this.builder = new CdcScannerBuilder(cdcBridge,
                                             partitionId,
                                             cdcOptions,
                                             stats,
                                             tokenRange,
                                             startState,
                                             asyncExecutor,
                                             false,
                                             logs,
                                             cassandraSource);
        this.scanner = builder.build();

        if (LOGGER.isTraceEnabled())
        {
            CdcState endState = this.scanner.endState();
            endState.markers.values()
                            .forEach(marker -> LOGGER.trace("Next epoch marker epoch={} instance={} segmentId={} position={} partitionId={}",
                                                            endState.epoch, marker.instance().nodeName(), marker.segmentId(), marker.position(), partitionId));
        }
    }

    public CdcState endState()
    {
        return this.scanner.endState();
    }

    // java.util.Iterator

    @Override
    public boolean hasNext()
    {
        if (exhausted)
        {
            return false;
        }
        if (this.curr != null)
        {
            return true;
        }

        try
        {
            if (this.scanner.next())
            {
                this.curr = this.scanner.data();
            }
            else
            {
                exhausted = true;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return this.curr != null;
    }

    public CdcEvent next()
    {
        CdcEvent event = curr;
        this.curr = null;
        return event;
    }

    // java.lang.AutoClosable

    public void close()
    {
        if (scanner != null)
        {
            IOUtils.closeQuietly(this.scanner);
        }
    }
}
