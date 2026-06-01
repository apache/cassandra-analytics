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
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Range;

import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;

/**
 * Stream session for bulk writes to keyspaces with mutation tracking enabled.
 *
 * <p>
 * Tracked stream session uploads and triggers import on the <em>coordinator node only</em>. Cassandra's coordinated
 * transfer then propagates the data to all other replicas, avoiding the duplicate-row updates that would occur if each
 * replica independently streamed the data to its peers.
 * <p>
 */
public class TrackedDirectStreamSession extends StreamSession<TransportContext.DirectDataBulkWriterContext>
{
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

    @Override
    protected void onSSTablesProduced(Set<SSTableDescriptor> sstables)
    {
        throw new UnsupportedOperationException("TrackedDirectStreamSession is not yet implemented");
    }

    @Override
    protected StreamResult doFinalizeStream()
    {
        throw new UnsupportedOperationException("TrackedDirectStreamSession is not yet implemented");
    }

    @Override
    protected void sendRemainingSSTables()
    {
        throw new UnsupportedOperationException("TrackedDirectStreamSession is not yet implemented");
    }
}
