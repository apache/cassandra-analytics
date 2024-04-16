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

import com.google.common.collect.Range;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.jetbrains.annotations.NotNull;

public class CassandraDirectDataTransportContext implements TransportContext.DirectDataBulkWriterContext
{
    @NotNull
    private final BulkSparkConf conf;
    @NotNull
    private final JobInfo jobInfo;
    @NotNull
    private final ClusterInfo clusterInfo;
    @NotNull
    private final DirectDataTransferApi dataTransferApi;

    public CassandraDirectDataTransportContext(@NotNull BulkWriterContext bulkWriterContext,
                                               @NotNull BulkSparkConf conf,
                                               boolean isOnDriver) // DIRECT mode does not need to distinguish driver and executor
    {
        this.conf = conf;
        this.jobInfo = bulkWriterContext.job();
        this.clusterInfo = bulkWriterContext.cluster();
        this.dataTransferApi = createDirectDataTransferApi();
    }

    @Override
    public DirectStreamSession createStreamSession(BulkWriterContext writerContext,
                                                   String sessionId,
                                                   SortedSSTableWriter sstableWriter,
                                                   Range<BigInteger> range,
                                                   ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        return new DirectStreamSession(writerContext,
                                       sstableWriter,
                                       this,
                                       sessionId,
                                       range,
                                       failureHandler);
    }

    @Override
    public DirectDataTransferApi dataTransferApi()
    {
        return dataTransferApi;
    }

    // only invoke in constructor
    protected DirectDataTransferApi createDirectDataTransferApi()
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(clusterInfo.getLowestCassandraVersion());
        return new SidecarDataTransferApi(clusterInfo.getCassandraContext(), bridge, jobInfo, conf);
    }
}
