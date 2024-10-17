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

package org.apache.cassandra.cdc.scanner.jdk;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.msg.jdk.CdcEvent;
import org.apache.cassandra.cdc.msg.jdk.RangeTombstone;
import org.apache.cassandra.cdc.msg.jdk.Value;
import org.apache.cassandra.cdc.scanner.AbstractCdcScannerBuilder;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * CdcScannerBuilder is an AbstractCdcScannerBuilder implementation that converts the mutation into
 * `org.apache.cassandra.cdc.msg.jdk.CdcEvent` and `org.apache.cassandra.cdc.msg.jdk.CdcMessage`, deserializing the values into Java types.
 */
public class CdcScannerBuilder extends AbstractCdcScannerBuilder<Value, RangeTombstone, CdcEvent, CdcSortedStreamScanner>
{
    public CdcScannerBuilder(int partitionId,
                             CdcOptions cdcOptions,
                             CdcStats stats,
                             @Nullable TokenRange tokenRange,
                             @NotNull CdcState cdcState,
                             @NotNull AsyncExecutor executor,
                             boolean readCommitLogHeader,
                             @NotNull Map<CassandraInstance, List<CommitLog>> logs,
                             final CassandraSource cassandraSource)
    {
        super(partitionId,
              cdcOptions,
              stats,
              tokenRange,
              cdcState,
              executor,
              readCommitLogHeader,
              logs,
              cassandraSource);
    }

    @Override
    public CdcSortedStreamScanner buildStreamScanner(Collection<PartitionUpdateWrapper> updates, @NotNull CdcState endState)
    {
        return new CdcSortedStreamScanner(updates, endState, cassandraSource, cdcOptions.samplingRate());
    }
}
