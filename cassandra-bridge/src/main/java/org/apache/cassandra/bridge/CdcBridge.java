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

package org.apache.cassandra.bridge;

import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.CommitLogReader;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.Row;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.scanner.CdcStreamScanner;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.TimeProvider;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class CdcBridge
{
    // Used to indicate if a column is unset; used in generating mutations for CommitLog
    public static final Object UNSET_MARKER = new Object();

    // Implementations of CassandraBridge must be named as such to load dynamically using the {@link CassandraBridgeFactory}
    public static final String IMPLEMENTATION_FQCN = "org.apache.cassandra.bridge.CdcBridgeImplementation";
    public static final String CONVERTER_IMPLEMENTATION_FQCN = "org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverterImplementation";

    public void log(CqlTable cqlTable, CommitLogInstance log, Row row, long timestamp)
    {
        log(TimeProvider.DEFAULT, cqlTable, log, row, timestamp);
    }

    public abstract void updateCdcSchema(@Nonnull Set<CqlTable> cdcTables,
                                         @Nonnull Partitioner partitioner,
                                         @Nonnull TableIdLookup tableIdLookup);

    public abstract CommitLogReader.Result readLog(@Nonnull CommitLog log,
                                                   @Nullable TokenRange tokenRange,
                                                   @Nonnull CommitLogMarkers markers,
                                                   int partitionId,
                                                   @Nonnull ICdcStats stats,
                                                   @Nullable AsyncExecutor executor,
                                                   @Nullable Consumer<Marker> listener,
                                                   @Nullable Long startTimestampMicros,
                                                   boolean readCommitLogHeader);

    public abstract CdcStreamScanner openCdcStreamScanner(Collection<PartitionUpdateWrapper> updates,
                                                          @Nonnull CdcState endState,
                                                          Random random,
                                                          CassandraSource cassandraSource,
                                                          double traceSampleRate);

    @VisibleForTesting
    public abstract void log(TimeProvider timeProvider, CqlTable cqlTable, CommitLogInstance log, Row row, long timestamp);
}
