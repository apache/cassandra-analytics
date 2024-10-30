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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cdc.FourZeroMutation;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.CommitLogReader;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.RangeTombstoneData;
import org.apache.cassandra.cdc.api.Row;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.scanner.CdcSortedStreamScanner;
import org.apache.cassandra.cdc.scanner.CdcStreamScanner;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.db.commitlog.FourZeroPartitionUpdateWrapper;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CdcBridgeImplementation extends CdcBridge
{
    public static volatile boolean setup = false;

    public static void setup(Path path, int commitLogSegmentSize, boolean enableCompression)
    {
        CassandraTypesImplementation.setup();
        setCDC(path, commitLogSegmentSize, enableCompression);
    }

    public CdcBridgeImplementation()
    {
    }

    protected static synchronized void setCDC(Path path, int commitLogSegmentSize, boolean enableCompression)
    {
        if (setup)
        {
            return;
        }
        Path commitLogPath = path.resolve("commitlog");
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath.toString();
        DatabaseDescriptor.getRawConfig().hints_directory = path.resolve("hints").toString();
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path.resolve("saved_caches").toString();
        DatabaseDescriptor.getRawConfig().cdc_raw_directory = path.resolve("cdc").toString();
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCDCSpaceInMB(1024);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        if (enableCompression)
        {
            DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        }
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(commitLogSegmentSize);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider((commitLog -> new CommitLogSegmentManagerCDC(commitLog, commitLogPath.toString())));
        setup = true;
    }

    public void log(CqlTable cqlTable, CommitLogInstance log, Row row, long timestamp)
    {
        log(TimeProvider.DEFAULT, cqlTable, log, row, timestamp);
    }

    public void updateCdcSchema(@NotNull Set<CqlTable> cdcTables, @NotNull Partitioner partitioner, @NotNull TableIdLookup tableIdLookup)
    {
        CassandraSchema.updateCdcSchema(cdcTables, partitioner, tableIdLookup);
    }

    public CommitLogReader.Result readLog(@NotNull CommitLog log,
                                          @Nullable TokenRange tokenRange,
                                          @NotNull CommitLogMarkers markers,
                                          int partitionId,
                                          @NotNull ICdcStats stats,
                                          @Nullable AsyncExecutor executor,
                                          @Nullable Consumer<Marker> listener,
                                          @Nullable Long startTimestampMicros,
                                          boolean readCommitLogHeader)
    {
        try (BufferingCommitLogReader reader = new BufferingCommitLogReader(log,
                                                                            tokenRange,
                                                                            markers,
                                                                            partitionId,
                                                                            stats,
                                                                            executor,
                                                                            null,
                                                                            startTimestampMicros,
                                                                            readCommitLogHeader))
        {
            return reader.result();
        }
    }

    public CdcStreamScanner openCdcStreamScanner(Collection<PartitionUpdateWrapper> updates,
                                                 @NotNull CdcState endState,
                                                 Random random,
                                                 CassandraSource cassandraSource,
                                                 double traceSampleRate)
    {
        return new CdcSortedStreamScanner(updates.stream().map(a -> (FourZeroPartitionUpdateWrapper) a).collect(Collectors.toList()),
                                          endState,
                                          ThreadLocalRandom.current(),
                                          cassandraSource,
                                          traceSampleRate);
    }


    @VisibleForTesting
    public void log(TimeProvider timeProvider, CqlTable cqlTable, CommitLogInstance log, Row row, long timestamp)
    {
        final Mutation mutation = makeMutation(timeProvider, cqlTable, row, timestamp);
        log.add(FourZeroMutation.wrap(mutation));
    }

    @NotNull
    @VisibleForTesting
    public static Mutation makeMutation(TimeProvider timeProvider, CqlTable cqlTable, Row row, long timestamp)
    {
        final TableMetadata table = Schema.instance.getTableMetadata(cqlTable.keyspace(), cqlTable.table());
        assert table != null;

        final org.apache.cassandra.db.rows.Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        if (row.isInsert())
        {
            rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, timeProvider.nowInSeconds()));
        }
        org.apache.cassandra.db.rows.Row staticRow = Rows.EMPTY_STATIC_ROW;

        // build partition key
        final List<CqlField> partitionKeys = cqlTable.partitionKeys();
        final ByteBuffer partitionKey = ByteBufferUtils.buildPartitionKey(partitionKeys,
                                                                          partitionKeys.stream()
                                                                                       .map(f -> row.get(f.position()))
                                                                                       .toArray());

        final DecoratedKey decoratedPartitionKey = table.partitioner.decorateKey(partitionKey);
        // create a mutation and return early
        if (isPartitionDeletion(cqlTable, row))
        {
            PartitionUpdate delete = PartitionUpdate.fullPartitionDelete(table, partitionKey, timestamp, timeProvider.nowInSeconds());
            return new Mutation(delete);
        }

        final List<CqlField> clusteringKeys = cqlTable.clusteringKeys();

        // create a mutation with rangetombstones
        if (row.rangeTombstones() != null && !row.rangeTombstones().isEmpty())
        {
            return makeRangeTombstone(cqlTable, table, decoratedPartitionKey, timestamp, timeProvider, row);
        }

        // When the test row data (IRow) defines no regular row, noRegularRow is true. It happens when clustering keys are defined, but not set.
        boolean noRegularRow = false;
        // build clustering key
        if (clusteringKeys.isEmpty())
        {
            rowBuilder.newRow(Clustering.EMPTY);
        }
        else if (clusteringKeys.stream().allMatch(f -> row.get(f.position()) == null))
        {
            // clustering key is defined, but not set ==> no regular row
            noRegularRow = true;
        }
        else
        {
            rowBuilder.newRow(Clustering.make(
                              clusteringKeys.stream()
                                            .map(f -> f.serialize(row.get(f.position())))
                                            .toArray(ByteBuffer[]::new))
            );
        }

        if (row.isDeleted())
        {
            rowBuilder.addRowDeletion(org.apache.cassandra.db.rows.Row.Deletion.regular(new DeletionTime(timestamp, timeProvider.nowInSeconds())));
        }
        else
        {
            BiConsumer<org.apache.cassandra.db.rows.Row.Builder, CqlField> rowBuildFunc = (builder, field) -> {
                final CqlType type = (CqlType) field.type();
                final ColumnMetadata cd = table.getColumn(new ColumnIdentifier(field.name(), false));
                Object value = row.get(field.position());
                if (value != UNSET_MARKER) // if unset, do not add the cell
                {
                    if (value == null)
                    {
                        if (cd.isComplex())
                        {
                            type.addComplexTombstone(builder, cd, timestamp);
                        }
                        else
                        {
                            type.addTombstone(builder, cd, timestamp);
                        }
                    }
                    else if (value instanceof CollectionElement)
                    {
                        CollectionElement ce = (CollectionElement) value;
                        if (ce.value == null)
                        {
                            type.addTombstone(builder, cd, timestamp, ce.cellPath);
                        }
                        else
                        {
                            type.addCell(builder, cd, timestamp, row.ttl(), timeProvider.nowInSeconds(), ce.value, ce.cellPath);
                        }
                    }
                    else
                    {
                        type.addCell(builder, cd, timestamp, row.ttl(), timeProvider.nowInSeconds(), value);
                    }
                }
            };

            if (!cqlTable.staticColumns().isEmpty())
            {
                org.apache.cassandra.db.rows.Row.Builder staticRowBuilder = BTreeRow.sortedBuilder();
                staticRowBuilder.newRow(Clustering.STATIC_CLUSTERING);
                for (final CqlField field : cqlTable.staticColumns())
                {
                    rowBuildFunc.accept(staticRowBuilder, field);
                }
                staticRow = staticRowBuilder.build(); // replace the empty row with the new static row built
            }

            // build value cells
            for (final CqlField field : cqlTable.valueColumns())
            {
                rowBuildFunc.accept(rowBuilder, field);
            }
        }

        return new Mutation(PartitionUpdate.singleRowUpdate(table, decoratedPartitionKey,
                                                            noRegularRow ? null : rowBuilder.build(), // regular row
                                                            staticRow)); // static row
    }

    protected static Mutation makeRangeTombstone(CqlTable cqlTable,
                                                 TableMetadata table,
                                                 DecoratedKey decoratedPartitionKey,
                                                 long timestamp,
                                                 TimeProvider timeProvider,
                                                 Row row)
    {
        final List<CqlField> clusteringKeys = cqlTable.clusteringKeys();
        PartitionUpdate.SimpleBuilder pub = PartitionUpdate.simpleBuilder(table, decoratedPartitionKey)
                                                           .timestamp(timestamp)
                                                           .nowInSec(timeProvider.nowInSeconds());
        for (RangeTombstoneData rt : row.rangeTombstones())
        {
            // range tombstone builder is built when partition update builder builds
            PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder rangeTombstoneBuilder = pub.addRangeTombstone();
            rangeTombstoneBuilder = rt.open.inclusive
                                    ? rangeTombstoneBuilder.inclStart()
                                    : rangeTombstoneBuilder.exclStart(); // returns the same ref. just to make compiler happy
            Object[] startValues = clusteringKeys.stream()
                                                 .map(f -> {
                                                     Object v = rt.open.values[f.position() - cqlTable.numPartitionKeys()];
                                                     return v == null ? null : f.serialize(v);
                                                 })
                                                 .filter(Objects::nonNull)
                                                 .toArray(ByteBuffer[]::new);
            rangeTombstoneBuilder.start(startValues);
            rangeTombstoneBuilder = rt.close.inclusive ? rangeTombstoneBuilder.inclEnd() : rangeTombstoneBuilder.exclEnd();
            Object[] endValues = clusteringKeys.stream()
                                               .map(f -> {
                                                   Object v = rt.close.values[f.position() - cqlTable.numPartitionKeys()];
                                                   return v == null ? null : f.serialize(v);
                                               })
                                               .filter(Objects::nonNull)
                                               .toArray(ByteBuffer[]::new);
            rangeTombstoneBuilder.end(endValues);
        }
        return new Mutation(pub.build());
    }

    @VisibleForTesting
    protected static boolean isPartitionDeletion(CqlTable cqlTable, Row row)
    {
        final List<CqlField> clusteringKeys = cqlTable.clusteringKeys();
        final List<CqlField> valueFields = cqlTable.valueColumns();
        final List<CqlField> staticFields = cqlTable.staticColumns();
        for (CqlField f : Iterables.concat(clusteringKeys, valueFields, staticFields))
        {
            if (row.get(f.position()) != null)
            {
                return false;
            }
        }
        return true;
    }
}
