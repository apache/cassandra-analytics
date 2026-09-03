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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.CommitLogReader;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.Row;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.StatePersister;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.scanner.CdcStreamScanner;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.types.Duration;
import org.apache.cassandra.spark.data.types.TimeUUID;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.api.Pair;

import static org.apache.cassandra.cdc.test.CdcTester.DEFAULT_NUM_ROWS;
import static org.apache.cassandra.cdc.test.CdcTester.assertCqlTypeEquals;
import static org.apache.cassandra.cdc.test.CdcTester.newUniqueRow;
import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.apache.cassandra.spark.CommonTestUtils.qtRandom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

@SuppressWarnings("DataFlowIssue")
public class CdcTests extends CdcTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTests.class);
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
                                                                                new ThreadFactoryBuilder()
                                                                                .setNameFormat("cdc-io-%d")
                                                                                .setDaemon(true)
                                                                                .build());
    public static final AsyncExecutor ASYNC_EXECUTOR = AsyncExecutor.wrap(EXECUTOR);

    private static final int TTL = 42;

    public static CommitLogProvider logProvider(Path dir)
    {
        return (rangeFilter) -> {
            try
            {
                try (Stream<Path> stream = Files.list(dir.resolve("cdc")))
                {
                    return stream.filter(Files::isRegularFile)
                                 .filter(path -> path.getFileName().toString().endsWith(".log"))
                                 .map(LocalCommitLog::new)
                                 .collect(Collectors.toSet())
                                 .stream()
                                 .map(l -> (LocalCommitLog) l);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
    }

    enum OperationType implements Consumer<TestSchema.TestRow>
    {
        INSERT(TestSchema.TestRow::fromInsert),
        UPDATE(TestSchema.TestRow::fromUpdate);

        private final Consumer<TestSchema.TestRow> testRowConsumer;

        OperationType(Consumer<TestSchema.TestRow> testRowConsumer)
        {
            this.testRowConsumer = testRowConsumer;
        }

        public void accept(TestSchema.TestRow row)
        {
            testRowConsumer.accept(row);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMockedCdc(CassandraVersion version)
    {
        try
        {
            Set<String> seenMutations = ConcurrentHashMap.newKeySet();
            EventConsumer eventConsumer = event -> {
                String key = event.getHexKey();
                if (seenMutations.contains(key))
                {
                    throw new IllegalStateException("Event seen before");
                }
                seenMutations.add(key);
            };
            final int maxRows = 5000;
            final int batchSize = 500;
            final int numBatches = maxRows / batchSize;
            // withCdc(true) is required so table.cdc() is true — SchemaSupplier.getTables() now
            // returns all tables, and Cdc.refreshSchema() filters to CDC-enabled ones via
            // CqlTable#cdc(), not via any pre-filtering by the supplier itself. Without this,
            // cdcEnabledTables ends up empty, keyspaceSupplier() watches no keyspaces, and the
            // CDC scanner never sees the mutations this test writes.
            TestSchema testSchema = TestSchema.basicBuilder(bridge).withCdc(true).build();

            CqlTable table = testSchema.buildTable();
            bridge.buildSchema(table.createStatement(),
                               table.keyspace(),
                               table.replicationFactor(),
                               Partitioner.Murmur3Partitioner,
                               table.udtCreateStmts(bridge.cassandraTypes()),
                               null,
                               0,
                               true);
            SchemaSupplier schemaSupplier = () -> CompletableFuture.completedFuture(ImmutableSet.of(table));
            AtomicReference<byte[]> state = new AtomicReference<>();
            StatePersister statePersister = new StatePersister()
            {
                public void persist(String jobId, int partitionId, @Nullable TokenRange tokenRange, @NotNull ByteBuffer buf)
                {
                    byte[] ar = new byte[buf.remaining()];
                    buf.get(ar);
                    state.set(ar);
                }

                @NotNull
                public List<CdcState> loadState(String jobId, int partitionId, @Nullable TokenRange tokenRange)
                {
                    byte[] ar = state.get();
                    if (ar == null)
                    {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(CdcState.deserialize(CdcKryoRegister.kryo(), bridge.compressionUtil(), state.get()));
                }
            };

            Map<String, TestSchema.TestRow> writtenRows = new HashMap<>();
            Random random = new Random();
            Runnable writer = () -> {
                IntStream.range(0, batchSize)
                         .forEach(i -> {
                             TestSchema.TestRow testRow = CdcTester.newUniqueRow(testSchema, writtenRows, random);
                             cdcBridge.log(table, commitLog, testRow, TimeUtils.nowMicros());
                             writtenRows.put(testRow.getPrimaryHexKey(), testRow);
                         });
                commitLog.sync();
            };

            long startTime = System.currentTimeMillis();
            try (Cdc cdc = Cdc.builder("101", 0, eventConsumer, schemaSupplier)
                              .withExecutor(CdcTests.ASYNC_EXECUTOR)
                              .withStatePersister(statePersister)
                              .withTableIdLookup(cdcBridge.internalTableIdLookup())
                              .withCommitLogProvider(CdcTests.logProvider(commitLogDir))
                              .withCdcOptions(cdcOptions)
                              .build())
            {
                cdc.start();

                long startMillis = startTime;
                for (int i = 0; i < numBatches; i++)
                {
                    // write in batches and verify we can read each batch
                    writer.run();

                    while (seenMutations.size() < writtenRows.size())
                    {
                        Thread.sleep(5);
                        if (CdcTester.maybeTimeout(startMillis, maxRows, seenMutations.size(), "testMockedCdc"))
                        {
                            fail("Failed to read all mutations after timeout");
                        }
                    }
                    startMillis = System.currentTimeMillis();
                }

                assertThat(seenMutations.size()).isEqualTo(writtenRows.size());
                assertThat(seenMutations).isEqualTo(writtenRows.values()
                                        .stream()
                                        .map(TestSchema.TestRow::getPrimaryHexKey)
                                        .collect(Collectors.toSet()));
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            // verify state is correct
            CdcState endState = statePersister.loadCanonicalState("101", 0, null);
            long numSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
            assertThat(endState.epoch >= Math.max(0, numSeconds - 4)).isTrue(); // epochs should be around ~ 1 per second
            assertThat(endState.replicaCount.isEmpty()).isTrue();
            Marker endMarker = endState.markers.startMarker(new CassandraInstance("0", "local-instance", "DC1"));
            assertThat(logProvider(commitLogDir).logs().map(CommitLog::segmentId).collect(Collectors.toSet()).contains(endMarker.segmentId)).isTrue();
        }
        finally
        {
            CdcTester.closeQuietly(commitLog);
            IOUtils.clearDirectory(commitLogDir, path -> LOGGER.info("Clearing test output path={}", path.toString()));
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testRefreshSchemaUpdatesBridgeWithAllTablesButFiltersCdcEnabledTables(CassandraVersion version)
    {
        // Regression test for the CDC batch-write bug: refreshSchema() must register EVERY
        // table returned by the schema supplier (CDC-enabled and CDC-disabled) with the
        // bridge's Schema.instance — not just the CDC-enabled ones — so that a commit log
        // Mutation spanning both a CDC-enabled and a CDC-disabled table in the same keyspace
        // (e.g. a BEGIN BATCH statement) can still be deserialized. Separately, only the
        // CDC-enabled tables should end up in cdcEnabledTables, since that set drives
        // publishing/replica-check decisions, not schema completeness.
        TestSchema cdcSchema = TestSchema.builder(bridge)
                                         .withPartitionKey("pk", bridge.uuid())
                                         .withColumn("val", bridge.text())
                                         .withCdc(true)
                                         .build();
        CqlTable cdcTable = cdcSchema.buildTable();

        TestSchema nonCdcSchema = TestSchema.builder(bridge)
                                            .withKeyspace(cdcSchema.keyspace)
                                            .withPartitionKey("pk", bridge.uuid())
                                            .withColumn("val", bridge.text())
                                            .withCdc(false)
                                            .build();
        CqlTable nonCdcTable = nonCdcSchema.buildTable();

        AtomicReference<Set<CqlTable>> tablesPassedToBridge = new AtomicReference<>();
        SchemaSupplier schemaSupplier = () -> CompletableFuture.completedFuture(ImmutableSet.of(cdcTable, nonCdcTable));

        try (RecordingCdc cdc = new RecordingCdc(Cdc.builder("test-refresh-schema", 0, event -> { }, schemaSupplier)
                                                    .withExecutor(CdcTests.ASYNC_EXECUTOR)
                                                    .withTableIdLookup(cdcBridge.internalTableIdLookup())
                                                    .withCommitLogProvider(tokenRange -> Stream.empty())
                                                    .withCdcOptions(cdcOptions),
                                                    tablesPassedToBridge))
        {
            // start() flips isRunning to true and calls refreshSchema() synchronously (the
            // schemaSupplier future is already completed, so the .handle()/.whenComplete()
            // chain runs inline). scheduleRun()/scheduleMonitorSchema() are overridden to
            // no-ops in RecordingCdc, so this only exercises the schema refresh path.
            cdc.start();

            assertThat(tablesPassedToBridge.get())
                .as("Schema.instance must be updated with ALL tables (CDC and non-CDC) so a "
                  + "batch mutation spanning both can still deserialize")
                .containsExactlyInAnyOrder(cdcTable, nonCdcTable);

            assertThat(cdc.cdcEnabledTables)
                .as("cdcEnabledTables (used for publishing/replica-check decisions) must contain "
                  + "only the CDC-enabled table, even though Schema.instance was given both")
                .containsExactly(cdcTable);
        }
    }

    /**
     * Test-only {@link Cdc} subclass that intercepts {@link #cdcBridge()} to record exactly
     * what table set gets passed to {@code CdcBridge.updateCdcSchema(...)} during
     * {@code refreshSchema()}, while still delegating to the real bridge so Schema.instance is
     * genuinely updated (not just observed). scheduleRun()/scheduleMonitorSchema() are
     * overridden to no-ops so start() only exercises the schema refresh path, without kicking
     * off an unrelated micro-batch read or a recurring schema-refresh loop.
     */
    private static final class RecordingCdc extends Cdc
    {
        private final AtomicReference<Set<CqlTable>> tablesPassedToBridge;

        RecordingCdc(CdcBuilder builder, AtomicReference<Set<CqlTable>> tablesPassedToBridge)
        {
            super(builder);
            this.tablesPassedToBridge = tablesPassedToBridge;
        }

        @Override
        protected void scheduleRun(long delayMillis)
        {
            // no-op — this test only exercises schema refresh, not the micro-batch read loop
        }

        @Override
        public void scheduleMonitorSchema()
        {
            // no-op — avoid recursively rescheduling refreshSchema() after start() calls it
        }

        @Override
        protected CdcBridge cdcBridge()
        {
            CdcBridge real = super.cdcBridge();
            return new RecordingCdcBridge(real, tablesPassedToBridge);
        }
    }

    /**
     * Delegates every call to the real {@link CdcBridge}, except {@code updateCdcSchema}, whose
     * argument is captured before delegating.
     */
    private static final class RecordingCdcBridge extends CdcBridge
    {
        private final CdcBridge delegate;
        private final AtomicReference<Set<CqlTable>> tablesPassedToBridge;

        RecordingCdcBridge(CdcBridge delegate, AtomicReference<Set<CqlTable>> tablesPassedToBridge)
        {
            this.delegate = delegate;
            this.tablesPassedToBridge = tablesPassedToBridge;
        }

        @Override
        public void updateCdcSchema(@NotNull Set<CqlTable> cdcTables,
                                    @NotNull Partitioner partitioner,
                                    @NotNull TableIdLookup tableIdLookup)
        {
            tablesPassedToBridge.set(cdcTables);
            delegate.updateCdcSchema(cdcTables, partitioner, tableIdLookup);
        }

        @Override
        public void unregisterNonCdcTables(@NotNull Set<TableIdentifier> tables)
        {
            delegate.unregisterNonCdcTables(tables);
        }

        @Override
        public CommitLogInstance createCommitLogInstance(Path path)
        {
            return delegate.createCommitLogInstance(path);
        }

        @Override
        public TableIdLookup internalTableIdLookup()
        {
            return delegate.internalTableIdLookup();
        }

        @Override
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
            return delegate.readLog(log, tokenRange, markers, partitionId, stats, executor, listener, startTimestampMicros, readCommitLogHeader);
        }

        @Override
        public CdcStreamScanner openCdcStreamScanner(Collection<PartitionUpdateWrapper> updates,
                                                     @NotNull CdcState endState,
                                                     Random random,
                                                     CassandraSource cassandraSource,
                                                     double traceSampleRate)
        {
            return delegate.openCdcStreamScanner(updates, endState, random, cassandraSource, traceSampleRate);
        }

        @Override
        public void log(TimeProvider timeProvider, CqlTable cqlTable, CommitLogInstance log, Row row, long timestamp)
        {
            delegate.log(cqlTable, log, row, timestamp);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testSinglePartitionKey(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert((type, random) ->
                         testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                             .withPartitionKey("pk", bridge.uuid())
                                                                             .withColumn("c1", bridge.bigint())
                                                                             .withColumn("c2", type))
                         .withRandom(random)
                         .withCdcEventChecker((testRows, events) -> {
                             assertThat(events.isEmpty()).isFalse();
                             for (CdcEvent event : events)
                             {
                                 assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                                 assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                                 assertThat(event.getClusteringKeys()).isNull();
                                 assertThat(event.getStaticColumns()).isNull();
                                 assertThat(event.getValueColumns().stream()
                                                   .map(v -> v.columnName)
                                                   .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                                 assertThat(event.getTtl()).isNull();
                             }
                         })
                         .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testClusteringKey(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge), qtRandom())
            .assuming((type, random) -> type.supportedAsPrimaryKeyColumn())
            .checkAssert((type, random) ->
                         testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                             .withPartitionKey("pk", bridge.uuid())
                                                                             .withClusteringKey("ck", type)
                                                                             .withColumn("c1", bridge.bigint())
                                                                             .withColumn("c2", bridge.text()))
                         .withRandom(random)
                         .withCdcEventChecker((testRows, events) -> {
                             for (CdcEvent event : events)
                             {
                                 assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                                 assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                                 assertThat(event.getClusteringKeys().size()).isEqualTo(1);
                                 assertThat(event.getClusteringKeys().get(0).columnName).isEqualTo("ck");
                                 assertCqlTypeEquals(type.cqlName(), event.getClusteringKeys().get(0).columnType);
                                 assertThat(event.getStaticColumns()).isNull();
                                 assertThat(event.getValueColumns().stream()
                                                   .map(v -> v.columnName)
                                                   .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                                 assertThat(event.getTtl()).isNull();
                             }
                         })
                         .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMultipleClusteringKeys(CassandraVersion version)
    {
        qt().withExamples(50).forAll(cql3Type(bridge), cql3Type(bridge), cql3Type(bridge), qtRandom())
            .assuming((t1, t2, t3, random) -> t1.supportedAsPrimaryKeyColumn()
                                      && t2.supportedAsPrimaryKeyColumn()
                                      && t3.supportedAsPrimaryKeyColumn())
            .checkAssert(
            (t1, t2, t3, random) ->
            testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                .withPartitionKey("pk", bridge.uuid())
                                                                .withClusteringKey("ck1", t1)
                                                                .withClusteringKey("ck2", t2)
                                                                .withClusteringKey("ck3", t3)
                                                                .withColumn("c1", bridge.bigint())
                                                                .withColumn("c2", bridge.text()))
            .withRandom(random)
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                    assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                    assertThat(event.getClusteringKeys().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("ck1", "ck2", "ck3"));
                    assertCqlTypeEquals(t1.cqlName(), event.getClusteringKeys().get(0).columnType);
                    assertCqlTypeEquals(t2.cqlName(), event.getClusteringKeys().get(1).columnType);
                    assertCqlTypeEquals(t3.cqlName(), event.getClusteringKeys().get(2).columnType);
                    assertThat(event.getStaticColumns()).isNull();
                    assertThat(event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                    assertThat(event.getTtl()).isNull();
                }
            })
            .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testSet(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge), qtRandom())
            .assuming((t, random) -> t.supportedAsSetElement())
            .checkAssert(
            (t, random) -> testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                     .withPartitionKey("pk", bridge.uuid())
                                                                     .withColumn("c1", bridge.bigint())
                                                                     .withColumn("c2", bridge.set(t)))
                 .withRandom(random)
                 .withCdcEventChecker((testRows, events) -> {
                     for (CdcEvent event : events)
                     {
                         assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                         assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                         assertThat(event.getClusteringKeys()).isNull();
                         assertThat(event.getStaticColumns()).isNull();
                         assertThat(event.getValueColumns().stream()
                                           .map(v -> v.columnName)
                                           .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                         Value setValue = event.getValueColumns().get(1);
                         String setType = setValue.columnType;
                         assertThat(setType.startsWith("set<")).isTrue();
                         assertCqlTypeEquals(t.cqlName(),
                                             setType.substring(4, setType.length() - 1)); // extract the type in set<>
                         Object v = bridge.parseType(setType).deserializeToJavaType(setValue.getValue());
                         assertThat(v).isInstanceOf(Set.class);
                         Set set = (Set) v;
                         assertThat(set.isEmpty()).isFalse();
                         assertThat(event.getTtl()).isNull();
                     }
                 })
                 .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testList(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert(
            (t, random) ->
            testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                .withPartitionKey("pk", bridge.uuid())
                                                                .withColumn("c1", bridge.bigint())
                                                                .withColumn("c2", bridge.list(bridge.aInt())))
            .withRandom(random)
            .withCassandraSource((keyspace, table, columnsToFetch, primaryKeyColumns) -> {
                // mutations to unfrozen lists require reading the full list from Cassandra
                List<ByteBuffer> byteBuffers = new ArrayList<>();
                byteBuffers.add(ByteBufferUtil.bytes(1));
                byteBuffers.add(ByteBufferUtil.bytes(2));
                byteBuffers.add(ByteBufferUtil.bytes(3));
                byteBuffers.add(ByteBufferUtil.bytes(4));
                return ImmutableList.of(CollectionSerializer.pack(byteBuffers, ByteBufferAccessor.instance, byteBuffers.size(), ProtocolVersion.V3));
            })
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                    assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                    assertThat(event.getClusteringKeys()).isNull();
                    assertThat(event.getStaticColumns()).isNull();
                    assertThat(event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                    Value listValue = event.getValueColumns().get(1);
                    String listType = listValue.columnType;
                    assertThat(listType.startsWith("list<")).isTrue();
                    assertCqlTypeEquals(bridge.aInt().cqlName(),
                                        listType.substring(5, listType.length() - 1)); // extract the type in list<>
                    Object v = bridge.parseType(listType).deserializeToJavaType(listValue.getValue());
                    assertThat(v).isInstanceOf(List.class);
                    List list = (List) v;
                    assertThat(list).isEqualTo(Arrays.asList(1, 2, 3, 4));
                    assertThat(event.getTtl()).isNull();
                }
            })
            .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testVector(CassandraVersion version)
    {
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().forAll(cql3Type(bridge), qtRandom())
            // Cassandra VectorType does not support swapping custom subtype serializer,
            // so we cannot use AnalyticsTimeUUIDSerializer or AnalyticsDurationSerializer.
            .assuming((t, random) -> !t.cqlName().equals(Duration.INSTANCE.name()) && !t.cqlName().equals(TimeUUID.INSTANCE.name()))
            .checkAssert(
            (t, random) ->
            testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                .withPartitionKey("pk", bridge.uuid())
                                                                .withColumn("c1", bridge.bigint())
                                                                .withColumn("c2", bridge.vector(t, 5)))
            .withRandom(random)
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                    assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                    assertThat(event.getClusteringKeys()).isNull();
                    assertThat(event.getStaticColumns()).isNull();
                    assertThat(event.getValueColumns().stream()
                                    .map(v -> v.columnName)
                                    .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                    Value vectorValue = event.getValueColumns().get(1);
                    String vectorType = vectorValue.columnType;
                    assertThat(vectorType.startsWith("vector<")).isTrue();
                    assertThat(vectorType.endsWith(">")).isTrue();
                    assertCqlTypeEquals(t.cqlName(),
                                        vectorType.substring(vectorType.indexOf("<") + 1, vectorType.indexOf(","))); // extract the type in vector<?, ?>
                    String dimensions = StringUtils.substringAfter(vectorType, ",");
                    dimensions = dimensions.substring(0, dimensions.length() - 1).trim();
                    assertThat(dimensions).isEqualTo("5");
                    Object v = bridge.parseType(vectorType).deserializeToJavaType(vectorValue.getValue());
                    assertThat(v).isInstanceOf(List.class);
                    List list = (List) v;
                    assertThat(list.size()).isGreaterThan(0);
                    assertThat(event.getTtl()).isNull();
                }
            })
            .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMap(CassandraVersion version)
    {
        qt().withExamples(50).forAll(cql3Type(bridge), cql3Type(bridge), qtRandom())
            .assuming((t1, t2, random) -> t1.supportedAsMapKey() && t2.supportedAsMapKey())
            .checkAssert(
            (t1, t2, random) -> testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                            .withPartitionKey("pk", bridge.uuid())
                                                                            .withColumn("c1", bridge.bigint())
                                                                            .withColumn("c2", bridge.map(t1, t2)))
                        .withRandom(random)
                        .withCdcEventChecker((testRows, events) -> {
                            for (CdcEvent event : events)
                            {
                                assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                                assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                                assertThat(event.getClusteringKeys()).isNull();
                                assertThat(event.getStaticColumns()).isNull();
                                assertThat(event.getValueColumns().stream()
                                                  .map(v -> v.columnName)
                                                  .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                                Value mapValue = event.getValueColumns().get(1);
                                String mapType = mapValue.columnType;
                                assertThat(mapType.startsWith("map<")).isTrue();
                                int commaIndex = mapType.indexOf(',');
                                assertCqlTypeEquals(t1.cqlName(),
                                                    // extract the key type in map<>
                                                    mapType.substring(4, commaIndex)); // extract the key type in map<>
                                assertCqlTypeEquals(t2.cqlName(),
                                                    // extract the value type in map<>; +2 to exclude , and the following space
                                                    mapType.substring(commaIndex + 2, mapType.length() - 1));
                                Object v = bridge.parseType(mapType).deserializeToJavaType(mapValue.getValue());
                                assertThat(v).isInstanceOf(Map.class);
                                Map map = (Map) v;
                                assertThat(map.size()).isGreaterThan(0);
                                assertThat(event.getTtl()).isNull();
                            }
                        })
                        .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMultiTable(CassandraVersion version)
    {
        TestSchema.Builder tableBuilder1 = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withClusteringKey("ck1", bridge.text())
                                                     .withColumn("c1", bridge.bigint())
                                                     .withColumn("c2", bridge.text())
                                                     .withCdc(true);
        TestSchema.Builder tableBuilder2 = TestSchema.builder(bridge)
                                                     .withPartitionKey("a", bridge.aInt())
                                                     .withPartitionKey("b", bridge.timeuuid())
                                                     .withClusteringKey("c", bridge.text())
                                                     .withClusteringKey("d", bridge.bigint())
                                                     .withColumn("e", bridge.map(bridge.aInt(), bridge.text()))
                                                     .withCdc(true);
        TestSchema.Builder tableBuilder3 = TestSchema.builder(bridge)
                                                     .withPartitionKey("c1", bridge.text())
                                                     .withClusteringKey("c2", bridge.aInt())
                                                     .withColumn("c3", bridge.set(bridge.bigint()))
                                                     .withCdc(false);
        TestSchema schema2 = tableBuilder2.build();
        TestSchema schema3 = tableBuilder3.build();
        CqlTable cqlTable2 = schema2.buildTable();
        CqlTable cqlTable3 = schema3.buildTable();
        bridge.buildSchema(cqlTable2.createStatement(),
                           cqlTable2.keyspace(),
                           ReplicationFactor.simpleStrategy(1),
                           Partitioner.Murmur3Partitioner,
                           Collections.emptySet(),
                           null, 0, schema2.withCdc);
        bridge.buildSchema(cqlTable3.createStatement(),
                           cqlTable3.keyspace(),
                           ReplicationFactor.simpleStrategy(1),
                           Partitioner.Murmur3Partitioner,
                           Collections.emptySet(),
                           null, 0, schema3.withCdc);
        int numRows = DEFAULT_NUM_ROWS;
        Random random = new Random();

        AtomicReference<TestSchema> schema1Holder = new AtomicReference<>();
        CdcTester.Builder testBuilder = CdcTester.builder(bridge, cdcBridge, tableBuilder1, commitLogDir)
                                                 .withRandom(random)
                                                 .clearWriters()
                                                 .withWriter((tester, rows, writer) -> {
                                                     for (int i = 0; i < numRows; i++)
                                                     {
                                                         writer.accept(CdcTester.newUniqueRow(tester.schema, rows, random),
                                                                       TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                     }
                                                 })
                                                 .withWriter(new CdcWriter()
                                                 {
                                                     public void write(CdcTester tester,
                                                                       Map<String, TestSchema.TestRow> rows,
                                                                       BiConsumer<TestSchema.TestRow, Long> writer)
                                                     {
                                                         Map<String, TestSchema.TestRow> prevRows = new HashMap<>(numRows);
                                                         for (int i = 0; i < numRows; i++)
                                                         {
                                                             writer.accept(CdcTester.newUniqueRow(schema2, prevRows, random),
                                                                           TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                         }
                                                     }

                                                     public CqlTable cqlTable(CdcTester tester)
                                                     {
                                                         return cqlTable2;
                                                     }
                                                 })
                                                 .withWriter(new CdcWriter()
                                                 {
                                                     public void write(CdcTester tester,
                                                                       Map<String, TestSchema.TestRow> rows,
                                                                       BiConsumer<TestSchema.TestRow, Long> writer)
                                                     {
                                                         Map<String, TestSchema.TestRow> prevRows = new HashMap<>(numRows);
                                                         for (int i = 0; i < numRows; i++)
                                                         {
                                                             writer.accept(CdcTester.newUniqueRow(schema3, prevRows, random),
                                                                           TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                         }
                                                     }

                                                     public CqlTable cqlTable(CdcTester tester)
                                                     {
                                                         return cqlTable3;
                                                     }
                                                 })
                                                 .withExpectedNumRows(numRows * 2)
                                                 .withCdcEventChecker((testRows, events) -> {
                                                     TestSchema schema1 = schema1Holder.get();
                                                     assertThat(events.size()).isEqualTo(numRows * 2);
                                                     assertThat(events.stream()
                                                                                 .filter(f -> f.keyspace.equals(schema1.keyspace))
                                                                                 .filter(f -> f.table.equals(schema1.table)).count()).isEqualTo(numRows);
                                                     assertThat(events.stream()
                                                                                 .filter(f -> f.keyspace.equals(schema2.keyspace))
                                                                                 .filter(f -> f.table.equals(schema2.table)).count()).isEqualTo(numRows);
                                                     assertThat(events.stream()
                                                                           .filter(f -> f.keyspace.equals(schema3.keyspace))
                                                                           .filter(f -> f.table.equals(schema3.table)).count()).isEqualTo(0);
                                                 });
        CdcTester cdcTester = testBuilder.build();
        schema1Holder.set(cdcTester.schema);
        cdcTester.run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testUpdateStaticColumnOnly(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge).zip(arbitrary().enumValues(OperationType.class), Pair::of), qtRandom())
            .checkAssert((cql3TypeAndInsertFlag, random) -> {
                CqlField.NativeType cqlType = cql3TypeAndInsertFlag._1;
                OperationType insertOrUpdate = cql3TypeAndInsertFlag._2;
                testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                    .withPartitionKey("pk", bridge.uuid())
                                                                    .withClusteringKey("ck", bridge.uuid())
                                                                    .withStaticColumn("sc", cqlType))
                .withRandom(random)
                .clearWriters()
                .withWriter(((tester, rows, writer) -> {
                    long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    IntStream.range(0, tester.numRows)
                             .forEach(i -> {
                                 TestSchema.TestRow row = newUniqueRow(tester.schema, rows, random);
                                 row = row.copy(1, null);
                                 insertOrUpdate.accept(row);
                                 writer.accept(row, timestampMicros);
                             });
                }))
                .withCdcEventChecker((testRows, events) -> {
                    for (CdcEvent event : events)
                    {
                        assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                        Value pk = event.getPartitionKeys().get(0);
                        assertThat(pk.columnName).isEqualTo("pk");
                        assertThat(event.getClusteringKeys()).isNull();
                        assertThat(event.getValueColumns()).isNull();
                        assertThat(event.getStaticColumns().size()).isEqualTo(1);
                        Value sc = event.getStaticColumns().get(0);
                        assertThat(sc.columnName).isEqualTo("sc");
                        TestSchema.TestRow testRow = testRows.get(ByteBufferUtils.toHexString(pk.getValue()) + ":null:");
                        assertThat(cqlType.deserializeToJavaType(sc.getValue())).isEqualTo(testRow.get(2));
                    }
                })
                .run();
            });
    }

    // Test mutations that partially update are correctly reflected in the cdc event.
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testUpdatePartialColumns(CassandraVersion version)
    {
        Set<UUID> ttlRowIdx = new HashSet<>();
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert((type, random) -> {
                ttlRowIdx.clear();
                testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                    .withPartitionKey("pk", bridge.uuid())
                                                                    .withColumn("c1", bridge.bigint())
                                                                    .withColumn("c2", type))
                .withRandom(random)
                .clearWriters()
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    long time = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = newUniqueRow(tester.schema, rows, random);
                        // mark c1 as not updated / unset
                        testRow = testRow.copy("c1", CdcBridge.UNSET_MARKER);
                        if (random.nextDouble() > 0.5)
                        {
                            testRow.setTTL(TTL);
                            ttlRowIdx.add(testRow.getUUID("pk"));
                        }
                        writer.accept(testRow, time++);
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (CdcEvent event : events)
                    {
                        assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                        assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                        UUID pk = (UUID) messageConverter.toCdcMessage(event.getPartitionKeys().get(0)).value();
                        assertThat(event.getClusteringKeys()).isNull();
                        assertThat(event.getStaticColumns()).isNull();
                        assertThat(event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList())).isEqualTo(ImmutableList.of("c2"));

                        if (ttlRowIdx.contains(pk))
                        {
                            assertThat(event.getTtl()).isNotNull();
                            assertThat(event.getTtl().ttlInSec).isEqualTo(TTL);
                        }
                        else
                        {
                            assertThat(event.getTtl()).isNull();
                        }
                    }
                })
                .run();
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testCellDeletion(CassandraVersion version)
    {
        // The test write cell-level tombstones,
        // i.e. deleting one or more columns in a row, for cdc job to aggregate.
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert(
            (type, random) ->
            testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                .withPartitionKey("pk", bridge.uuid())
                                                                .withColumn("c1", bridge.bigint())
                                                                .withColumn("c2", type)
                                                                .withColumn("c3", bridge.list(type)))
            .withRandom(random)
            .clearWriters()
            .withWriter((tester, rows, writer) -> {
                for (int i = 0; i < tester.numRows; i++)
                {
                    TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                    testRow = testRow.copy("c1", CdcBridge.UNSET_MARKER); // mark c1 as not updated / unset
                    testRow = testRow.copy("c2", null); // delete c2
                    testRow = testRow.copy("c3", null); // delete c3
                    writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                }
            })
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.DELETE);
                    assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                    assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                    assertThat(event.getClusteringKeys()).isNull();
                    assertThat(event.getStaticColumns()).isNull();
                    assertThat(event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("c2", "c3")); // c1 is not updated
                    Value c2 = event.getValueColumns().get(0);
                    assertCqlTypeEquals(type.cqlName(), c2.columnType);
                    assertThat(event.getTtl()).isNull();
                }
            })
            .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testCompositePartitionKey(CassandraVersion version)
    {
        qt().forAll(cql3Type(bridge), qtRandom())
            .assuming((type, random) -> type.supportedAsPrimaryKeyColumn())
            .checkAssert(
            (type, random) ->
            testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                .withPartitionKey("pk1", bridge.uuid())
                                                                .withPartitionKey("pk2", type)
                                                                .withPartitionKey("pk3", bridge.timestamp())
                                                                .withColumn("c1", bridge.bigint())
                                                                .withColumn("c2", bridge.text()))
            .withRandom(random)
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertThat(event.getPartitionKeys().size()).isEqualTo(3);
                    assertThat(event.getPartitionKeys().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("pk1", "pk2", "pk3"));
                    assertThat(event.getClusteringKeys()).isNull();
                    assertThat(event.getStaticColumns()).isNull();
                    assertThat(event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                    assertThat(event.getTtl()).isNull();
                }
            })
            .run());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testUpdateFlag(CassandraVersion version)
    {
        qt().withExamples(10)
            .forAll(cql3Type(bridge), qtRandom())
            .checkAssert((type, random) -> {
                testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                    .withPartitionKey("pk", bridge.uuid())
                                                                    .withColumn("c1", bridge.aInt())
                                                                    .withColumn("c2", type))
                .withRandom(random)
                .clearWriters()
                .withNumRows(1000)
                .withWriter((tester, rows, writer) -> {
                    int halfway = tester.numRows / 2;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                        testRow = testRow.copy("c1", i);
                        if (i >= halfway)
                        {
                            testRow.fromUpdate();
                        }
                        testRow.setTTL(TTL);
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    int halfway = events.size() / 2;
                    for (CdcEvent event : events)
                    {
                        assertThat(event.getPartitionKeys().size()).isEqualTo(1);
                        assertThat(event.getPartitionKeys().get(0).columnName).isEqualTo("pk");
                        assertThat(event.getClusteringKeys()).isNull();
                        assertThat(event.getStaticColumns()).isNull();
                        assertThat(event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList())).isEqualTo(Arrays.asList("c1", "c2"));
                        ByteBuffer c1Bb = event.getValueColumns().get(0).getValue();
                        int i = (Integer) bridge.aInt().deserializeToJavaType(c1Bb);
                        CdcEvent.Kind expectedKind = i >= halfway
                                                     ? CdcEvent.Kind.UPDATE
                                                     : CdcEvent.Kind.INSERT;
                        assertThat(event.getKind()).isEqualTo(expectedKind);
                        assertThat(event.getTtl().ttlInSec).isEqualTo(TTL);
                    }
                })
                .run();
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMultipleWritesToSameKeyInBatch(CassandraVersion version)
    {
        // The test writes different groups of mutations.
        // Each group of mutations write to the same key with the different timestamp.
        // For CDC, it only deduplicate and emit the replicated mutations, i.e. they have the same writetime.
        qt().forAll(cql3Type(bridge), qtRandom())
            .checkAssert((type, random) -> {
                testWith(bridge, cdcBridge, commitLogDir, TestSchema.builder(bridge)
                                                                    .withPartitionKey("pk", bridge.uuid())
                                                                    .withColumn("c1", bridge.bigint())
                                                                    .withColumn("c2", type))
                .withRandom(random)
                .clearWriters()
                .withNumRows(1000)
                .withExpectedNumRows(2000)
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    // write initial values
                    long timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        writer.accept(CdcTester.newUniqueRow(tester.schema, rows, random), timestamp++);
                    }

                    // overwrite with new mutations at later timestamp
                    for (TestSchema.TestRow row : rows.values())
                    {
                        TestSchema.TestRow newUniqueRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                        for (CqlField field : tester.cqlTable.valueColumns())
                        {
                            // update value columns
                            row = row.copy(field.position(), newUniqueRow.get(field.position()));
                        }
                        row.setTTL(TTL);
                        writer.accept(row, timestamp++);
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    assertThat(events.size()).as("Each PK should get 2 mutations").isEqualTo(testRows.size() * 2);
                    long ts = -1L;
                    int partitions = testRows.size();
                    int i = 0;
                    for (CdcEvent event : events)
                    {
                        if (ts == -1L)
                        {
                            ts = event.getTimestamp(TimeUnit.MICROSECONDS);
                        }
                        else
                        {
                            long lastTs = ts;
                            ts = event.getTimestamp(TimeUnit.MICROSECONDS);
                            assertThat(lastTs < ts).as("Writetime should be monotonic increasing").isTrue();
                        }
                        if (i >= partitions) // the rows in the second batch has ttl specified.
                        {
                            assertThat(event.getTtl()).isNotNull();
                            assertThat(event.getTtl().ttlInSec).isEqualTo(TTL);
                        }
                        i++;
                    }
                })
                .run();
            });
    }
}
