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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.StatePersister;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.msg.jdk.JdkMessageConverter;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaBuilder;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.api.Pair;

import static org.apache.cassandra.cdc.CdcTester.DEFAULT_NUM_ROWS;
import static org.apache.cassandra.cdc.CdcTester.assertCqlTypeEquals;
import static org.apache.cassandra.cdc.CdcTester.newUniqueRow;
import static org.apache.cassandra.cdc.CdcTester.testWith;
import static org.apache.cassandra.spark.CommonTestUtils.cql3Type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

@SuppressWarnings("DataFlowIssue")
public class CdcTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTests.class);
    public static final CdcOptions TEST_OPTIONS = new CdcOptions()
    {
        public int minimumReplicas(String keyspace)
        {
            return 1;
        }
    };
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
                                                                                new ThreadFactoryBuilder()
                                                                                .setNameFormat("cdc-io-%d")
                                                                                .setDaemon(true)
                                                                                .build());
    public static final AsyncExecutor ASYNC_EXECUTOR = AsyncExecutor.wrap(EXECUTOR);
    public static final CassandraBridge BRIDGE = new CassandraBridgeImplementation();
    public static final JdkMessageConverter MESSAGE_CONVERTER = new JdkMessageConverter(BRIDGE.cassandraTypes());
    public static final CdcBridge CDC_BRIDGE = new CdcBridgeImplementation();

    private static final int TTL = 42;

    public static Path directory;
    private static volatile boolean isSetup = false;

    static
    {
        setup();
    }

    public static synchronized void setup()
    {
        if (isSetup)
        {
            return;
        }
        try
        {
            directory = Files.createTempDirectory(UUID.randomUUID().toString());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        CdcTester.setup(directory);
        isSetup = true;
    }

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

    @Test
    public void testMockedCdc()
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
            TestSchema testSchema = TestSchema.basicBuilder(BRIDGE).build();

            CqlTable table = testSchema.buildTable();
            BRIDGE.buildSchema(table.createStatement(),
                               table.keyspace(),
                               table.replicationFactor(),
                               Partitioner.Murmur3Partitioner,
                               table.udtCreateStmts(BRIDGE.cassandraTypes()),
                               null,
                               0,
                               true);
            UUID tableId = Schema.instance.getTableMetadata(table.keyspace(), table.table()).id.asUUID();
            SchemaSupplier schemaSupplier = () -> CompletableFuture.completedFuture(ImmutableSet.of(table));
            final AtomicReference<byte[]> state = new AtomicReference<>();
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
                    return Collections.singletonList(CdcState.deserialize(CdcKryoRegister.kryo(), BRIDGE.compressionUtil(), state.get()));
                }
            };

            Map<String, TestSchema.TestRow> writtenRows = new HashMap<>();
            Runnable writer = () -> {
                IntStream.range(0, batchSize)
                         .forEach(i -> {
                             TestSchema.TestRow testRow = CdcTester.newUniqueRow(testSchema, writtenRows);
                             CDC_BRIDGE.log(table, CdcTester.testCommitLog, testRow, TimeUtils.nowMicros());
                             writtenRows.put(testRow.getPrimaryHexKey(), testRow);
                         });
                CdcTester.testCommitLog.sync();
            };

            final long startTime = System.currentTimeMillis();
            try (Cdc cdc = Cdc.builder("101", 0, eventConsumer, schemaSupplier)
                              .withExecutor(CdcTests.ASYNC_EXECUTOR)
                              .withStatePersister(statePersister)
                              .withTableIdLookup((ks, tb) -> tableId)
                              .withCommitLogProvider(CdcTests.logProvider(CdcTests.directory))
                              .withCdcOptions(CdcTests.TEST_OPTIONS)
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

                assertEquals(writtenRows.size(), seenMutations.size());
                assertEquals(writtenRows.values()
                                        .stream()
                                        .map(TestSchema.TestRow::getPrimaryHexKey)
                                        .collect(Collectors.toSet()),
                             seenMutations);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            // verify state is correct
            CdcState endState = statePersister.loadCanonicalState("101", 0, null);
            long numSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
            assertTrue(endState.epoch >= Math.max(0, numSeconds - 4)); // epochs should be around ~ 1 per second
            assertTrue(endState.replicaCount.isEmpty());
            Marker endMarker = endState.markers.startMarker(new CassandraInstance("0", "local-instance", "DC1"));
            assertTrue(logProvider(directory).logs().map(CommitLog::segmentId).collect(Collectors.toSet()).contains(endMarker.segmentId));
        }
        finally
        {
            CdcTester.tearDown();
            IOUtils.clearDirectory(directory, path -> LOGGER.info("Clearing test output path={}", path.toString()));
            CdcTester.testCommitLog.start();
        }
    }

    @Test
    public void testSinglePartitionKey()
    {
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(type ->
                         testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                               .withPartitionKey("pk", BRIDGE.uuid())
                                                               .withColumn("c1", BRIDGE.bigint())
                                                               .withColumn("c2", type))
                         .withCdcEventChecker((testRows, events) -> {
                             assertFalse(events.isEmpty());
                             for (CdcEvent event : events)
                             {
                                 assertEquals(1, event.getPartitionKeys().size());
                                 assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                                 assertNull(event.getClusteringKeys());
                                 assertNull(event.getStaticColumns());
                                 assertEquals(Arrays.asList("c1", "c2"),
                                              event.getValueColumns().stream()
                                                   .map(v -> v.columnName)
                                                   .collect(Collectors.toList()));
                                 assertNull(event.getTtl());
                             }
                         })
                         .run());
    }

    @Test
    public void testClusteringKey()
    {
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(type ->
                         testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                               .withPartitionKey("pk", BRIDGE.uuid())
                                                               .withClusteringKey("ck", type)
                                                               .withColumn("c1", BRIDGE.bigint())
                                                               .withColumn("c2", BRIDGE.text()))
                         .withCdcEventChecker((testRows, events) -> {
                             for (CdcEvent event : events)
                             {
                                 assertEquals(1, event.getPartitionKeys().size());
                                 assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                                 assertEquals(1, event.getClusteringKeys().size());
                                 assertEquals("ck", event.getClusteringKeys().get(0).columnName);
                                 assertCqlTypeEquals(type.cqlName(), event.getClusteringKeys().get(0).columnType);
                                 assertNull(event.getStaticColumns());
                                 assertEquals(Arrays.asList("c1", "c2"),
                                              event.getValueColumns().stream()
                                                   .map(v -> v.columnName)
                                                   .collect(Collectors.toList()));
                                 assertNull(event.getTtl());
                             }
                         })
                         .run());
    }

    @Test
    public void testMultipleClusteringKeys()
    {
        qt().withExamples(50).forAll(cql3Type(BRIDGE), cql3Type(BRIDGE), cql3Type(BRIDGE))
            .checkAssert(
            (t1, t2, t3) ->
            testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                  .withPartitionKey("pk", BRIDGE.uuid())
                                                  .withClusteringKey("ck1", t1)
                                                  .withClusteringKey("ck2", t2)
                                                  .withClusteringKey("ck3", t3)
                                                  .withColumn("c1", BRIDGE.bigint())
                                                  .withColumn("c2", BRIDGE.text()))
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertEquals(1, event.getPartitionKeys().size());
                    assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                    assertEquals(Arrays.asList("ck1", "ck2", "ck3"),
                                 event.getClusteringKeys().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    assertCqlTypeEquals(t1.cqlName(), event.getClusteringKeys().get(0).columnType);
                    assertCqlTypeEquals(t2.cqlName(), event.getClusteringKeys().get(1).columnType);
                    assertCqlTypeEquals(t3.cqlName(), event.getClusteringKeys().get(2).columnType);
                    assertNull(event.getStaticColumns());
                    assertEquals(Arrays.asList("c1", "c2"),
                                 event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    assertNull(event.getTtl());
                }
            })
            .run());
    }

    @Test
    public void testSet()
    {
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            t -> testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                       .withPartitionKey("pk", BRIDGE.uuid())
                                                       .withColumn("c1", BRIDGE.bigint())
                                                       .withColumn("c2", BRIDGE.set(t)))
                 .withCdcEventChecker((testRows, events) -> {
                     for (CdcEvent event : events)
                     {
                         assertEquals(1, event.getPartitionKeys().size());
                         assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                         assertNull(event.getClusteringKeys());
                         assertNull(event.getStaticColumns());
                         assertEquals(Arrays.asList("c1", "c2"),
                                      event.getValueColumns().stream()
                                           .map(v -> v.columnName)
                                           .collect(Collectors.toList()));
                         Value setValue = event.getValueColumns().get(1);
                         String setType = setValue.columnType;
                         assertTrue(setType.startsWith("set<"));
                         assertCqlTypeEquals(t.cqlName(),
                                             setType.substring(4, setType.length() - 1)); // extract the type in set<>
                         Object v = BRIDGE.parseType(setType).deserializeToJavaType(setValue.getValue());
                         assertInstanceOf(Set.class, v);
                         Set set = (Set) v;
                         assertFalse(set.isEmpty());
                         assertNull(event.getTtl());
                     }
                 })
                 .run());
    }

    @Test
    public void testList()
    {
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            t ->
            testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                  .withPartitionKey("pk", BRIDGE.uuid())
                                                  .withColumn("c1", BRIDGE.bigint())
                                                  .withColumn("c2", BRIDGE.list(BRIDGE.aInt())))
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
                    assertEquals(1, event.getPartitionKeys().size());
                    assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                    assertNull(event.getClusteringKeys());
                    assertNull(event.getStaticColumns());
                    assertEquals(Arrays.asList("c1", "c2"),
                                 event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    Value listValue = event.getValueColumns().get(1);
                    String listType = listValue.columnType;
                    assertTrue(listType.startsWith("list<"));
                    assertCqlTypeEquals(BRIDGE.aInt().cqlName(),
                                        listType.substring(5, listType.length() - 1)); // extract the type in list<>
                    Object v = BRIDGE.parseType(listType).deserializeToJavaType(listValue.getValue());
                    assertInstanceOf(List.class, v);
                    List list = (List) v;
                    assertEquals(Arrays.asList(1, 2, 3, 4), list);
                    assertNull(event.getTtl());
                }
            })
            .run());
    }

    @Test
    public void testMap()
    {
        qt().withExamples(50).forAll(cql3Type(BRIDGE), cql3Type(BRIDGE))
            .checkAssert(
            (t1, t2) -> testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                              .withPartitionKey("pk", BRIDGE.uuid())
                                                              .withColumn("c1", BRIDGE.bigint())
                                                              .withColumn("c2", BRIDGE.map(t1, t2)))
                        .withCdcEventChecker((testRows, events) -> {
                            for (CdcEvent event : events)
                            {
                                assertEquals(1, event.getPartitionKeys().size());
                                assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                                assertNull(event.getClusteringKeys());
                                assertNull(event.getStaticColumns());
                                assertEquals(Arrays.asList("c1", "c2"),
                                             event.getValueColumns().stream()
                                                  .map(v -> v.columnName)
                                                  .collect(Collectors.toList()));
                                Value mapValue = event.getValueColumns().get(1);
                                String mapType = mapValue.columnType;
                                assertTrue(mapType.startsWith("map<"));
                                int commaIndex = mapType.indexOf(',');
                                assertCqlTypeEquals(t1.cqlName(),
                                                    // extract the key type in map<>
                                                    mapType.substring(4, commaIndex)); // extract the key type in map<>
                                assertCqlTypeEquals(t2.cqlName(),
                                                    // extract the value type in map<>; +2 to exclude , and the following space
                                                    mapType.substring(commaIndex + 2, mapType.length() - 1));
                                Object v = BRIDGE.parseType(mapType).deserializeToJavaType(mapValue.getValue());
                                assertInstanceOf(Map.class, v);
                                Map map = (Map) v;
                                assertTrue(map.size() > 0);
                                assertNull(event.getTtl());
                            }
                        })
                        .run());
    }

    @Test
    public void testMultiTable()
    {
        TestSchema.Builder tableBuilder1 = TestSchema.builder(BRIDGE)
                                                     .withPartitionKey("pk", BRIDGE.uuid())
                                                     .withClusteringKey("ck1", BRIDGE.text())
                                                     .withColumn("c1", BRIDGE.bigint())
                                                     .withColumn("c2", BRIDGE.text())
                                                     .withCdc(true);
        TestSchema.Builder tableBuilder2 = TestSchema.builder(BRIDGE)
                                                     .withPartitionKey("a", BRIDGE.aInt())
                                                     .withPartitionKey("b", BRIDGE.timeuuid())
                                                     .withClusteringKey("c", BRIDGE.text())
                                                     .withClusteringKey("d", BRIDGE.bigint())
                                                     .withColumn("e", BRIDGE.map(BRIDGE.aInt(), BRIDGE.text()))
                                                     .withCdc(true);
        TestSchema.Builder tableBuilder3 = TestSchema.builder(BRIDGE)
                                                     .withPartitionKey("c1", BRIDGE.text())
                                                     .withClusteringKey("c2", BRIDGE.aInt())
                                                     .withColumn("c3", BRIDGE.set(BRIDGE.bigint()))
                                                     .withCdc(false);
        TestSchema schema2 = tableBuilder2.build();
        TestSchema schema3 = tableBuilder3.build();
        CqlTable cqlTable2 = schema2.buildTable();
        CqlTable cqlTable3 = schema3.buildTable();
        new SchemaBuilder(cqlTable2, Partitioner.Murmur3Partitioner, schema2.withCdc);
        new SchemaBuilder(cqlTable3, Partitioner.Murmur3Partitioner, schema3.withCdc);
        int numRows = DEFAULT_NUM_ROWS;

        AtomicReference<TestSchema> schema1Holder = new AtomicReference<>();
        CdcTester.Builder testBuilder = CdcTester.builder(BRIDGE, tableBuilder1, directory)
                                                 .clearWriters()
                                                 .withWriter((tester, rows, writer) -> {
                                                     for (int i = 0; i < numRows; i++)
                                                     {
                                                         writer.accept(CdcTester.newUniqueRow(tester.schema, rows),
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
                                                             writer.accept(CdcTester.newUniqueRow(schema2, prevRows),
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
                                                             writer.accept(CdcTester.newUniqueRow(schema3, prevRows),
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
                                                     assertEquals(numRows * 2, events.size());
                                                     assertEquals(numRows, events.stream()
                                                                                 .filter(f -> f.keyspace.equals(schema1.keyspace))
                                                                                 .filter(f -> f.table.equals(schema1.table)).count());
                                                     assertEquals(numRows, events.stream()
                                                                                 .filter(f -> f.keyspace.equals(schema2.keyspace))
                                                                                 .filter(f -> f.table.equals(schema2.table)).count());
                                                     assertEquals(0, events.stream()
                                                                           .filter(f -> f.keyspace.equals(schema3.keyspace))
                                                                           .filter(f -> f.table.equals(schema3.table)).count());
                                                 });
        CdcTester cdcTester = testBuilder.build();
        schema1Holder.set(cdcTester.schema);
        cdcTester.run();
    }

    @Test
    public void testUpdateStaticColumnOnly()
    {
        qt().forAll(cql3Type(BRIDGE).zip(arbitrary().enumValues(OperationType.class), Pair::of))
            .checkAssert(cql3TypeAndInsertFlag -> {
                CqlField.NativeType cqlType = cql3TypeAndInsertFlag._1;
                OperationType insertOrUpdate = cql3TypeAndInsertFlag._2;
                testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                      .withPartitionKey("pk", BRIDGE.uuid())
                                                      .withClusteringKey("ck", BRIDGE.uuid())
                                                      .withStaticColumn("sc", cqlType))
                .clearWriters()
                .withWriter(((tester, rows, writer) -> {
                    long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    IntStream.range(0, tester.numRows)
                             .forEach(i -> {
                                 TestSchema.TestRow row = newUniqueRow(tester.schema, rows);
                                 row = row.copy(1, null);
                                 insertOrUpdate.accept(row);
                                 writer.accept(row, timestampMicros);
                             });
                }))
                .withCdcEventChecker((testRows, events) -> {
                    for (CdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        Value pk = event.getPartitionKeys().get(0);
                        assertEquals("pk", pk.columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getValueColumns());
                        assertEquals(1, event.getStaticColumns().size());
                        Value sc = event.getStaticColumns().get(0);
                        assertEquals("sc", sc.columnName);
                        TestSchema.TestRow testRow = testRows.get(ByteBufferUtils.toHexString(pk.getValue()) + ":null:");
                        assertEquals(testRow.get(2), cqlType.deserializeToJavaType(sc.getValue()));
                    }
                })
                .run();
            });
    }

    // Test mutations that partially update are correctly reflected in the cdc event.
    @Test
    public void testUpdatePartialColumns()
    {
        Set<UUID> ttlRowIdx = new HashSet<>();
        Random rnd = new Random(1);
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(type -> {
                ttlRowIdx.clear();
                testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                      .withPartitionKey("pk", BRIDGE.uuid())
                                                      .withColumn("c1", BRIDGE.bigint())
                                                      .withColumn("c2", type))
                .clearWriters()
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    long time = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = newUniqueRow(tester.schema, rows);
                        // mark c1 as not updated / unset
                        testRow = testRow.copy("c1", CdcBridge.UNSET_MARKER);
                        if (rnd.nextDouble() > 0.5)
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
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        UUID pk = (UUID) MESSAGE_CONVERTER.toCdcMessage(event.getPartitionKeys().get(0)).value();
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(ImmutableList.of("c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));

                        if (ttlRowIdx.contains(pk))
                        {
                            assertNotNull(event.getTtl());
                            assertEquals(TTL, event.getTtl().ttlInSec);
                        }
                        else
                        {
                            assertNull(event.getTtl());
                        }
                    }
                })
                .run();
            });
    }

    @Test
    public void testCellDeletion()
    {
        // The test write cell-level tombstones,
        // i.e. deleting one or more columns in a row, for cdc job to aggregate.
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            type ->
            testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                  .withPartitionKey("pk", BRIDGE.uuid())
                                                  .withColumn("c1", BRIDGE.bigint())
                                                  .withColumn("c2", type)
                                                  .withColumn("c3", BRIDGE.list(type)))
            .clearWriters()
            .withWriter((tester, rows, writer) -> {
                for (int i = 0; i < tester.numRows; i++)
                {
                    TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows);
                    testRow = testRow.copy("c1", CdcBridge.UNSET_MARKER); // mark c1 as not updated / unset
                    testRow = testRow.copy("c2", null); // delete c2
                    testRow = testRow.copy("c3", null); // delete c3
                    writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                }
            })
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertEquals(CdcEvent.Kind.DELETE, event.getKind());
                    assertEquals(1, event.getPartitionKeys().size());
                    assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                    assertNull(event.getClusteringKeys());
                    assertNull(event.getStaticColumns());
                    assertEquals(Arrays.asList("c2", "c3"), // c1 is not updated
                                 event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    Value c2 = event.getValueColumns().get(0);
                    assertCqlTypeEquals(type.cqlName(), c2.columnType);
                    assertNull(event.getTtl());
                }
            })
            .run());
    }

    @Test
    public void testCompositePartitionKey()
    {
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(
            type ->
            testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                  .withPartitionKey("pk1", BRIDGE.uuid())
                                                  .withPartitionKey("pk2", type)
                                                  .withPartitionKey("pk3", BRIDGE.timestamp())
                                                  .withColumn("c1", BRIDGE.bigint())
                                                  .withColumn("c2", BRIDGE.text()))
            .withCdcEventChecker((testRows, events) -> {
                for (CdcEvent event : events)
                {
                    assertEquals(3, event.getPartitionKeys().size());
                    assertEquals(Arrays.asList("pk1", "pk2", "pk3"),
                                 event.getPartitionKeys().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    assertNull(event.getClusteringKeys());
                    assertNull(event.getStaticColumns());
                    assertEquals(Arrays.asList("c1", "c2"),
                                 event.getValueColumns().stream()
                                      .map(v -> v.columnName)
                                      .collect(Collectors.toList()));
                    assertNull(event.getTtl());
                }
            })
            .run());
    }

    @Test
    public void testUpdateFlag()
    {
        qt().withExamples(10)
            .forAll(cql3Type(BRIDGE))
            .checkAssert(type -> {
                testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                      .withPartitionKey("pk", BRIDGE.uuid())
                                                      .withColumn("c1", BRIDGE.aInt())
                                                      .withColumn("c2", type))
                .clearWriters()
                .withNumRows(1000)
                .withWriter((tester, rows, writer) -> {
                    int halfway = tester.numRows / 2;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows);
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
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        ByteBuffer c1Bb = event.getValueColumns().get(0).getValue();
                        int i = (Integer) BRIDGE.aInt().deserializeToJavaType(c1Bb);
                        CdcEvent.Kind expectedKind = i >= halfway
                                                     ? CdcEvent.Kind.UPDATE
                                                     : CdcEvent.Kind.INSERT;
                        assertEquals(expectedKind, event.getKind());
                        assertEquals(TTL, event.getTtl().ttlInSec);
                    }
                })
                .run();
            });
    }

    @Test
    public void testMultipleWritesToSameKeyInBatch()
    {
        // The test writes different groups of mutations.
        // Each group of mutations write to the same key with the different timestamp.
        // For CDC, it only deduplicate and emit the replicated mutations, i.e. they have the same writetime.
        qt().forAll(cql3Type(BRIDGE))
            .checkAssert(type -> {
                testWith(BRIDGE, directory, TestSchema.builder(BRIDGE)
                                                      .withPartitionKey("pk", BRIDGE.uuid())
                                                      .withColumn("c1", BRIDGE.bigint())
                                                      .withColumn("c2", type))
                .clearWriters()
                .withNumRows(1000)
                .withExpectedNumRows(2000)
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    // write initial values
                    long timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        writer.accept(CdcTester.newUniqueRow(tester.schema, rows), timestamp++);
                    }

                    // overwrite with new mutations at later timestamp
                    for (TestSchema.TestRow row : rows.values())
                    {
                        TestSchema.TestRow newUniqueRow = CdcTester.newUniqueRow(tester.schema, rows);
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
                    assertEquals(testRows.size() * 2, events.size(), "Each PK should get 2 mutations");
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
                            assertTrue(lastTs < ts, "Writetime should be monotonic increasing");
                        }
                        if (i >= partitions) // the rows in the second batch has ttl specified.
                        {
                            assertNotNull(event.getTtl());
                            assertEquals(TTL, event.getTtl().ttlInSec);
                        }
                        i++;
                    }
                })
                .run();
            });
    }
}
