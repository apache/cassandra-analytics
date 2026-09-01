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

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroConstants;
import org.apache.cassandra.cdc.avro.AvroGenericRecordTransformer;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CdcEventUtils;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.avro.TestSchemaStore;
import org.apache.cassandra.cdc.kafka.AvroGenericRecordSerializer;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.newUniquePartitionDeletion;
import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that exercise the full CDC-to-Avro pipeline:
 * write mutations -> read CDC events from commit logs -> convert to Avro GenericRecord -> validate.
 */
@SuppressWarnings("DataFlowIssue")
public class AvroGenericRecordTransformerTest extends CdcTestBase
{
    private static final int NUM_ROWS = 50;

    private CqlToAvroSchemaConverter getConverter(CassandraVersion version)
    {
        CqlToAvroSchemaConverter converter = CdcBridgeFactory.getCqlToAvroSchemaConverter(version);
        assertThat(converter).isNotNull();
        return converter;
    }

    /**
     * Build the Avro transformer by converting the CQL table schema (already registered by CdcTester)
     * into an Avro schema and registering it in the test schema store.
     */
    private AvroGenericRecordTransformer buildTransformer(CqlToAvroSchemaConverter converter,
                                                          CqlTable cqlTable)
    {
        TestSchemaStore schemaStore = new TestSchemaStore();
        Schema avroSchema = converter.convert(cqlTable);
        String namespace = cqlTable.keyspace() + "." + cqlTable.table();
        schemaStore.registerSchema(namespace, avroSchema);

        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup = key -> bridge.parseType(key.type);
        return new AvroGenericRecordTransformer(schemaStore, typeLookup, "");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testBasicInsertAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withClusteringKey("ck", bridge.bigint())
                                                     .withColumn("c1", bridge.bigint())
                                                     .withColumn("c2", bridge.text());

        // Capture CqlTable from the tester via the writer callback
        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            IntStream.range(0, tester.numRows)
                     .forEach(i -> writer.accept(CdcTester.newUniqueRow(tester.schema, rows, random), timestampMicros));
        })
        .withCdcEventChecker((testRows, events) -> {
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            for (CdcEvent event : events)
            {
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                // Validate header fields
                assertThat(record.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo(event.table);
                assertThat(record.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo(event.keyspace);
                assertThat(record.get(AvroConstants.OPERATION_TYPE_KEY).toString()).isEqualTo("INSERT");
                assertThat(record.get(AvroConstants.TIMESTAMP_KEY)).isNotNull();
                assertThat(record.get(AvroConstants.VERSION_KEY).toString()).isEqualTo(AvroConstants.CURRENT_VERSION);

                // Validate payload contains expected fields including clustering key
                GenericRecord payload = (GenericRecord) record.get(AvroConstants.PAYLOAD_KEY);
                assertThat(payload).isNotNull();
                assertThat(payload.getSchema().getField("pk")).isNotNull();
                assertThat(payload.getSchema().getField("ck")).isNotNull();
                assertThat(payload.getSchema().getField("c1")).isNotNull();
                assertThat(payload.getSchema().getField("c2")).isNotNull();
                // Payload fields should have data
                assertThat(payload.get("pk")).isNotNull();
                assertThat(payload.get("ck")).isNotNull();
                assertThat(payload.get("c1")).isNotNull();

                // Validate updateFields lists all updated column names
                List<String> updateFields = CdcEventUtils.updatedFieldNames(event);
                assertThat(updateFields).contains("pk", "ck", "c1", "c2");
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testCollectionTypesAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("m", bridge.map(bridge.text(), bridge.text()))
                                                     .withColumn("s", bridge.set(bridge.aInt()))
                                                     .withColumn("l", bridge.list(bridge.text()));

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            IntStream.range(0, tester.numRows)
                     .forEach(i -> writer.accept(CdcTester.newUniqueRow(tester.schema, rows, random), timestampMicros));
        })
        .withCdcEventChecker((testRows, events) -> {
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            for (CdcEvent event : events)
            {
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                GenericRecord payload = (GenericRecord) record.get(AvroConstants.PAYLOAD_KEY);
                assertThat(payload).isNotNull();
                assertThat(payload.getSchema().getField("m")).isNotNull();
                assertThat(payload.getSchema().getField("s")).isNotNull();
                assertThat(payload.getSchema().getField("l")).isNotNull();
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testAvroSerializeDeserializeRoundTrip(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.bigint())
                                                     .withColumn("c2", bridge.text());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            IntStream.range(0, tester.numRows)
                     .forEach(i -> writer.accept(CdcTester.newUniqueRow(tester.schema, rows, random), timestampMicros));
        })
        .withCdcEventChecker((testRows, events) -> {
            assertThat(events).hasSize(NUM_ROWS);
            TestSchemaStore schemaStore = new TestSchemaStore();
            CqlTable cqlTable = tableRef.get();
            Schema avroSchema = converter.convert(cqlTable);
            String namespace = cqlTable.keyspace() + "." + cqlTable.table();
            schemaStore.registerSchema(namespace, avroSchema);

            Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup = key -> bridge.parseType(key.type);
            AvroGenericRecordSerializer serializer = new AvroGenericRecordSerializer(schemaStore, typeLookup, "");

            for (CdcEvent event : events)
            {
                // Serialize: CdcEvent -> bytes
                byte[] bytes = serializer.serialize("test-topic", event);
                assertThat(bytes).isNotNull();
                assertThat(bytes.length).isGreaterThan(0);

                // Get the Avro record's schema for deserialization
                GenericData.Record record = serializer.getTransformer().transform(event);
                Schema recordSchema = record.getSchema();

                // Deserialize: bytes -> CdcEnvelope
                org.apache.cassandra.cdc.avro.msg.CdcEnvelope envelope =
                    serializer.deserializer().deserialize(event.keyspace, event.table, bytes, recordSchema);
                assertThat(envelope).isNotNull();
                assertThat(envelope.header).isNotNull();
                assertThat(envelope.payload).isNotNull();

                // Validate round-trip preserves operation type and table info
                assertThat(envelope.header.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo(event.table);
                assertThat(envelope.header.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo(event.keyspace);
                assertThat(envelope.header.get(AvroConstants.OPERATION_TYPE_KEY).toString()).isEqualTo("INSERT");

                // Validate payload fields survived round-trip
                assertThat(envelope.payload.get("pk")).isNotNull();
                assertThat(envelope.payload.get("c1")).isNotNull();
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testDeleteEventAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.bigint())
                                                     .withColumn("c2", bridge.text());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                testRow = testRow.copy("c1", org.apache.cassandra.bridge.CdcBridge.UNSET_MARKER);
                testRow = testRow.copy("c2", null);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            for (CdcEvent event : events)
            {
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                // Verify the Avro record correctly encodes the operation type from the event
                CdcEventUtils.OperationType opType = CdcEventUtils.getOperationType(event);
                assertThat(record.get(AvroConstants.OPERATION_TYPE_KEY).toString()).isEqualTo(opType.name());
                assertThat(record.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo(event.table);
                assertThat(record.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo(event.keyspace);

                GenericRecord payload = (GenericRecord) record.get(AvroConstants.PAYLOAD_KEY);
                assertThat(payload).isNotNull();
                // pk should still be present
                assertThat(payload.get("pk")).isNotNull();
                // c2 is deleted (null in the payload)
                assertThat(payload.get("c2")).isNull();
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMaxSupportedTtlAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        // Use a large TTL value (~20 years). Safe for both 4.0 and 5.0.
        int largeTtl = 20 * 365 * 24 * 3600;
        long beforeTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.aInt());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                testRow.setTTL(largeTtl);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            for (CdcEvent event : events)
            {
                // Validate CdcEvent-level TTL
                assertThat(event.getTtl()).isNotNull();
                assertThat(event.getTtl().ttlInSec).isEqualTo(largeTtl);
                long expirationTime = event.getTtl().expirationTimeInSec;
                long expectedLower = beforeTestEpochSec + largeTtl;
                long expectedUpper = afterTestEpochSec + largeTtl;
                if (expirationTime <= Integer.MAX_VALUE && expectedLower > Integer.MAX_VALUE)
                {
                    // Cassandra 4.0 caps the value at Integer.MAX_VALUE
                    assertThat(expirationTime)
                        .as("expirationTimeInSec should be capped near Integer.MAX_VALUE on Cassandra 4.0")
                        .isBetween((long) Integer.MAX_VALUE - 1, (long) Integer.MAX_VALUE);
                }
                else
                {
                    assertThat(expirationTime)
                        .as("expirationTimeInSec should be approximately nowInSeconds + largeTtl")
                        .isBetween(expectedLower, expectedUpper);
                }

                // Validate Avro-level TTL encoding
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                Object ttlField = record.get(AvroConstants.TTL_KEY);
                assertThat(ttlField).isNotNull();
                GenericRecord ttlRecord = (GenericRecord) ttlField;
                assertThat(ttlRecord.get(AvroConstants.TTL_KEY)).isEqualTo(largeTtl);

                Object deletedAt = ttlRecord.get(AvroConstants.DELETED_AT_KEY);
                assertThat(deletedAt).isInstanceOf(Long.class);
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testPartitionDeleteAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        long beforeTestMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.aInt());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = newUniquePartitionDeletion(tester.schema, rows, random);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            for (CdcEvent event : events)
            {
                // Validate CdcEvent-level partition delete
                assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.PARTITION_DELETE);
                long eventTimestampMicros = event.getTimestamp(TimeUnit.MICROSECONDS);
                assertThat(eventTimestampMicros)
                    .as("deletion timestamp should be within the test time window")
                    .isBetween(beforeTestMicros, afterTestMicros);

                // Validate Avro-level encoding
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();
                assertThat(record.get(AvroConstants.OPERATION_TYPE_KEY).toString()).isEqualTo("DELETE_PARTITION");
                assertThat(record.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo(event.table);
                assertThat(record.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo(event.keyspace);
            }
        })
        .run();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testMixedTtlAndNonTtlAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        int ttlSeconds = 3600;
        long beforeTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.aInt());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        Random random = new Random();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .withRandom(random)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows, random);
                if (i % 2 == 0)
                {
                    testRow.setTTL(ttlSeconds);
                }
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            assertThat(events).hasSize(NUM_ROWS);
            AvroGenericRecordTransformer transformer = buildTransformer(converter, tableRef.get());

            int withTtl = 0;
            int withoutTtl = 0;
            for (CdcEvent event : events)
            {
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                if (event.getTtl() != null)
                {
                    withTtl++;
                    assertThat(event.getTtl().ttlInSec).isEqualTo(ttlSeconds);
                    assertThat(event.getTtl().expirationTimeInSec).isGreaterThan(0L);

                    // Avro TTL field should be present
                    Object ttlField = record.get(AvroConstants.TTL_KEY);
                    assertThat(ttlField).as("Avro TTL record should be present for TTL row").isNotNull();
                    GenericRecord ttlRecord = (GenericRecord) ttlField;
                    assertThat(ttlRecord.get(AvroConstants.TTL_KEY)).isEqualTo(ttlSeconds);

                    // Validate deletedAt is a Long with expected value
                    Object deletedAt = ttlRecord.get(AvroConstants.DELETED_AT_KEY);
                    assertThat(deletedAt).isInstanceOf(Long.class);
                    long deletedAtValue = (Long) deletedAt;
                    assertThat(deletedAtValue)
                        .as("deletedAt should be approximately nowInSeconds + TTL")
                        .isBetween(beforeTestEpochSec + ttlSeconds, afterTestEpochSec + ttlSeconds);
                }
                else
                {
                    withoutTtl++;
                    // Avro TTL field should be absent
                    assertThat(record.get(AvroConstants.TTL_KEY))
                        .as("Avro TTL record should be null for non-TTL row")
                        .isNull();
                }
            }
            assertThat(withTtl).isEqualTo(NUM_ROWS / 2);
            assertThat(withoutTtl).isEqualTo(NUM_ROWS / 2);
        })
        .run();
    }
}
