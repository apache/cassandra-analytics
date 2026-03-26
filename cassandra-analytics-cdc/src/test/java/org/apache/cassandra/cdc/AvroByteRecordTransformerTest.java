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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroByteRecordTransformer;
import org.apache.cassandra.cdc.avro.AvroConstants;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.avro.TestSchemaStore;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.test.CdcTestBase;
import org.apache.cassandra.cdc.test.CdcTester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.apache.cassandra.cdc.test.CdcTester.testWith;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that exercise the CDC-to-Avro byte-serialization pipeline ({@code cdc_bytes.avsc}),
 */
public class AvroByteRecordTransformerTest extends CdcTestBase
{
    private static final int NUM_ROWS = 50;

    private CqlToAvroSchemaConverter getConverter(CassandraVersion version)
    {
        CqlToAvroSchemaConverter converter = CdcBridgeFactory.getCqlToAvroSchemaConverter(version);
        assertThat(converter).isNotNull();
        return converter;
    }

    /**
     * Build the Avro byte transformer by converting the CQL table schema
     * into an Avro schema and registering it in the test schema store.
     */
    private AvroByteRecordTransformer buildTransformer(CqlToAvroSchemaConverter converter,
                                                        CqlTable cqlTable,
                                                        TestSchemaStore schemaStore)
    {
        Schema avroSchema = converter.convert(cqlTable);
        String namespace = cqlTable.keyspace() + "." + cqlTable.table();
        schemaStore.registerSchema(namespace, avroSchema);

        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup = key -> bridge.parseType(key.type);
        return new AvroByteRecordTransformer(schemaStore, typeLookup);
    }

    /**
     * Deserialize a byte payload using the table's Avro schema from the schema store.
     */
    private GenericRecord deserializePayload(ByteBuffer payloadBytes, GenericDatumReader<GenericRecord> reader) throws IOException
    {
        byte[] bytes = new byte[payloadBytes.remaining()];
        payloadBytes.get(bytes);
        return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.cdc.test.TestVersionSupplier#testVersions")
    public void testTtlDeletedAtByteAvroEncoding(CassandraVersion version)
    {
        AvroSchemas.registerLogicalTypes();
        CqlToAvroSchemaConverter converter = getConverter(version);

        int ttlSeconds = 3600;
        long beforeTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        TestSchema.Builder schemaBuilder = TestSchema.builder(bridge)
                                                     .withPartitionKey("pk", bridge.uuid())
                                                     .withColumn("c1", bridge.bigint())
                                                     .withColumn("c2", bridge.text());

        AtomicReference<CqlTable> tableRef = new AtomicReference<>();
        testWith(bridge, cdcBridge, commitLogDir, schemaBuilder)
        .withNumRows(NUM_ROWS)
        .clearWriters()
        .withWriter((tester, rows, writer) -> {
            tableRef.set(tester.cqlTable);
            for (int i = 0; i < tester.numRows; i++)
            {
                TestSchema.TestRow testRow = CdcTester.newUniqueRow(tester.schema, rows);
                testRow.setTTL(ttlSeconds);
                writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            }
        })
        .withCdcEventChecker((testRows, events) -> {
            long afterTestEpochSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            assertThat(events).hasSize(NUM_ROWS);

            TestSchemaStore schemaStore = new TestSchemaStore();
            AvroByteRecordTransformer transformer = buildTransformer(converter, tableRef.get(), schemaStore);
            String namespace = tableRef.get().keyspace() + "." + tableRef.get().table();

            for (CdcEvent event : events)
            {
                GenericData.Record record = transformer.transform(event);
                assertThat(record).isNotNull();

                // Validate header-level fields
                assertThat(record.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo(event.table);
                assertThat(record.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo(event.keyspace);
                assertThat(record.get(AvroConstants.OPERATION_TYPE_KEY).toString()).isEqualTo("INSERT");
                assertThat(record.get(AvroConstants.TIMESTAMP_KEY)).isNotNull();
                assertThat(record.get(AvroConstants.VERSION_KEY).toString()).isEqualTo(AvroConstants.CURRENT_VERSION);

                // TTL record should be present
                Object ttlField = record.get(AvroConstants.TTL_KEY);
                assertThat(ttlField).isNotNull();
                GenericRecord ttlRecord = (GenericRecord) ttlField;

                // Validate TTL value
                assertThat(ttlRecord.get(AvroConstants.TTL_KEY)).isEqualTo(ttlSeconds);

                // Validate deletedAt is a Long (confirms long type in cdc_bytes.avsc)
                Object deletedAt = ttlRecord.get(AvroConstants.DELETED_AT_KEY);
                assertThat(deletedAt).isInstanceOf(Long.class);

                // Validate deletedAt is approximately nowInSeconds + TTL
                long deletedAtValue = (Long) deletedAt;
                assertThat(deletedAtValue)
                    .as("deletedAt should be approximately nowInSeconds + TTL")
                    .isBetween(beforeTestEpochSec + ttlSeconds, afterTestEpochSec + ttlSeconds);

                // Validate payload: bytes in the byte schema need deserialization to verify content
                Object payloadObj = record.get(AvroConstants.PAYLOAD_KEY);
                assertThat(payloadObj).isInstanceOf(ByteBuffer.class);
                GenericRecord payloadRecord;
                try
                {
                    payloadRecord = deserializePayload((ByteBuffer) payloadObj,
                                                        schemaStore.getReader(namespace, null));
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Failed to deserialize payload", e);
                }
                assertThat(payloadRecord.get("pk")).isNotNull();
            }
        })
        .run();
    }
}
