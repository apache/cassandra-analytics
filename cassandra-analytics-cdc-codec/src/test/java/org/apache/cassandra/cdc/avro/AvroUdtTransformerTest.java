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

package org.apache.cassandra.cdc.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.CdcEventBuilder;
import org.apache.cassandra.cdc.msg.FourZeroRangeTombstoneBuilder;
import org.apache.cassandra.cdc.msg.RangeTombstone;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.apache.cassandra.spark.utils.ArrayUtils.listOf;
import static org.apache.cassandra.spark.utils.ArrayUtils.setOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that CDC events with UDT columns are transformed into Avro records, i.e. UDT values are
 * published as nested Avro records instead of raw bytes.
 */
public class AvroUdtTransformerTest
{
    private static final String KEYSPACE = "avro_udt_transformer_ks";
    private static final ReplicationFactor REPLICATION_FACTOR =
    new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, Collections.singletonMap("replication_factor", 1));

    private final CassandraBridgeImplementation bridge = new CassandraBridgeImplementation();

    @Test
    public void testListOfFrozenUdts() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".task (id uuid PRIMARY KEY, name text, " +
                                       "results list<frozen<task_result>>);",
                                       "CREATE TYPE " + KEYSPACE + ".task_result (status text, message text);");
        UUID id = UUID.randomUUID();
        CdcEvent event = insert(table, id,
                                Value.of(KEYSPACE, "name", "text", text("test-task")),
                                Value.of(KEYSPACE, "results", "list<frozen<task_result>>",
                                         list(fields(text("SUCCESS"), text("completed")))));

        GenericRecord payload = transformAndEncode(table, event);

        assertThat(payload.get("id")).hasToString(id.toString());
        assertThat(payload.get("name")).hasToString("test-task");
        List<?> results = (List<?>) payload.get("results");
        assertThat(results).hasSize(1);
        GenericRecord result = (GenericRecord) results.get(0);
        assertThat(result.get("status")).hasToString("SUCCESS");
        assertThat(result.get("message")).hasToString("completed");
    }

    @Test
    public void testListOfFrozenUdtsWithNestedUdt() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".command (id uuid PRIMARY KEY, command_class text, " +
                                       "execution_results list<frozen<execution_result>>);",
                                       "CREATE TYPE " + KEYSPACE + ".transient_event (id uuid, event_class text);",
                                       "CREATE TYPE " + KEYSPACE + ".execution_result " +
                                       "(domain_event frozen<transient_event>, status text);");
        UUID id = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        CdcEvent event = insert(table, id,
                                Value.of(KEYSPACE, "command_class", "text", text("TestCommand")),
                                Value.of(KEYSPACE, "execution_results", "list<frozen<execution_result>>",
                                         list(fields(fields(uuid(eventId), text("TestEvent")), text("SUCCESS")))));

        GenericRecord payload = transformAndEncode(table, event);

        GenericRecord executionResult = (GenericRecord) ((List<?>) payload.get("execution_results")).get(0);
        GenericRecord domainEvent = (GenericRecord) executionResult.get("domain_event");
        assertThat(domainEvent.get("id")).hasToString(eventId.toString());
        assertThat(domainEvent.get("event_class")).hasToString("TestEvent");
        assertThat(executionResult.get("status")).hasToString("SUCCESS");
    }

    @Test
    public void testFrozenUdtColumn() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".single (id uuid PRIMARY KEY, result frozen<single_result>);",
                                       "CREATE TYPE " + KEYSPACE + ".single_result (status text, code int);");
        CdcEvent event = insert(table, UUID.randomUUID(),
                                Value.of(KEYSPACE, "result", "frozen<single_result>",
                                         fields(text("SUCCESS"), ByteBuffer.allocate(4).putInt(0, 200))));

        GenericRecord result = (GenericRecord) transformAndEncode(table, event).get("result");

        assertThat(result.get("status")).hasToString("SUCCESS");
        assertThat(result.get("code")).isEqualTo(200);
    }

    @Test
    public void testNonFrozenUdtColumn() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".multi_cell (id uuid PRIMARY KEY, result multi_cell_result);",
                                       "CREATE TYPE " + KEYSPACE + ".multi_cell_result (status text, code int);");
        // multi-cell UDT cells are packed with the cell count first
        CdcEvent event = insert(table, UUID.randomUUID(),
                                Value.of(KEYSPACE, "result", "multi_cell_result",
                                         list(text("SUCCESS"), ByteBuffer.allocate(4).putInt(0, 200))));

        GenericRecord result = (GenericRecord) transformAndEncode(table, event).get("result");

        assertThat(result.get("status")).hasToString("SUCCESS");
        assertThat(result.get("code")).isEqualTo(200);
    }

    @Test
    public void testQuotedColumnAndUdtFieldNames() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".\"quoted-table\" (id uuid PRIMARY KEY, " +
                                       "\"my-col\" frozen<quoted_result>, \"big-col\" text);",
                                       "CREATE TYPE " + KEYSPACE + ".quoted_result (\"status-code\" int, message text);");
        CdcEvent event = insert(table, UUID.randomUUID(),
                                Value.of(KEYSPACE, "my-col", "frozen<quoted_result>",
                                         fields(ByteBuffer.allocate(4).putInt(0, 200), text("ok"))),
                                Value.of(KEYSPACE, "big-col", "text", text(String.join("", Collections.nCopies(100, "x")))));

        // the id and my-col fit in the threshold, big-col does not
        GenericRecord record = transformAndEncode(table, event, 64, "prefix");
        GenericRecord payload = (GenericRecord) record.get(AvroConstants.PAYLOAD_KEY);

        // the fields are written under their Avro names
        GenericRecord result = (GenericRecord) payload.get("my_u002dcol");
        assertThat(result.get("status_u002dcode")).isEqualTo(200);
        assertThat(result.get("message")).hasToString("ok");
        // and the envelope names them the same way
        assertThat(stringsOf(record.get(AvroConstants.UPDATE_FIELDS_KEY))).containsExactly("id", "my_u002dcol", "big_u002dcol");
        assertThat(stringsOf(record.get(AvroConstants.TRUNCATED_FIELDS_KEY))).containsExactly("big_u002dcol");
        // the envelope is named like the table record
        assertThat(record.getSchema().getFullName()).isEqualTo("prefix." + KEYSPACE + ".quoted_u002dtable");
        assertThat(payload.getSchema().getName()).isEqualTo("quoted_u002dtable");
    }

    @Test
    public void testUdtWithQuotedFieldNamesRoundTrips() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".round_trip (id uuid PRIMARY KEY, c frozen<round_trip_udt>);",
                                       "CREATE TYPE " + KEYSPACE + ".round_trip_udt (\"zip-code\" int, city text);");
        CdcEvent event = insert(table, UUID.randomUUID(),
                                Value.of(KEYSPACE, "c", "frozen<round_trip_udt>",
                                         fields(ByteBuffer.allocate(4).putInt(0, 94000), text("sf"))));
        GenericRecord payload = (GenericRecord) transformAndEncode(table, event, AvroBaseRecordTransformer.DEFAULT_TRUNCATE_THRESHOLD, null)
                                                .get(AvroConstants.PAYLOAD_KEY);

        // reading a UDT gives the CQL field names, which is what writing it expects
        Map<Object, Object> read = (Map<Object, Object>) RecordReader.get().read(payload, "c");
        assertThat(read).containsOnlyKeys("zip-code", "city");
        Schema.Field column = payload.getSchema().getField("c");
        GenericRecord written = (GenericRecord) AvroDataUtils.toAvro(read, column.schema());
        assertThat(written).isEqualTo(payload.get("c"));
    }

    @Test
    public void testRangePredicateOnQuotedClusteringKey() throws IOException
    {
        CqlTable table = registerTable("CREATE TABLE " + KEYSPACE + ".quoted_range (id uuid, \"my-ck\" int, v text, " +
                                       "PRIMARY KEY (id, \"my-ck\"));");
        CdcEventBuilder builder = CdcEventBuilder.of(CdcEvent.Kind.RANGE_DELETE, KEYSPACE, table.table());
        builder.setPartitionKeys(listOf(Value.of(KEYSPACE, "id", "uuid", uuid(UUID.randomUUID()))));
        // the tombstones are complete, so the builder that assembles them from markers has nothing left
        builder.setRangeTombstoneBuilder(new FourZeroRangeTombstoneBuilder(CassandraSchema.getTable(KEYSPACE, table.table()).get()));
        builder.setRangeTombstoneList(listOf(new RangeTombstone(listOf(Value.of(KEYSPACE, "my-ck", "int", ByteBuffer.allocate(4).putInt(0, 1))), true,
                                                                listOf(Value.of(KEYSPACE, "my-ck", "int", ByteBuffer.allocate(4).putInt(0, 5))), false)));
        builder.setMaxTimestampMicros(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        GenericRecord record = transformAndEncode(table, builder.build(), AvroBaseRecordTransformer.DEFAULT_TRUNCATE_THRESHOLD, null);

        Schema tableSchema = new CqlToAvroSchemaConverterImplementation(bridge).convert(table);
        List<String> predicates = new ArrayList<>();
        for (Object predicate : (List<?>) record.get(AvroConstants.RANGE_KEY))
        {
            GenericRecord range = (GenericRecord) predicate;
            // the predicate names the field it encodes
            assertThat(range.get(AvroConstants.FIELD_KEY)).hasToString("my_u002dck");
            ByteBuffer value = (ByteBuffer) range.get(AvroConstants.VALUE_KEY);
            GenericRecord decoded = new GenericDatumReader<GenericRecord>(tableSchema)
                                    .read(null, DecoderFactory.get().binaryDecoder(value.array(), value.position(), value.remaining(), null));
            predicates.add(range.get(AvroConstants.RANGE_PREDICATE_KEY) + " " + decoded.get("my_u002dck"));
        }
        assertThat(predicates).containsExactly("GTE 1", "LT 5");
    }

    private static List<String> stringsOf(Object list)
    {
        return ((List<?>) list).stream().map(Object::toString).collect(Collectors.toList());
    }

    /**
     * Parses the table and registers its UDTs with the types used for type lookup, as the CDC bridge does
     */
    private CqlTable registerTable(String createTable, String... createTypes)
    {
        CqlTable table = bridge.buildSchema(createTable, KEYSPACE, REPLICATION_FACTOR, Partitioner.Murmur3Partitioner, setOf(createTypes));
        table.udts().forEach(udt -> bridge.cassandraTypes().updateUDTs(KEYSPACE, udt));
        return table;
    }

    private static CdcEvent insert(CqlTable table, UUID id, Value... valueColumns)
    {
        CdcEventBuilder builder = CdcEventBuilder.of(CdcEvent.Kind.INSERT, KEYSPACE, table.table());
        builder.setPartitionKeys(listOf(Value.of(KEYSPACE, "id", "uuid", uuid(id))));
        builder.setValueColumns(listOf(valueColumns));
        builder.setMaxTimestampMicros(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        return builder.build();
    }

    /**
     * Transforms the event, encodes and decodes the full CDC record, and returns the decoded payload
     */
    private GenericRecord transformAndEncode(CqlTable table, CdcEvent event) throws IOException
    {
        return (GenericRecord) transformAndEncode(table, event, AvroBaseRecordTransformer.DEFAULT_TRUNCATE_THRESHOLD, null)
                               .get(AvroConstants.PAYLOAD_KEY);
    }

    /**
     * Transforms the event, encodes and decodes the full CDC record, and returns the decoded record
     */
    private GenericRecord transformAndEncode(CqlTable table, CdcEvent event, int truncateThreshold, String schemaNamespacePrefix)
    throws IOException
    {
        Schema tableSchema = new CqlToAvroSchemaConverterImplementation(bridge).convert(table);
        AvroGenericRecordTransformer transformer =
        new AvroGenericRecordTransformer(new SingleTableSchemaStore(tableSchema),
                                         key -> bridge.cassandraTypes().parseType(key.keyspace, key.type),
                                         truncateThreshold,
                                         schemaNamespacePrefix);

        GenericData.Record record = transformer.transform(event);

        // encoding fails with a ClassCastException if UDT values are not Avro records
        byte[] encoded = AvroDataUtils.encode(new GenericDatumWriter<>(record.getSchema()), record, null);
        GenericRecord decoded = new GenericDatumReader<GenericRecord>(record.getSchema())
                                .read(null, DecoderFactory.get().binaryDecoder(encoded, null));
        return decoded;
    }

    private static ByteBuffer text(String value)
    {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer uuid(UUID value)
    {
        return ByteBuffer.allocate(16)
                         .putLong(0, value.getMostSignificantBits())
                         .putLong(8, value.getLeastSignificantBits());
    }

    // frozen UDT encoding: [int length][bytes] per field
    private static ByteBuffer fields(ByteBuffer... values)
    {
        return concat(false, values);
    }

    // collection encoding: [int count] followed by [int length][bytes] per element
    private static ByteBuffer list(ByteBuffer... elements)
    {
        return concat(true, elements);
    }

    private static ByteBuffer concat(boolean withCount, ByteBuffer... values)
    {
        int size = withCount ? Integer.BYTES : 0;
        for (ByteBuffer value : values)
        {
            size += Integer.BYTES + value.remaining();
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        if (withCount)
        {
            result.putInt(values.length);
        }
        for (ByteBuffer value : values)
        {
            result.putInt(value.remaining());
            result.put(value.duplicate());
        }
        result.flip();
        return result;
    }

    /**
     * {@link SchemaStore} serving the Avro schema of a single table
     */
    private static class SingleTableSchemaStore implements SchemaStore
    {
        private final Schema schema;

        SingleTableSchemaStore(Schema schema)
        {
            this.schema = schema;
        }

        @Override
        public Schema getSchema(String namespace, String name)
        {
            return schema;
        }

        @Override
        public GenericDatumWriter<GenericRecord> getWriter(String namespace, String name)
        {
            return new GenericDatumWriter<>(schema);
        }

        @Override
        public GenericDatumReader<GenericRecord> getReader(String namespace, String name)
        {
            return new GenericDatumReader<>(schema);
        }
    }
}
