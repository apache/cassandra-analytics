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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.cdc.CdcEventTransformer;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;

import static org.apache.cassandra.cdc.avro.AvroFields.CURRENT_VERSION;
import static org.apache.cassandra.cdc.avro.AvroFields.IS_PARTIAL_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.OPERATION_TYPE_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.RANGE_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.SOURCE_KEYSPACE_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.SOURCE_TABLE_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.TIMESTAMP_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.TTL_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.UPDATE_FIELDS_KEY;
import static org.apache.cassandra.cdc.avro.AvroFields.VERSION_KEY;

/**
 * Base abstraction to convert CdcEvent objects into another data format, e.g. Avro, Json etc
 */
public abstract class CdcEventAvroEncoder implements CdcEventTransformer<GenericData.Record>
{
    public final Schema cdcSchema;
    public final Schema ttlSchema;
    public final Schema rangeSchema;

    protected final BinaryEncoder encoder;
    protected final Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup;
    protected final SchemaStore store;

    public CdcEventAvroEncoder(SchemaStore store,
                               Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                               String templatePath)
    {
        this.cdcSchema = readSchema(templatePath);
        this.ttlSchema = extractTtlSchema(cdcSchema);
        this.rangeSchema = extractRangeSchema(cdcSchema);
        this.encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(0), null);
        this.typeLookup = typeLookup;
        this.store = store;
    }

    /**
     * Transform CdcEvent into Avro record.
     *
     * @param event the CdcEvent
     * @return transformed cdc event as an Avro GenericData.Record
     */
    public abstract GenericData.Record transform(CdcEvent event);

    protected void applyCommonFields(CdcEvent event, GenericData.Record record, Function<Value, Object> avroFieldEncoder)
    {
        long timestamp = event.getTimestamp(TimeUnit.MICROSECONDS);
        record.put(TIMESTAMP_KEY, timestamp);
        record.put(VERSION_KEY, CURRENT_VERSION);
        record.put(IS_PARTIAL_KEY, true);
        record.put(SOURCE_TABLE_KEY, event.table);
        record.put(SOURCE_KEYSPACE_KEY, event.keyspace);
        record.put(OPERATION_TYPE_KEY, CdcEventUtils.getAvroOperationType(event, cdcSchema));
        record.put(UPDATE_FIELDS_KEY, CdcEventUtils.updatedFieldNames(event));
        record.put(RANGE_KEY, CdcEventUtils.getRangeTombstoneAvro(event, rangeSchema, avroFieldEncoder));
        record.put(TTL_KEY, CdcEventUtils.getTTLAvro(event, ttlSchema));
    }

    private static Schema readSchema(String filename)
    {
        ClassLoader classLoader = CdcEventAvroEncoder.class.getClassLoader();
        final InputStream is = classLoader.getResourceAsStream(filename);
        try
        {
            return new Schema.Parser().parse(is);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Schema extractTtlSchema(Schema cdcSchema)
    {
        List<Schema> nullableTtlUnion = cdcSchema.getField("ttl").schema().getTypes();
        return nullableTtlUnion.stream()
                               .filter(s -> s.getType() == Schema.Type.RECORD)
                               .findFirst()
                               .orElseThrow(
                               () -> new IllegalStateException(
                               "ttl field should exist in cdc.avsc")); // the field exist. see cdc.avsc file
    }

    private static Schema extractRangeSchema(Schema cdcSchema)
    {
        List<Schema> nullableRangeUnion = cdcSchema.getField("range").schema().getTypes();
        return nullableRangeUnion.stream()
                                 .filter(s -> s.getType() == Schema.Type.ARRAY)
                                 .map(Schema::getElementType)
                                 .findFirst()
                                 .orElseThrow(() -> new IllegalStateException(
                                 "range field should exist in cdc.avsc")); // the field exist. see cdc.avsc file
    }

    /**
     * Encode the transformed CdcEvent to a byte array.
     *
     * @param writer the Avro record writer
     * @param record the CdcEvent transformed as a GenericData.Record
     * @return Avro message serialized to bytes
     */
    public byte[] encode(GenericDatumWriter<GenericRecord> writer, GenericData.Record record)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, encoder);
        try
        {
            writer.write(record, binaryEncoder);
            binaryEncoder.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }
}
