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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.msg.CdcEnvelope;
import org.apache.cassandra.cdc.kafka.KafkaCdcSerializer;
import org.apache.cassandra.cdc.kafka.RecordProducer;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.LocalTableSchemaStore;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Serializes Cassandra CDC POJO classes to Avro bytes for publishing to Kafka,
 * and deserializes the bytes to Avro records. The data types in Avro records are suitable for Spark.
 * <p>
 * Optional logical type conversions can be applied via {@link org.apache.cassandra.cdc.avro.RecordReader} to convert
 * data to CQL-appropriate types.
 */
public class AvroSerializer implements KafkaCdcSerializer<CdcEvent>
{
    private final GenericDatumWriter<GenericRecord> cdcWriter;
    private final BinaryEncoder encoderReuse;
    private final AvroByteRecordTransformer recordTransformer;
    private final Deserializer deserializer;

    public AvroSerializer(Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        this(LocalTableSchemaStore.getInstance(), typeLookup);
    }

    public AvroSerializer(SchemaStore schemaStore, Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        this(schemaStore, typeLookup, AvroByteRecordTransformer.DEFAULT_TRUNCATE_THRESHOLD);
    }

    public AvroSerializer(SchemaStore schemaStore, Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup, int truncateThreshold)
    {
        this.recordTransformer = new AvroByteRecordTransformer(schemaStore, typeLookup, truncateThreshold);
        this.cdcWriter = new GenericDatumWriter<>(recordTransformer.cdcSchema);
        this.encoderReuse = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(0), null);
        this.deserializer = new Deserializer(recordTransformer.cdcSchema, schemaStore);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
    }

    @Override
    public byte[] serialize(String topic, CdcEvent event)
    {
        final GenericData.Record record = recordTransformer.transform(event);
        return encode(cdcWriter, record);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, CdcEvent data)
    {
        return serialize(topic, data);
    }

    @Override
    public void close()
    {
    }

    private byte[] encode(GenericDatumWriter<GenericRecord> writer, GenericData.Record update)
    {
        return AvroDataUtils.encode(writer, update, encoderReuse);
    }

    // deserialize

    public Deserializer deserializer()
    {
        return deserializer;
    }

    @Override
    public AvroByteRecordTransformer getTransformer()
    {
        return recordTransformer;
    }

    public static class Deserializer
    {
        private final SchemaStore store;
        private final GenericDatumReader<GenericRecord> cdcReader;
        private final BinaryDecoder decoderReuse;

        public Deserializer(Schema cdcSchema, SchemaStore store)
        {
            this.store = store;
            this.cdcReader = new GenericDatumReader<>(cdcSchema);
            this.decoderReuse = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(
            ByteBufferUtils.EMPTY), null);
        }

        /**
         * Deserialize a CDC Avro message, extracting {@code schemaUuid} from the Kafka message
         * headers to locate the correct payload schema version in the schema store.
         */
        public CdcEnvelope deserialize(String keyspace, String table, byte[] data, Headers headers)
        {
            Header h = headers.lastHeader(RecordProducer.SCHEMA_UUID_HEADER);
            String schemaUuid = h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
            return deserialize(keyspace, table, data, schemaUuid);
        }

        /**
         * Deserialize a CDC Avro message, using {@code schemaUuid} (e.g. from the Kafka header)
         * to locate the payload schema.
         */
        public CdcEnvelope deserialize(String keyspace, String table, byte[] data, String schemaUuid)
        {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, decoderReuse);
            try
            {
                GenericRecord header = cdcReader.read(null, decoder);
                GenericRecord payload = deserializePayload(keyspace, table, schemaUuid, getPayload(header));
                return new CdcEnvelope(header, payload);
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format("Unable to deserialize CDC update from %s/%s", keyspace, table), e);
            }
        }

        public static byte[] getPayload(GenericRecord header)
        {
            ByteBuffer buf = (ByteBuffer) header.get(AvroConstants.PAYLOAD_KEY);
            byte[] ar = new byte[buf.remaining()];
            buf.get(ar);
            return ar;
        }

        public GenericRecord deserializePayload(String keyspace, String table, String schemaUuid, byte[] data) throws IOException
        {
            GenericDatumReader<GenericRecord> payloadReader = store.getReader(keyspace + "." + table, schemaUuid);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, this.decoderReuse);
            return payloadReader.read(null, decoder);
        }
    }
}
