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

package org.apache.cassandra.cdc.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.cassandra.cdc.avro.AvroByteRecordTransformer;
import org.apache.cassandra.cdc.avro.AvroDataUtils;
import org.apache.cassandra.cdc.avro.AvroGenericRecordTransformer;
import org.apache.cassandra.cdc.avro.msg.CdcEnvelope;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.LocalTableSchemaStore;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.kafka.common.header.Headers;

/**
 * Serializes Cassandra CDC POJO classes to Avro bytes for publishing to Kafka.
 * Optional logical type conversions can be applied via {@link org.apache.cassandra.cdc.avro.RecordReader} to convert
 * data to CQL-appropriate types.
 */
public class AvroGenericRecordSerializer implements KafkaCdcSerializer<CdcEvent>
{
    private final BinaryEncoder encoderReuse;
    private final AvroGenericRecordTransformer recordTransformer;
    private final Deserializer deserializer;

    public AvroGenericRecordSerializer(Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup, String schemaNamespacePrefix)
    {
        this(LocalTableSchemaStore.getInstance(), typeLookup, schemaNamespacePrefix);
    }

    public AvroGenericRecordSerializer(SchemaStore schemaStore,
                                       Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                       String schemaNamespacePrefix)
    {
        this(schemaStore, typeLookup, AvroByteRecordTransformer.DEFAULT_TRUNCATE_THRESHOLD, schemaNamespacePrefix);
    }

    public AvroGenericRecordSerializer(SchemaStore schemaStore,
                                       Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                       int truncateThreshold,
                                       String schemaNamespacePrefix)
    {
        this.recordTransformer = new AvroGenericRecordTransformer(schemaStore, typeLookup, truncateThreshold, schemaNamespacePrefix);
        this.encoderReuse = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(65536), null);
        this.deserializer = new Deserializer(schemaStore);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
    }

    @Override
    public byte[] serialize(String topic, CdcEvent event)
    {
        final GenericData.Record record = recordTransformer.transform(event);
        return encode(new GenericDatumWriter<>(record.getSchema()), record);
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

    @Override
    public AvroGenericRecordTransformer getTransformer()
    {
        return recordTransformer;
    }

    public Deserializer deserializer()
    {
        return deserializer;
    }

    public static class Deserializer
    {
        private final SchemaStore store;
        private final BinaryDecoder decoderReuse;

        public Deserializer(SchemaStore store)
        {
            this.store = store;
            this.decoderReuse = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(ByteBufferUtils.EMPTY), null);
        }

        public CdcEnvelope deserialize(String keyspace, String table, byte[] data, Schema cdcSchema)
        {
            final GenericDatumReader<GenericRecord> cdcReader = new GenericDatumReader<>(cdcSchema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, decoderReuse);
            try
            {
                GenericRecord header = cdcReader.read(null, decoder);
                GenericRecord payload = (GenericRecord) header.get("payload");
                return new CdcEnvelope(header, payload);
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format("Unable to deserialize CDC update from %s/%s", keyspace, table), e);
            }
        }

        Object deserializeRangePredicateValue(String keyspace, String table, String fieldName, ByteBuffer value)
        {
            GenericDatumReader<GenericRecord> reader = store.getReader(keyspace + '.' + table, null);
            byte[] bytes = new byte[value.remaining()];
            try
            {
                value.get(bytes);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, this.decoderReuse);
                final GenericRecord valueRecord = reader.read(null, decoder);
                return valueRecord.get(fieldName);
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format("Unable to deserialize CDC update from %s/%s", keyspace, table), e);
            }
        }
    }

    /**
     * Deserialize the data and return a pair of cdc update and cdc record
     * The left of the pair is the cdc update of a table.
     * The right of the pair is the header/metadata.
     *
     * @param keyspace Cassandra keyspace
     * @param table    Cassandra table
     * @param data     serialized Avro message
     * @param schema   Avro schema
     * @return returned deserialized CdcEnvelope wrapping the payload and header
     */
    @Deprecated
    public CdcEnvelope deserialize(String keyspace, String table, byte[] data, Schema schema)
    {
        return deserializer.deserialize(keyspace, table, data, schema);
    }
}
