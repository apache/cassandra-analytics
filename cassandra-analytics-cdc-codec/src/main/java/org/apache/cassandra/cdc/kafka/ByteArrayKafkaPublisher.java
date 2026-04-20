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

import java.io.ByteArrayOutputStream;
import java.util.function.Function;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.CdcEventTransformer;
import org.apache.cassandra.cdc.CdcLogMode;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroByteRecordTransformer;
import org.apache.cassandra.cdc.avro.AvroDataUtils;
import org.apache.cassandra.cdc.avro.CdcEventAvroEncoder;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * {@link KafkaPublisher} for the no-registry path where {@code value.serializer} is
 * {@code org.apache.kafka.common.serialization.ByteArraySerializer}.
 *
 * <p>Overrides {@link #getPayload} to Avro-encode the transformed {@link GenericData.Record}
 * to {@code byte[]} before handing it to the producer, since {@code ByteArraySerializer}
 * is a pass-through and expects pre-encoded bytes.
 */
public class ByteArrayKafkaPublisher extends KafkaPublisher<byte[]> {
    private final CdcEventTransformer<GenericData.Record> transformer;
    private final BinaryEncoder encoderReuse;

    ByteArrayKafkaPublisher(CassandraVersion version,
                            TopicSupplier topicSupplier,
                            KafkaProducer<String, byte[]> producer,
                            SchemaStore schemaStore,
                            Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                            int maxRecordSizeBytes,
                            boolean failOnRecordTooLargeError,
                            boolean failOnKafkaError,
                            CdcLogMode logMode) {
        this(version, topicSupplier, producer, schemaStore, typeLookup,
                maxRecordSizeBytes, failOnRecordTooLargeError, failOnKafkaError, logMode,
                KafkaStats.STUB, RecordProducer.defaultProducer(), EventHasher.MURMUR2);
    }

    ByteArrayKafkaPublisher(CassandraVersion version,
                            TopicSupplier topicSupplier,
                            KafkaProducer<String, byte[]> producer,
                            SchemaStore schemaStore,
                            Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                            int maxRecordSizeBytes,
                            boolean failOnRecordTooLargeError,
                            boolean failOnKafkaError,
                            CdcLogMode logMode,
                            KafkaStats kafkaStats,
                            RecordProducer<byte[]> recordProducer,
                            EventHasher eventHasher) {
        super(version, topicSupplier, producer, schemaStore, maxRecordSizeBytes, failOnRecordTooLargeError,
                failOnKafkaError, logMode, kafkaStats, recordProducer, eventHasher);
        this.transformer = new AvroByteRecordTransformer(schemaStore, typeLookup);
        this.encoderReuse = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(0), null);
    }

    @Override
    protected byte[] getPayload(CdcEvent cdcEvent) {
        GenericData.Record record = transformer.transform(cdcEvent);
        return AvroDataUtils.encode(new GenericDatumWriter<>(record.getSchema()), record, encoderReuse);
    }
}
