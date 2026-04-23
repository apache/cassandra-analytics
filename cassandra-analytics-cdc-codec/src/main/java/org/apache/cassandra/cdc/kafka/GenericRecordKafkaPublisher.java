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

import java.util.function.Function;

import org.apache.avro.generic.GenericData;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.CdcEventTransformer;
import org.apache.cassandra.cdc.CdcLogMode;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroGenericRecordTransformer;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * {@link KafkaPublisher} for schema-registry paths where {@code value.serializer} is an
 * Avro-aware serializer that accepts {@link GenericData.Record} directly.
 *
 * <p>Each CDC event is transformed into a merged Avro record whose schema combines the fixed CDC
 * envelope ({@code cdc_generic_record.avsc}) with the table-specific payload schema produced by
 * {@link org.apache.cassandra.cdc.avro.AvroGenericRecordTransformer}. The Avro serializer
 * registered with the Kafka producer handles encoding and schema registration.
 *
 * <p>{@code schemaNamespacePrefix} controls the Avro schema identity of each produced record:
 * <ul>
 *   <li>If non-empty, the merged schema is given the name of the source table and the namespace
 *       {@code <schemaNamespacePrefix>.<keyspace>}, producing a unique schema per table that can
 *       be registered independently in the schema registry.</li>
 *   <li>If empty, the name and namespace from the CDC envelope template are used for all tables,
 *       resulting in a single shared schema registration.</li>
 * </ul>
 */
public class GenericRecordKafkaPublisher extends KafkaPublisher<GenericData.Record>
{
    private final CdcEventTransformer<GenericData.Record> transformer;

    GenericRecordKafkaPublisher(CassandraVersion version,
                                TopicSupplier topicSupplier,
                                KafkaProducer<String, GenericData.Record> producer,
                                SchemaStore schemaStore,
                                Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                String schemaNamespacePrefix,
                                int maxRecordSizeBytes,
                                boolean failOnRecordTooLargeError,
                                boolean failOnKafkaError,
                                CdcLogMode logMode)
    {
        this(version, topicSupplier, producer, schemaStore, typeLookup, schemaNamespacePrefix,
             maxRecordSizeBytes, failOnRecordTooLargeError, failOnKafkaError, logMode,
             KafkaStats.STUB, RecordProducer.defaultProducer(), EventHasher.MURMUR2);
    }

    GenericRecordKafkaPublisher(CassandraVersion version,
                                TopicSupplier topicSupplier,
                                KafkaProducer<String, GenericData.Record> producer,
                                SchemaStore schemaStore,
                                Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                String schemaNamespacePrefix,
                                int maxRecordSizeBytes,
                                boolean failOnRecordTooLargeError,
                                boolean failOnKafkaError,
                                CdcLogMode logMode,
                                KafkaStats kafkaStats,
                                RecordProducer<GenericData.Record> recordProducer,
                                EventHasher eventHasher)
    {
        super(version, topicSupplier, producer, schemaStore,
              maxRecordSizeBytes, failOnRecordTooLargeError, failOnKafkaError, logMode,
              kafkaStats, recordProducer, eventHasher);
        this.transformer = new AvroGenericRecordTransformer(schemaStore, typeLookup, schemaNamespacePrefix);
    }

    @Override
    protected GenericData.Record getPayload(CdcEvent cdcEvent)
    {
        return transformer.transform(cdcEvent);
    }
}
