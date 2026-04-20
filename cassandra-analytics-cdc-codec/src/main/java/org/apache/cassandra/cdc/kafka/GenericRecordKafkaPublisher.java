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
