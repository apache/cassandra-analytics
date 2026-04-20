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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Builds Kafka {@link ProducerRecord} objects for CDC events.
 *
 * <p>Every produced record carries at minimum {@code keyspace} and {@code table} headers.
 * When a {@code schemaUuid} is provided it is also added as the {@value #SCHEMA_UUID_HEADER} header,
 * allowing consumers to look up the correct Avro schema.
 *
 * @param <V> the Kafka producer value type ({@code GenericData.Record} for PIE/Confluent,
 *            {@code byte[]} for the no-registry path)
 */
public interface RecordProducer<V>
{
    String KEYSPACE_HEADER = "keyspace";
    String TABLE_HEADER = "table";
    String SCHEMA_UUID_HEADER = "schemaUuid";

    RecordProducer<?> DEFAULT_INSTANCE = new RecordProducer<Object>()
    {
    };

    @SuppressWarnings("unchecked")
    static <V> RecordProducer<V> defaultProducer()
    {
        return (RecordProducer<V>) DEFAULT_INSTANCE;
    }

    default ProducerRecord<String, V> buildRecord(String keyspace,
                                                  String table,
                                                  String topic,
                                                  String key,
                                                  V payload)
    {
        return buildRecord(keyspace, table, topic, key, payload, null);
    }

    default ProducerRecord<String, V> buildRecord(String keyspace,
                                                  String table,
                                                  String topic,
                                                  String key,
                                                  V payload,
                                                  String schemaUuid)
    {
        ProducerRecord<String, V> record = new ProducerRecord<>(topic, key, payload);
        RecordProducer.addHeader(record, RecordProducer.KEYSPACE_HEADER, keyspace);
        RecordProducer.addHeader(record, RecordProducer.TABLE_HEADER, table);
        if (schemaUuid != null)
        {
            RecordProducer.addHeader(record, RecordProducer.SCHEMA_UUID_HEADER, schemaUuid);
        }
        return record;
    }

    default List<ProducerRecord<String, V>> buildRecords(CdcEvent cdcEvent,
                                                         String topic,
                                                         String key,
                                                         V payload)
    {
        return buildRecords(cdcEvent.keyspace, cdcEvent.table, topic, key, payload, null);
    }

    default List<ProducerRecord<String, V>> buildRecords(CdcEvent cdcEvent,
                                                         String topic,
                                                         String key,
                                                         V payload,
                                                         String schemaUuid)
    {
        return buildRecords(cdcEvent.keyspace, cdcEvent.table, topic, key, payload, schemaUuid);
    }

    default List<ProducerRecord<String, V>> buildRecords(String keyspace,
                                                         String table,
                                                         String topic,
                                                         String key,
                                                         V payload)
    {
        return buildRecords(keyspace, table, topic, key, payload, null);
    }

    default List<ProducerRecord<String, V>> buildRecords(String keyspace,
                                                         String table,
                                                         String topic,
                                                         String key,
                                                         V payload,
                                                         String schemaUuid)
    {
        return Collections.singletonList(buildRecord(keyspace, table, topic, key, payload, schemaUuid));
    }

    static <K, V> void addHeader(ProducerRecord<K, V> record, String name, short value)
    {
        addHeader(record, name, ByteBuffer.allocate(2).putShort(value).array());
    }

    static <K, V> void addHeader(ProducerRecord<K, V> record, String name, int value)
    {
        addHeader(record, name, ByteBuffer.allocate(4).putInt(value).array());
    }

    static <K, V> void addHeader(ProducerRecord<K, V> record, String name, String value)
    {
        addHeader(record, name, toBytes(value));
    }

    static <K, V> void addHeader(ProducerRecord<K, V> record, String name, byte[] value)
    {
        record.headers().add(name, value);
    }

    static byte[] toBytes(String str)
    {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
