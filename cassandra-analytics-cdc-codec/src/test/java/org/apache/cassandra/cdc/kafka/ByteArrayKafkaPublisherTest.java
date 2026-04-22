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

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.CdcLogMode;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroConstants;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.CdcEventBuilder;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.LocalTableSchemaStore;
import org.apache.cassandra.spark.data.CqlField;

import static org.apache.cassandra.spark.utils.ArrayUtils.listOf;
import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayKafkaPublisherTest
{
    /**
     * Verifies that {@link ByteArrayKafkaPublisher#getPayload} returns Avro-encoded bytes
     * that can be decoded back into a CDC envelope with the expected keyspace and table metadata.
     */
    @Test
    public void getPayloadProducesAvroDecodableBytes() throws IOException
    {
        LocalTableSchemaStore store = LocalTableSchemaStore.getInstance();
        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup =
                key -> TypeCache.get(CassandraVersion.FOURZERO).getType(key.keyspace, key.type);

        ByteArrayKafkaPublisher publisher = new ByteArrayKafkaPublisher(
                CassandraVersion.FOURZERO,
                event -> "test-topic",
                null,
                store,
                typeLookup,
                1_000_000, false, false,
                CdcLogMode.MINIMAL);

        CassandraBridge bridge = CdcBridgeFactory.get(CassandraVersion.FOURZERO);
        CdcEventBuilder builder = CdcEventBuilder.of(CdcEvent.Kind.UPDATE, "test_ks", "test_tbl_basic");
        builder.setPartitionKeys(listOf(
                Value.of("test_ks", "a", "int", bridge.aInt().serialize(42))
        ));
        builder.setValueColumns(Collections.singletonList(
                Value.of("test_ks", "b", "int", bridge.aInt().serialize(7))
        ));
        CdcEvent event = builder.build();

        byte[] bytes = publisher.getPayload(event);
        assertThat(bytes).isNotEmpty();

        // The bytes are the Avro-encoded CDC envelope (cdc_bytes.avsc).
        // Decode with the cdc_bytes schema and verify the keyspace/table metadata fields.
        Schema cdcBytesSchema = new Schema.Parser().parse(
                ByteArrayKafkaPublisherTest.class.getClassLoader().getResourceAsStream("cdc_bytes.avsc"));
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord envelope = new GenericDatumReader<GenericRecord>(cdcBytesSchema).read(null, decoder);

        assertThat(envelope.get(AvroConstants.SOURCE_TABLE_KEY).toString()).isEqualTo("test_tbl_basic");
        assertThat(envelope.get(AvroConstants.SOURCE_KEYSPACE_KEY).toString()).isEqualTo("test_ks");
    }
}
