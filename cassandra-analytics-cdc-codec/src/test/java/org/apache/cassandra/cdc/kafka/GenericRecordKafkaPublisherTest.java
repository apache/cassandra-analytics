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

import java.util.Collections;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

public class GenericRecordKafkaPublisherTest
{
    /**
     * Verifies that {@link GenericRecordKafkaPublisher#getPayload} returns a non-null
     * {@link GenericData.Record} whose {@code payload} field is a {@link GenericRecord}
     * containing the table-specific row data.
     */
    @Test
    public void getPayloadReturnsGenericRecordWithPayloadField()
    {
        GenericRecordKafkaPublisher publisher = publisher("");
        CdcEvent event = makeEvent();

        GenericData.Record record = publisher.getPayload(event);

        assertThat(record).isNotNull();
        assertThat(record.get(AvroConstants.PAYLOAD_KEY)).isInstanceOf(GenericRecord.class);
    }

    /**
     * Verifies that when {@code schemaNamespacePrefix} is empty the merged schema uses the
     * name ({@code CassandraCDC}) and namespace ({@code org.apache.cassandra}) from the
     * {@code cdc_generic_record.avsc} envelope template, so all tables share a single
     * schema identity in the registry.
     */
    @Test
    public void getPayloadSchemaUsesEnvelopeTemplateNameWhenPrefixEmpty()
    {
        GenericRecordKafkaPublisher publisher = publisher("");
        CdcEvent event = makeEvent();

        GenericData.Record record = publisher.getPayload(event);

        assertThat(record.getSchema().getName()).isEqualTo("CassandraCDC");
        assertThat(record.getSchema().getNamespace()).isEqualTo("org.apache.cassandra");
    }

    /**
     * Verifies that when {@code schemaNamespacePrefix} is non-empty the merged schema is named
     * after the source table with namespace {@code <prefix>.<keyspace>}, producing a unique
     * schema identity per table in the registry.
     */
    @Test
    public void getPayloadSchemaUsesTableNameAndPrefixWhenPrefixNonEmpty()
    {
        GenericRecordKafkaPublisher publisher = publisher("com.example");
        CdcEvent event = makeEvent();

        GenericData.Record record = publisher.getPayload(event);

        assertThat(record.getSchema().getName()).isEqualTo("test_tbl_basic");
        assertThat(record.getSchema().getNamespace()).isEqualTo("com.example.test_ks");
    }

    private static GenericRecordKafkaPublisher publisher(String schemaNamespacePrefix)
    {
        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup =
                key -> TypeCache.get(CassandraVersion.FOURZERO).getType(key.keyspace, key.type);
        return new GenericRecordKafkaPublisher(
                CassandraVersion.FOURZERO,
                event -> "test-topic",
                null,
                LocalTableSchemaStore.getInstance(),
                typeLookup,
                schemaNamespacePrefix,
                1_000_000, false, false,
                CdcLogMode.MINIMAL);
    }

    private static CdcEvent makeEvent()
    {
        CassandraBridge bridge = CdcBridgeFactory.get(CassandraVersion.FOURZERO);
        CdcEventBuilder builder = CdcEventBuilder.of(CdcEvent.Kind.UPDATE, "test_ks", "test_tbl_basic");
        builder.setPartitionKeys(listOf(
                Value.of("test_ks", "a", "int", bridge.aInt().serialize(42))
        ));
        builder.setValueColumns(Collections.singletonList(
                Value.of("test_ks", "b", "int", bridge.aInt().serialize(7))
        ));
        return builder.build();
    }
}
