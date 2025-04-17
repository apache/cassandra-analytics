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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.Preconditions;

import static org.apache.cassandra.cdc.avro.AvroGenericRecordTransformer.GenericRecordSerializedEvent;

public class AvroGenericRecordTransformer extends AvroBaseRecordTransformer<GenericRecordSerializedEvent, GenericRecord>
{
    final String schemaNamespacePrefix;

    public AvroGenericRecordTransformer(SchemaStore schemaStore,
                                        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                        String schemaNamespacePrefix)
    {
        this(schemaStore, typeLookup, DEFAULT_TRUNCATE_THRESHOLD, schemaNamespacePrefix);
    }

    public AvroGenericRecordTransformer(SchemaStore schemaStore,
                                        Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                        int truncateThreshold,
                                        String schemaNamespacePrefix)
    {
        super(schemaStore, typeLookup, truncateThreshold, "cdc_generic_record.avsc");
        this.schemaNamespacePrefix = schemaNamespacePrefix;
    }

    @Override
    public GenericRecordSerializedEvent serializeEvent(CdcEvent event)
    {
        CdcEventUtils.UpdatedEvent updatedEvent = CdcEventUtils.getUpdatedEvent(event, store, truncateThreshold, typeLookup);
        return new GenericRecordSerializedEvent(updatedEvent.getRecord(), updatedEvent.getTruncatedFields(), event.table, event.keyspace);
    }

    @Override
    public GenericData.Record buildRecordWithPayload(GenericRecordSerializedEvent serializedEvent)
    {
        GenericRecord payload = serializedEvent.payload;
        Schema newSchema = getTempSchemaForEvent(serializedEvent);
        GenericData.Record record = new GenericData.Record(newSchema);
        record.put(AvroConstants.PAYLOAD_KEY, payload);
        return record;
    }

    public Schema getTempSchemaForEvent(GenericRecordSerializedEvent event)
    {
        String name = cdcSchema.getName();
        String namespace = cdcSchema.getNamespace();
        if (!Preconditions.isNullOrEmpty(schemaNamespacePrefix))
        {
            name = event.table;
            namespace = schemaNamespacePrefix + '.' + event.keyspace;
        }
        Schema tempSchema = Schema.createRecord(name, "schema", namespace, false);
        Schema.Field payloadField = new Schema.Field(AvroConstants.PAYLOAD_KEY, event.payload.getSchema(), AvroConstants.PAYLOAD_KEY);

        // Avro schema fields can't be modified. That's why we need to create a new schema with the
        // dynamically generated payload field.
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field f : cdcSchema.getFields())
        {
            if (!f.name().equals(AvroConstants.PAYLOAD_KEY) && !f.name().equals("namespace") && !f.name().equals("name"))
            {
                Schema.Field ff = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
                fields.add(ff);
            }
        }

        fields.add(payloadField);
        tempSchema.setFields(fields);
        return tempSchema;
    }

    /**
     * Serialized event with payload in {@link GenericRecord}
     */
    public static class GenericRecordSerializedEvent extends AvroBaseRecordTransformer.BaseSerializedEvent<GenericRecord>
    {
        private final String table;
        private final String keyspace;

        public GenericRecordSerializedEvent(GenericRecord payload,
                                            List<String> truncatedFields,
                                            String table,
                                            String keyspace)
        {
            super(payload, truncatedFields);
            this.table = table;
            this.keyspace = keyspace;
        }
    }
}
