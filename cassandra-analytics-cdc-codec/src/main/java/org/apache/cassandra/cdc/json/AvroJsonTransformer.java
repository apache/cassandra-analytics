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

package org.apache.cassandra.cdc.json;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.cdc.avro.CdcEventAvroEncoder;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.avro.AvroDataUtils;
import org.apache.cassandra.cdc.avro.CdcEventUtils;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;

public class AvroJsonTransformer extends CdcEventAvroEncoder
{
    public AvroJsonTransformer(SchemaStore schemaStore, Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        super(schemaStore, typeLookup, "cdc_bytes.avsc");
    }

    @Override
    public GenericData.Record transform(CdcEvent event)
    {
        final GenericData.Record payload = transformPayload(event, typeLookup);
        final long timestamp = event.getTimestamp(TimeUnit.MICROSECONDS);
        final GenericData.Record record = new GenericData.Record(cdcSchema);
        record.put("payload", payload);
        record.put("timestampMicros", timestamp);
        record.put("version", "2");
        record.put("isPartial", true);
        record.put("sourceTable", event.table);
        record.put("sourceKeyspace", event.keyspace);
        record.put("operationType", CdcEventUtils.getAvroOperationType(event, cdcSchema));
        record.put("updateFields", CdcEventUtils.updatedFieldNames(event));
        Function<Value, Object> avroFieldEncoder = field -> {
            Schema tableSchema = store.getSchema(event.keyspace + '.' + event.table, null);
            GenericData.Record update = new GenericData.Record(tableSchema);
            CqlField.CqlType type = typeLookup.apply(KeyspaceTypeKey.of(event.keyspace, field.columnType));
            Object javaValue = type.deserializeToJavaType(field.getValue());
            update.put(field.columnName, AvroDataUtils.toAvro(javaValue, tableSchema.getField(field.columnName).schema()));
            return ByteBuffer.wrap(encode(store.getWriter(event.keyspace + '.' + event.table, null), update));
        };
        record.put("range", CdcEventUtils.getRangeTombstoneAvro(event, rangeSchema, avroFieldEncoder));
        record.put("ttl", CdcEventUtils.getTTLAvro(event, ttlSchema));
        return record;
    }

    private GenericData.Record transformPayload(CdcEvent event, Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        Schema tableSchema = store.getSchema(event.keyspace + '.' + event.table, null);
        GenericData.Record update = new GenericData.Record(tableSchema);
        CdcEventUtils.updatedFields(event)
                     .forEach(field -> {
                         CqlField.CqlType type = typeLookup.apply(KeyspaceTypeKey.of(event.keyspace, field.columnType));
                         Object javaValue = type.deserializeToJavaType(field.getValue());
                         update.put(field.columnName, AvroDataUtils.toAvro(javaValue, tableSchema.getField(field.columnName).schema()));
                     });
        return update;
    }
}
