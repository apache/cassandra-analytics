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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.Preconditions;

public abstract class AvroBaseRecordTransformer<T extends AvroBaseRecordTransformer.BaseSerializedEvent<P>, P>
extends CdcEventAvroEncoder
{
    // Kafka generally limits message size to 1 MiB, so we truncate messages
    // that are too large and update the truncated flag
    public static final int DEFAULT_TRUNCATE_THRESHOLD = (1 << 20) * 4 / 5; // 0.8 MiB = 1024 * 1024 * 0.8

    protected final int truncateThreshold;

    public AvroBaseRecordTransformer(SchemaStore store,
                                     Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                     String templatePath)
    {
        this(store, typeLookup, DEFAULT_TRUNCATE_THRESHOLD, templatePath);
    }

    public AvroBaseRecordTransformer(SchemaStore store,
                                     Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                     int truncateThreshold,
                                     String templatePath)
    {
        super(store, typeLookup, templatePath);
        this.truncateThreshold = truncateThreshold;
    }

    public abstract T serializeEvent(CdcEvent event);

    public abstract GenericData.Record buildRecordWithPayload(T serializedEvent);

    @Override
    public GenericData.Record transform(CdcEvent event)
    {
        T serializedEvent = serializeEvent(event);
        GenericData.Record record = buildRecordWithPayload(serializedEvent);

        Function<Value, Object> predicateFieldEncoder = field -> {
            ByteBuffer fieldValue = field.getValue();
            Preconditions.checkNotNull(fieldValue, "Field value of column %s should not be null for range predicate", field.columnName);
            Schema tableSchema = store.getSchema(event.keyspace + '.' + event.table, null);
            Schema.Field column = tableSchema.getField(field.columnName);
            Preconditions.checkNotNull(column,
                                       "Encountered an unknown field during range predicate encoding. " +
                                       "Field: %s. Avro schema: %s", field.columnName, tableSchema.getFullName());

            GenericData.Record update = new GenericData.Record(tableSchema);
            CqlField.CqlType type = typeLookup.apply(KeyspaceTypeKey.of(event.keyspace, field.columnType));
            Object javaValue = type.deserializeToJavaType(fieldValue);
            update.put(field.columnName, AvroDataUtils.toAvro(javaValue, column.schema()));
            return ByteBuffer.wrap(encode(store.getWriter(event.keyspace + '.' + event.table, null), update));
        };
        applyCommonFields(event, record, predicateFieldEncoder);
        String schemaUuid = store.getVersion(event.keyspace + '.' + event.table, null);
        record.put(AvroConstants.SCHEMA_UUID_KEY, schemaUuid);
        record.put(AvroConstants.TRUNCATED_FIELDS_KEY, serializedEvent.truncatedFields);
        return record;
    }

    /**
     * POJO of a serialized event
     * @param <P> serialized payload type
     */
    public static abstract class BaseSerializedEvent<P>
    {
        public final P payload;
        public final List<String> truncatedFields;

        public BaseSerializedEvent(P payload, List<String> truncatedFields)
        {
            this.payload = payload;
            this.truncatedFields = truncatedFields;
        }
    }
}
