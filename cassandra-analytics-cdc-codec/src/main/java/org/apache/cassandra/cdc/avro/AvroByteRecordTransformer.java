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

import org.apache.avro.generic.GenericData;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;

import static org.apache.cassandra.cdc.avro.AvroByteRecordTransformer.ByteRecordSerializedEvent;

public class AvroByteRecordTransformer extends AvroBaseRecordTransformer<ByteRecordSerializedEvent, byte[]>
{
    public AvroByteRecordTransformer(SchemaStore schemaStore,
                                     Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        this(schemaStore, typeLookup, DEFAULT_TRUNCATE_THRESHOLD);
    }

    public AvroByteRecordTransformer(SchemaStore schemaStore,
                                     Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                                     int truncateThreshold)
    {
        super(schemaStore, typeLookup, truncateThreshold, "cdc_bytes.avsc");
    }

    @Override
    public ByteRecordSerializedEvent serializeEvent(CdcEvent event)
    {
        CdcEventUtils.UpdatedEvent updatedEvent = CdcEventUtils.getUpdatedEvent(event, store, truncateThreshold, typeLookup);
        return new ByteRecordSerializedEvent(
        encode(store.getWriter(event.keyspace + '.' + event.table, null), updatedEvent.getRecord()),
        updatedEvent.getTruncatedFields()
        );
    }

    @Override
    public GenericData.Record buildRecordWithPayload(ByteRecordSerializedEvent serializedEvent)
    {
        final byte[] payload = serializedEvent.payload;
        final GenericData.Record record = new GenericData.Record(cdcSchema);
        record.put(AvroConstants.PAYLOAD_KEY, ByteBuffer.wrap(payload));
        return record;
    }

    public static class ByteRecordSerializedEvent extends AvroBaseRecordTransformer.BaseSerializedEvent<byte[]>
    {
        private ByteRecordSerializedEvent(byte[] payload, List<String> truncatedFields)
        {
            super(payload, truncatedFields);
        }
    }
}
