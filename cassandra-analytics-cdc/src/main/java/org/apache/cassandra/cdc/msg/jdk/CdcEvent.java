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

package org.apache.cassandra.cdc.msg.jdk;

import java.nio.ByteBuffer;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.MessageConverter;
import org.apache.cassandra.cdc.msg.AbstractCdcEvent;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Defines a CdcEvent that converts to a `org.apache.cassandra.cdc.msg.jdk.CdcMessage` deserializing the mutation values to Java types.
 */
public class CdcEvent extends AbstractCdcEvent<Value, RangeTombstone> implements MessageConverter<CdcMessage>
{
    CdcEvent(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
    {
        super(kind, partition, trackingId, cassandraSource);
    }

    protected CdcEvent(Kind kind, String keyspace, String table, String trackingId, CassandraSource cassandraSource)
    {
        super(kind, keyspace, table, trackingId, cassandraSource);
    }

    public RangeTombstoneBuilder rangeTombstoneBuilder(TableMetadata metadata)
    {
        return new RangeTombstoneBuilder(metadata);
    }

    public Value makeValue(String keyspace, String name, String type, ByteBuffer value)
    {
        return new Value(keyspace, name, type, value);
    }

    public CdcMessage toCdcMessage()
    {
        return new CdcMessage(this);
    }

    public static class Builder extends EventBuilder<Value, RangeTombstone, CdcEvent>
    {
        public static Builder of(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
        {
            return new Builder(kind, partition, trackingId, cassandraSource);
        }

        private Builder(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
        {
            super(kind, partition, trackingId, cassandraSource);
        }

        public CdcEvent buildEvent(Kind kind, UnfilteredRowIterator partition, String trackingId, CassandraSource cassandraSource)
        {
            return new CdcEvent(kind, partition, trackingId, cassandraSource);
        }
    }
}
