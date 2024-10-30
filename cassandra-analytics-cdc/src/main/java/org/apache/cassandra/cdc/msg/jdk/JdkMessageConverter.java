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

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.MessageConverter;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.RangeTombstone;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.data.CassandraTypes;

public class JdkMessageConverter implements MessageConverter<Column, RangeTombstoneMsg, CdcMessage>
{
    final CassandraTypes types;

    public JdkMessageConverter(CassandraVersion version)
    {
        this(CdcBridgeFactory.get(version).cassandraTypes());
    }

    public JdkMessageConverter(CassandraTypes types)
    {
        this.types = types;
    }

    public Column toCdcMessage(Value value)
    {
        return new Column(value.columnName, types.parseType(value.columnType), value.getValue());
    }

    public RangeTombstoneMsg toCdcMessage(RangeTombstone value)
    {
        return new RangeTombstoneMsg(this, value);
    }

    public CdcMessage toCdcMessage(CdcEvent event)
    {
        return new CdcMessage(this, event);
    }
}
