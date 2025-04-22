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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.CdcEventBuilder;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.apache.cassandra.spark.utils.ArrayUtils.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaPublisherTest
{
    @Test
    public void hashPartitionKeyMatches()
    {
        CdcEvent event1 = makeEvent("str", 123, 456L,
                                    1.2, "bytes".getBytes(),
                                    listOf(1, 2), 1);
        CdcEvent event2 = makeEvent("str", 123, 456L,
                                        1.2, "bytes".getBytes(),
                                    listOf(1, 2), 1);
        assertEquals(EventHasher.MURMUR2.hashEvent(event1),
                     EventHasher.MURMUR2.hashEvent(event2),
                     "Hash should be the same from rows with the same partition key values");
        assertEquals(EventHasher.MURMUR2.hashEvent(event1),
                     EventHasher.MURMUR2.hashEvent(event1),
                     "Hash should be the same from the same row");
    }

    @Test
    public void hashPartitionKeysNotMatch()
    {
        CdcEvent event1 = makeEvent("foo", 123, 456L,
                                        1.2, "bytes".getBytes(),
                                        Arrays.asList(1, 2), 1);
        CdcEvent event2 = makeEvent("bar", 123, 456L,
                                        1.2, "much long bytes".getBytes(),
                                    Arrays.asList(1, 2), 1);
        assertNotEquals("Hash should be different from different rows",
                        EventHasher.MURMUR2.hashEvent(event1),
                        EventHasher.MURMUR2.hashEvent(event2));
    }

    @Test
    public void extractTableId()
    {
        TableIdentifier id = KafkaPublisher.extractTableIdFromPublishKey("ks:table:1234");
        assertEquals("ks", id.keyspace());
        assertEquals("table", id.table());
    }

    @Test
    public void extractTableIdThrows()
    {
        assertThrows(IllegalArgumentException.class, () -> KafkaPublisher.extractTableIdFromPublishKey(":ks:tbl:123"));
        assertThrows(IllegalArgumentException.class, () -> KafkaPublisher.extractTableIdFromPublishKey(":tbl:123"));
        assertThrows(IllegalArgumentException.class, () -> KafkaPublisher.extractTableIdFromPublishKey("ks::123"));
        assertThrows(IllegalArgumentException.class, () -> KafkaPublisher.extractTableIdFromPublishKey("na"));
    }

    private CdcEvent makeEvent(String primaryKey1Value,
                               int primaryKey2Value,
                               long primaryKey3Value,
                               double primaryKey4Value,
                               byte[] primaryKey5Value,
                               List<Integer> primaryKey6Value,
                               int valueColumnValue)
    {
        CassandraBridge bridge = CdcBridgeFactory.get(CassandraVersion.FOURZERO);
        CdcEventBuilder eventBuilder = CdcEventBuilder.of(CdcEvent.Kind.UPDATE, "ks", "tbl");
        eventBuilder.setPartitionKeys(listOf(
        Value.of("ks", "pk1", "text", bridge.text().serialize(primaryKey1Value)),
        Value.of("ks", "pk2", "int", bridge.aInt().serialize(primaryKey2Value)),
        Value.of("ks", "pk3", "bigint", bridge.bigint().serialize(primaryKey3Value)),
        Value.of("ks", "pk4", "double", bridge.aDouble().serialize(primaryKey4Value)),
        Value.of("ks", "pk5", "blob", ByteBuffer.wrap(primaryKey5Value)),
        Value.of("ks", "pk6", "list<int>", bridge.list(bridge.aInt()).serialize(primaryKey6Value))
        ));
        eventBuilder.setValueColumns(Collections.singletonList(
        Value.of("ks", "v", "int", bridge.aInt().serialize(valueColumnValue))
        ));
        return eventBuilder.build();
    }
}
