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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.CdcEventBuilder;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.RandomUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonSerializerTests
{
    private static final CassandraTypes TYPES = CdcBridgeFactory.get(CassandraVersion.FOURZERO).cassandraTypes();
    private static final Function<KeyspaceTypeKey, CqlField.CqlType> TYPE_LOOKUP = (key) -> TYPES.parseType(key.keyspace, key.type);
    private static final String TEST_KS = "test_ks";
    private static final String TEST_TBL_BASIC = "test_tbl_basic";
    private static final String TEST_TBL_BINARY = "test_tbl_binary";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testJsonSerializer() throws IOException
    {
        CdcEventBuilder eventBuilder = CdcEventBuilder.of(CdcEvent.Kind.INSERT, TEST_KS, TEST_TBL_BASIC);
        eventBuilder.setPartitionKeys(List.of(Value.of(TEST_KS, "a", "int", TYPES.aInt().serialize(1))));
        eventBuilder.setValueColumns(List.of(
        Value.of(TEST_KS, "b", "int", TYPES.aInt().serialize(2)),
        Value.of(TEST_KS, "c", "int", TYPES.aInt().serialize(3))
        ));
        eventBuilder.setMaxTimestampMicros(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        eventBuilder.setTimeToLive(new CdcEvent.TimeToLive(10, 1658269));

        byte[] ar;
        try (JsonSerializer serializer = new JsonSerializer(TYPE_LOOKUP))
        {
            ar = serializer.serialize("topic", eventBuilder.build());
        }
        assertNotNull(ar);

        JsonNode root = MAPPER.readTree(ar);
        JsonNode payload = root.get("payload");
        assertEquals(1, payload.get("a").asInt());
        assertEquals(2, payload.get("b").asInt());
        assertEquals(3, payload.get("c").asInt());
        assertTrue(root.has("updateFields"));
        JsonNode updatedFields = root.get("updateFields");
        assertTrue(updatedFields.isArray());
        assertEquals("a", root.get("updateFields").get(0).textValue());
        assertEquals("b", root.get("updateFields").get(1).textValue());
        assertEquals("c", root.get("updateFields").get(2).textValue());
        Iterator<JsonNode> fields = updatedFields.elements();
        while (fields.hasNext())
        {
            JsonNode field = fields.next();
            assertTrue(payload.has(field.textValue()));
            assertNotEquals("null", payload.get(field.textValue()));
        }
        // these 2 fields are not updated, hence having null value and not being included in updateFields
        assertSame(NullNode.instance, payload.get("e"));
        assertSame(NullNode.instance, payload.get("f"));
        assertEquals("INSERT", root.get("operationType").asText());
        assertTrue(root.has("timestampMicros"));
        assertTrue(root.has("ttl"));
        JsonNode ttl = root.get("ttl");
        assertEquals(10, ttl.get("ttl").asInt());
        assertEquals(1658269, ttl.get("deletedAt").asInt());
    }

    @Test
    public void testJsonBinary() throws IOException
    {
        CdcEventBuilder eventBuilder = CdcEventBuilder.of(CdcEvent.Kind.INSERT, TEST_KS, TEST_TBL_BINARY);
        eventBuilder.setPartitionKeys(List.of(Value.of(TEST_KS, "a", "int", TYPES.aInt().serialize(1))));
        ByteBuffer randomBytes = ByteBuffer.wrap(RandomUtils.randomBytes(128));
        eventBuilder.setValueColumns(List.of(
        Value.of(TEST_KS, "b", "blob", TYPES.blob().serialize(randomBytes)),
        Value.of(TEST_KS, "c", "inet", TYPES.inet().serialize(InetAddress.getByName("127.0.0.1")))
        ));
        eventBuilder.setMaxTimestampMicros(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        eventBuilder.setTimeToLive(new CdcEvent.TimeToLive(10, 1658269));

        byte[] ar;
        try (JsonSerializer serializer = new JsonSerializer(TYPE_LOOKUP))
        {
            ar = serializer.serialize("topic", eventBuilder.build());
        }
        assertNotNull(ar);
        JsonNode root = MAPPER.readTree(ar);
        JsonNode payload = root.get("payload");

        // assert on column b
        assertTrue(payload.has("b"));
        String base64Str = payload.get("b").asText();
        ByteBuffer decoded = ByteBuffer.wrap(Base64.getDecoder().decode(base64Str));
        assertEquals(randomBytes, decoded);

        // assert on colum c
        assertTrue(payload.has("c"));
        base64Str = payload.get("c").asText();
        InetAddress address = InetAddress.getByAddress(Base64.getDecoder().decode(base64Str));
        assertEquals(InetAddress.getByName("127.0.0.1"), address);
    }
}
