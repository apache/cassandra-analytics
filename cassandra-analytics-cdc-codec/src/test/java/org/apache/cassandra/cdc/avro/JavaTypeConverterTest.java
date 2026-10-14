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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.apache.cassandra.spark.utils.ArrayUtils.setOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JavaTypeConverter}
 */
public class JavaTypeConverterTest
{
    private static final String KEYSPACE = "java_type_converter_ks";
    private static final ReplicationFactor REPLICATION_FACTOR =
    new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, Collections.singletonMap("replication_factor", 1));

    @Test
    public void testUdtInCollectionInTuple()
    {
        // tuple elements are deserialized without a converter, so nested collections and UDTs must still be converted
        CqlTable table = new CassandraBridgeImplementation()
                         .buildSchema("CREATE TABLE " + KEYSPACE + ".t (id int PRIMARY KEY, " +
                                      "c frozen<tuple<int, frozen<list<frozen<address>>>>>);",
                                      KEYSPACE, REPLICATION_FACTOR, Partitioner.Murmur3Partitioner,
                                      setOf("CREATE TYPE " + KEYSPACE + ".address (city text);"));
        CqlField.CqlType type = table.getField("c").type();
        ByteBuffer address = concat(false, ByteBuffer.wrap("Paris".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer serialized = concat(false, ByteBuffer.allocate(4).putInt(0, 7), concat(true, address));

        Object[] value = (Object[]) JavaTypeConverter.deserialize(type, serialized);

        assertThat(value).containsExactly(7, Collections.singletonList(Map.of("city", "Paris")));
    }

    // values encoded as [int length][bytes], optionally prefixed by the number of values (collections)
    private static ByteBuffer concat(boolean withCount, ByteBuffer... values)
    {
        int size = withCount ? Integer.BYTES : 0;
        for (ByteBuffer value : values)
        {
            size += Integer.BYTES + value.remaining();
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        if (withCount)
        {
            result.putInt(values.length);
        }
        for (ByteBuffer value : values)
        {
            result.putInt(value.remaining());
            result.put(value.duplicate());
        }
        result.flip();
        return result;
    }
}
