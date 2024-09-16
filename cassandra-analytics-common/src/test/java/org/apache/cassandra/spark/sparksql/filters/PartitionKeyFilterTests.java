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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class PartitionKeyFilterTests
{
    @Test
    public void testJDKSerialization()
    {
        testJDKSerialization(Partitioner.Murmur3Partitioner);
        testJDKSerialization(Partitioner.RandomPartitioner);
    }

    private void testJDKSerialization(Partitioner partitioner)
    {
        PartitionKeyFilter filter1 = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[]{'a', 'b', 'c'}), partitioner.maxToken());
        PartitionKeyFilter filter2 = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[]{'d', 'e', 'f'}), partitioner.maxToken());
        PartitionKeyFilter filter3 = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[]{'g', 'h', 'i'}), partitioner.minToken());
        PartitionKeyFilter filter4 = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[]{'a', 'b', 'c'}), partitioner.minToken());

        assertEquals(filter1, filter1);
        assertEquals(filter2, filter2);
        assertEquals(filter3, filter3);
        assertNotEquals(filter1, filter2);
        assertNotEquals(filter1, filter3);
        assertNotEquals(filter2, filter3);
        assertNotEquals(filter1, filter4);

        byte[] bytes1 = serialize(filter1);
        byte[] bytes2 = serialize(filter2);
        byte[] bytes3 = serialize(filter3);

        // should be able to serialize without changing state of ByteBuffer
        byte[] bytes1Again = serialize(filter1);
        assertArrayEquals(bytes1, bytes1Again);

        PartitionKeyFilter deserialized1 = deserialize(bytes1, PartitionKeyFilter.class);
        PartitionKeyFilter deserialized2 = deserialize(bytes2, PartitionKeyFilter.class);
        PartitionKeyFilter deserialized3 = deserialize(bytes3, PartitionKeyFilter.class);

        assertEquals(filter1, deserialized1);
        assertNotEquals(filter2, deserialized1);
        assertEquals(filter2, deserialized2);
        assertEquals(filter2, deserialized2);
        assertEquals(filter3, deserialized3);
        assertEquals(filter3, deserialized3);
    }

    public static byte[] serialize(Object object)
    {
        try (ByteArrayOutputStream arOut = new ByteArrayOutputStream(512))
        {
            try (ObjectOutputStream out = new ObjectOutputStream(arOut))
            {
                out.writeObject(object);
            }
            return arOut.toByteArray();
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> type)
    {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes)))
        {
            return type.cast(in.readObject());
        }
        catch (IOException | ClassNotFoundException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
