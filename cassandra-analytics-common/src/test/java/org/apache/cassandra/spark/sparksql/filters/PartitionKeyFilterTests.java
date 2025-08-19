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

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(filter1).isEqualTo(filter1);
        assertThat(filter2).isEqualTo(filter2);
        assertThat(filter3).isEqualTo(filter3);
        assertThat(filter1).isNotEqualTo(filter2);
        assertThat(filter1).isNotEqualTo(filter3);
        assertThat(filter2).isNotEqualTo(filter3);
        assertThat(filter1).isNotEqualTo(filter4);

        byte[] bytes1 = serialize(filter1);
        byte[] bytes2 = serialize(filter2);
        byte[] bytes3 = serialize(filter3);

        // should be able to serialize without changing state of ByteBuffer
        byte[] bytes1Again = serialize(filter1);
        assertThat(bytes1Again).isEqualTo(bytes1);

        PartitionKeyFilter deserialized1 = deserialize(bytes1, PartitionKeyFilter.class);
        PartitionKeyFilter deserialized2 = deserialize(bytes2, PartitionKeyFilter.class);
        PartitionKeyFilter deserialized3 = deserialize(bytes3, PartitionKeyFilter.class);

        assertThat(deserialized1).isEqualTo(filter1);
        assertThat(deserialized1).isNotEqualTo(filter2);
        assertThat(deserialized2).isEqualTo(filter2);
        assertThat(deserialized2).isEqualTo(filter2);
        assertThat(deserialized3).isEqualTo(filter3);
        assertThat(deserialized3).isEqualTo(filter3);
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
