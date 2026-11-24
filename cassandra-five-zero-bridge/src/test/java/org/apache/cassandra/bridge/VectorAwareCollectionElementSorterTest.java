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

package org.apache.cassandra.bridge;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;

import static org.assertj.core.api.Assertions.assertThat;

class VectorAwareCollectionElementSorterTest
{
    private static final CollectionElementSorter SORTER = VectorAwareCollectionElementSorter.VECTOR_AWARE_INSTANCE;
    private static final SetType<String> TEXT_SET = SetType.getInstance(UTF8Type.instance, false);

    @Test
    void sortsTheSetsInTheElementsOfAVector()
    {
        VectorType<?> type = VectorType.getInstance(TEXT_SET, 2);

        assertThat(SORTER.nestsSetOrMap(type)).isTrue();
        assertThat(SORTER.sort(type, vector(textSet("p", "a"), textSet("z", "b"))))
        .isEqualTo(vector(textSet("a", "p"), textSet("b", "z")));
    }

    @Test
    void doesNotSortAVectorWithoutSetsOrMaps()
    {
        assertThat(SORTER.nestsSetOrMap(VectorType.getInstance(FloatType.instance, 3))).isFalse();
    }

    /**
     * @return a set of text values, serialized in the given order
     */
    private static ByteBuffer textSet(String... elements)
    {
        ByteBuffer[] values = new ByteBuffer[elements.length];
        int length = Integer.BYTES;
        for (int index = 0; index < elements.length; index++)
        {
            values[index] = UTF8Type.instance.decompose(elements[index]);
            length += Integer.BYTES + values[index].remaining();
        }
        ByteBuffer output = ByteBuffer.allocate(length).putInt(elements.length);
        for (ByteBuffer value : values)
        {
            output.putInt(value.remaining()).put(value);
        }
        output.flip();
        return output;
    }

    /**
     * @return a vector of elements of variable length, each preceded by its length as an unsigned variable-length
     * integer, which takes a single byte for lengths under 128
     */
    private static ByteBuffer vector(ByteBuffer... elements)
    {
        int length = 0;
        for (ByteBuffer element : elements)
        {
            length += 1 + element.remaining();
        }
        ByteBuffer output = ByteBuffer.allocate(length);
        for (ByteBuffer element : elements)
        {
            output.put((byte) element.remaining()).put(element.duplicate());
        }
        output.flip();
        return output;
    }
}
