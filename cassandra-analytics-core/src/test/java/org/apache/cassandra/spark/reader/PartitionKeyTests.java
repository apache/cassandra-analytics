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

package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;

import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionKeyTests
{
    @Test
    public void testSinglePartitionKey()
    {
        runTest((partitioner, directory, bridge) -> {
            List<CqlField> singlePartitionKey = Collections.singletonList(new CqlField(true, false, false, "a", bridge.aInt(), 0));
            CqlTable table = mock(CqlTable.class);
            when(table.partitionKeys()).thenReturn(singlePartitionKey);

            ByteBuffer key = ByteBuffer.wrap(new byte[]{0, 0, 0, 1});
            AbstractMap.SimpleEntry<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(table, partitioner, ImmutableList.of("1"));

            assertEquals(key, actualKey.getKey());
            assertEquals(bridge.hash(partitioner, key), actualKey.getValue());
            assertNotEquals(bridge.aInt().serialize(2), actualKey.getKey());
        });
    }

    @Test
    public void testMultiplePartitionKey()
    {
        runTest((partitioner, directory, bridge) -> {
            List<CqlField> multiplePartitionKey = Arrays.asList(new CqlField(true, false, false, "a", bridge.aInt(), 0),
                                                                new CqlField(true, false, false, "b", bridge.bigint(), 1),
                                                                new CqlField(true, false, false, "c", bridge.text(), 2));
            CqlTable table = mock(CqlTable.class);
            when(table.partitionKeys()).thenReturn(multiplePartitionKey);

            ByteBuffer key = ByteBuffer.wrap(new byte[]{0, 4, 0, 0, 0, 3, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 3, 120, 121, 122, 0});
            AbstractMap.SimpleEntry<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(table, partitioner, ImmutableList.of("3", "1", "xyz"));

            assertEquals(key, actualKey.getKey());
            assertEquals(bridge.hash(partitioner, key), actualKey.getValue());
        });
    }
}
