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

package org.apache.cassandra.spark.bulkwriter.token;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * This is utility class for computing Cassandra token for a CQL row.
 * This is mainly written to partition Cassandra ring and split it between Spark executors.
 * And to make sure each executor is only talking to one Cassandra replica set.
 * This reduces number of SSTables that get created in Cassandra by the bulk writing job.
 * Fewer SSTables will result in lower read latencies and lower compaction overhead.
 */
@SuppressWarnings({"WeakerAccess", "rawtypes", "unchecked"})
public class TokenUtils implements Serializable
{
    private final String[] partitionKeyColumns;
    private final boolean isMurmur3Partitioner;
    private final ColumnType[] partitionKeyColumnTypes;

    public TokenUtils(List<String> partitionKeyColumns,
                      List<ColumnType<?>> partitionKeyColumnTypes,
                      boolean isMurmur3Partitioner)
    {
        this.partitionKeyColumns = partitionKeyColumns.toArray(new String[0]);
        this.partitionKeyColumnTypes = partitionKeyColumnTypes.toArray(new ColumnType[partitionKeyColumns.size()]);
        this.isMurmur3Partitioner = isMurmur3Partitioner;
    }

    private ByteBuffer getByteBuffer(Object columnValue, int partitionKeyColumnIdx)
    {
        ColumnType columnType = partitionKeyColumnTypes[partitionKeyColumnIdx];
        if (columnType == ColumnTypes.UUID)
        {
            return columnType.serialize(UUID.fromString((String) columnValue));
        }
        else
        {
            return columnType.serialize(columnValue);
        }
    }

    private BigInteger getToken(ByteBuffer[] byteBuffers)
    {
        return getToken(getCompositeKey(byteBuffers));
    }

    public BigInteger getToken(ByteBuffer key)
    {
        return isMurmur3Partitioner ? Partitioner.Murmur3Partitioner.hash(key) : Partitioner.RandomPartitioner.hash(key);
    }

    public ByteBuffer getCompositeKey(Object[] columnValues)
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[partitionKeyColumns.length];

        for (int column = 0; column < partitionKeyColumns.length; column++)
        {
            byteBuffers[column] = getByteBuffer(columnValues[column], column);
        }

        return getCompositeKey(byteBuffers);
    }

    public ByteBuffer getCompositeKey(ByteBuffer[] byteBuffers)
    {
        ByteBuffer key;
        if (byteBuffers.length == 1)
        {
            key = byteBuffers[0];
        }
        else
        {
            // Calculate length of the key
            int length = 0;
            for (ByteBuffer buffer : byteBuffers)
            {
                length += 2;
                length += buffer.remaining();
                length += 1;
            }

            // Add buffers one after another
            key = ByteBuffer.allocate(length);
            for (ByteBuffer buffer : byteBuffers)
            {
                key.putShort((short) buffer.remaining());
                key.put(buffer);
                key.put((byte) 0x00);
            }
            ((Buffer) key).flip();
        }

        return key;
    }

    /**
     * Calculate Cassandra token for CQL row
     *
     * @param columnValues Map of partitioner key columns to column value as Java type
     * @return Cassandra token
     */
    public BigInteger getToken(Map<String, Object> columnValues)
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[partitionKeyColumns.length];

        for (int column = 0; column < partitionKeyColumns.length; column++)
        {
            byteBuffers[column] = getByteBuffer(columnValues.get(partitionKeyColumns[column]), column);
        }

        return getToken(byteBuffers);
    }

    /**
     * Calculate Cassandra token for CQL row
     *
     * @param columnValues list of partitioner key columns in the same order as partition key
     * @return Cassandra token
     */
    public BigInteger getToken(List<Object> columnValues)
    {
        return getToken(columnValues.toArray());
    }

    /**
     * Calculate Cassandra token for CQL row
     *
     * @param columnValues array of partitioner key columns in the same order as partition key
     * @return Cassandra token
     */
    public BigInteger getToken(Object[] columnValues)
    {
        return getToken(getCompositeKey(columnValues));
    }
}
