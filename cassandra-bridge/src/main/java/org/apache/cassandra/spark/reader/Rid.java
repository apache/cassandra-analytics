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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.utils.ByteBufferUtils;

/**
 * Rid - Row Identifier - contains the partition key, clustering keys and column name that uniquely identifies a row and column of data in Cassandra
 */
public class Rid
{
    private ByteBuffer partitionKey;
    private ByteBuffer columnName;
    private ByteBuffer value;
    private long timestamp;
    private BigInteger token;
    @VisibleForTesting
    boolean isNewPartition = false;

    // Partition Key Value

    public void setPartitionKeyCopy(ByteBuffer partitionKeyBytes, BigInteger token)
    {
        this.partitionKey = partitionKeyBytes;
        this.token = token;
        this.columnName = null;
        this.value = null;
        this.isNewPartition = true;
        this.timestamp = 0L;
    }

    public boolean isNewPartition()
    {
        if (isNewPartition)
        {
            isNewPartition = false;
            return true;
        }
        return false;
    }

    public ByteBuffer getPartitionKey()
    {
        return partitionKey;
    }

    public BigInteger getToken()
    {
        return token;
    }

    // Column Name (contains concatenated clustering keys and column name)

    public void setColumnNameCopy(ByteBuffer columnBytes)
    {
        this.columnName = columnBytes;
    }

    public ByteBuffer getColumnName()
    {
        return columnName;
    }

    // Value of Cell

    public ByteBuffer getValue()
    {
        return value;
    }

    public void setValueCopy(ByteBuffer value)
    {
        this.value = value;
    }

    // Timestamp

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    public String toString()
    {
        return ByteBufferUtils.toHexString(getPartitionKey()) + ":"
             + ByteBufferUtils.toHexString(getColumnName()) + ":"
             + ByteBufferUtils.toHexString(getValue());
    }
}
