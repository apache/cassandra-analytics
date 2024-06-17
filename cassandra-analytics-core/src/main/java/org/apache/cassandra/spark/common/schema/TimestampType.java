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

package org.apache.cassandra.spark.common.schema;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * Provides functionality to convert {@link ByteBuffer}s to a {@link Date} column type and to serialize
 * {@link Date} types to {@link ByteBuffer}s
 */
public class TimestampType implements ColumnType<Date>
{
    private static final Date DEFAULT_DATE = new Date(0L);

    /**
     * Parses a value of this type from buffer. Value will be parsed from current position of the buffer. After
     * completion of the function, position will be moved by "length" bytes.
     *
     * @param buffer Buffer to parse column from
     * @param length Serialized value size in buffer is as big as length
     * @return value as {@link Date} type
     */
    @Override
    public Date parseColumn(ByteBuffer buffer, int length)
    {
        assert length == Long.SIZE / Byte.SIZE;
        return new Date(buffer.getLong());
    }

    /**
     * Default value, in case column value doesn't exist for CQL row
     *
     * @return the default value for the column type
     */
    @Override
    public Date getDefault()
    {
        return DEFAULT_DATE;
    }

    /**
     * Serialize {@link Date} into ByteBuffer and keeps the position at beginning of ByteBuffer
     *
     * @param value the value to serialize
     * @return A ByteBuffer containing the serialized value
     */
    @Override
    public ByteBuffer serialize(Date value)
    {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(0, value.getTime());
    }
}
