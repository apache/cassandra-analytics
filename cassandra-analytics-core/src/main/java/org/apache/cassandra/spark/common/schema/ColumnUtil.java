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

public final class ColumnUtil
{
    private ColumnUtil()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static int getLength(ByteBuffer buffer)
    {
        if (buffer.remaining() < 2)
        {
            return 0;
        }
        short length = buffer.getShort();
        if (buffer.remaining() < length)
        {
            throw new IllegalArgumentException("Invalid buffer length, expecting " + length + " bytes but got " + buffer.remaining());
        }
        return length;
    }

    public static <T> T getField(ByteBuffer buffer, ColumnType<T> column)
    {
        return column.parseColumn(buffer, getLength(buffer));
    }

    public static <T> T parseSingleColumn(ColumnType<T> column, ByteBuffer buffer)
    {
        return column.parseColumn(buffer, buffer.remaining());
    }
}
