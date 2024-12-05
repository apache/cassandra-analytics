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

package org.apache.cassandra.cdc.msg;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.jetbrains.annotations.Nullable;

public class Value
{
    public final String keyspace;
    public final String columnName;
    public final String columnType;
    @Nullable
    private final ByteBuffer value;

    public Value(String keyspace, String columnName, String columnType, @Nullable ByteBuffer value)
    {
        this.keyspace = keyspace;
        this.columnName = columnName;
        this.columnType = columnType;
        this.value = value;
    }

    /**
     * @return the value as byte array
     */
    @Nullable
    public byte[] getBytes()
    {
        // if bb is null, we should return null; Null means deletion
        return value == null ? null : ByteBufferUtils.getArray(value);
    }

    public CqlField.CqlType getCqlType(Function<String, CqlField.CqlType> typeMapping)
    {
        // The possible complex values of columnType
        // frozen<list<int>>
        // list<frozen<set<int>>>
        // frozen<map<int, int>>
        // map<int, frozen<my_udt>>
        return typeMapping.apply(columnType);
    }

    /**
     * @return the duplicated {@link ByteBuffer} of the value
     */
    @Nullable
    public ByteBuffer getValue()
    {
        return value == null ? null : value.duplicate();
    }
}
