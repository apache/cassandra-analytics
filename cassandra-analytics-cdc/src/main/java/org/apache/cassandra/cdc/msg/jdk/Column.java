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

package org.apache.cassandra.cdc.msg.jdk;

import java.nio.ByteBuffer;

import org.apache.cassandra.spark.data.CqlField;
import org.jetbrains.annotations.Nullable;

public class Column
{
    private final String name;
    private final CqlField.CqlType type;
    @Nullable
    private final Object value;

    public Column(String name, CqlField.CqlType type, @Nullable ByteBuffer buf)
    {
        this(name, type, buf == null ? null : type.deserializeToJavaType(buf));
    }

    public Column(String name, CqlField.CqlType type, @Nullable Object value)
    {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String name()
    {
        return name;
    }

    public CqlField.CqlType type()
    {
        return type;
    }

    @Nullable
    public Object value()
    {
        return value;
    }

    public <T> T getAs(Class<T> tClass)
    {
        return value == null ? null : tClass.cast(value);
    }

    @Override
    public String toString()
    {
        return "\"" + name + "\": " + (value == null ? "null" : "\"" + value + "\"");
    }
}
