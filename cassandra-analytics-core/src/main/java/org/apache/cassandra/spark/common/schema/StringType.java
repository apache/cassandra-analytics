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
import java.nio.charset.StandardCharsets;

public class StringType implements ColumnType<String>
{
    @Override
    public String parseColumn(ByteBuffer buffer, int length)
    {
        byte[] value = new byte[length];
        buffer.get(value, 0, length);
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer serialize(String value)
    {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
