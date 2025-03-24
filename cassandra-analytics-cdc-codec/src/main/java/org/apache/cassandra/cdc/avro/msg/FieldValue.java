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

package org.apache.cassandra.cdc.avro.msg;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.cdc.msg.Value;
import org.jetbrains.annotations.NotNull;

public final class FieldValue
{
    public final Value value;
    private final ByteBuffer byteBuffer;

    public FieldValue(@NotNull Value value)
    {
        this.value = value;
        this.byteBuffer = value.getValue();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value.columnName, value.columnType, byteBuffer);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        // Comparing with FieldValue by comparing with the underlying ValueWithMetadata
        if (obj instanceof FieldValue)
        {
            // extract the ValueWithMetadata
            obj = ((FieldValue) obj).value;
        }

        if (obj instanceof Value)
        {
            Value that = (Value) obj;
            return Objects.equals(this.value.columnName, that.columnName)
                   && Objects.equals(this.value.columnType, that.columnType)
                   && Objects.equals(this.byteBuffer, that.getValue());
        }
        else
        {
            return false;
        }
    }
}
