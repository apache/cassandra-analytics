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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.spark.util.SerializableBuffer;
import org.jetbrains.annotations.NotNull;

public final class PartitionKeyFilter implements Serializable
{
    @NotNull
    private final SerializableBuffer key;
    @NotNull
    private final BigInteger token;

    private PartitionKeyFilter(@NotNull ByteBuffer filterKey, @NotNull BigInteger filterKeyTokenValue)
    {
        key = new SerializableBuffer(filterKey);
        token = filterKeyTokenValue;
    }

    @NotNull
    public TokenRange tokenRange()
    {
        return TokenRange.singleton(token);
    }

    @NotNull
    public ByteBuffer key()
    {
        return key.buffer();
    }

    @NotNull
    public BigInteger token()
    {
        return token;
    }

    public boolean overlaps(@NotNull TokenRange tokenRange)
    {
        return tokenRange.contains(token);
    }

    public boolean matches(@NotNull ByteBuffer key)
    {
        return key.compareTo(this.key.buffer()) == 0;
    }

    public boolean filter(@NotNull ByteBuffer key)
    {
        return this.key.buffer().compareTo(key) == 0;
    }

    @NotNull
    public static PartitionKeyFilter create(@NotNull ByteBuffer filterKey, @NotNull BigInteger filterKeyTokenValue)
    {
        Preconditions.checkArgument(filterKey.capacity() != 0);
        return new PartitionKeyFilter(filterKey, filterKeyTokenValue);
    }
}
