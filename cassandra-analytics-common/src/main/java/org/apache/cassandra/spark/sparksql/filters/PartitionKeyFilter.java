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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.jetbrains.annotations.NotNull;

public final class PartitionKeyFilter implements Serializable
{
    @NotNull
    private BigInteger token;
    @NotNull
    private ByteBuffer filterKey;

    public PartitionKeyFilter(@NotNull ByteBuffer filterKey,
                              @NotNull BigInteger filterKeyTokenValue)
    {
        this.filterKey = filterKey.duplicate();
        this.token = filterKeyTokenValue;
    }

    @NotNull
    public TokenRange tokenRange()
    {
        return TokenRange.singleton(token);
    }

    @NotNull
    public ByteBuffer key()
    {
        return filterKey;
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
        return key.compareTo(key()) == 0;
    }

    public boolean filter(@NotNull ByteBuffer key)
    {
        return key().compareTo(key) == 0;
    }

    @NotNull
    public static PartitionKeyFilter create(@NotNull ByteBuffer filterKey, @NotNull BigInteger filterKeyTokenValue)
    {
        Preconditions.checkArgument(filterKey.capacity() != 0);
        return new PartitionKeyFilter(filterKey, filterKeyTokenValue);
    }

    private void readObject(final ObjectInputStream input) throws IOException
    {
        int keyLength = input.readInt();
        byte[] keyArray = new byte[keyLength];
        ByteBufferUtils.readFully(input, keyArray, keyLength);
        this.filterKey = ByteBuffer.wrap(keyArray);

        // read token
        int tokenLength = input.readByte();
        byte[] tokenArray = new byte[tokenLength];
        ByteBufferUtils.readFully(input, tokenArray, tokenLength);
        this.token = new BigInteger(tokenArray);
    }

    private void writeObject(final ObjectOutputStream output) throws IOException
    {
        ByteBuffer key = filterKey.duplicate();
        byte[] keyArray = new byte[key.remaining()];
        key.get(keyArray);
        output.writeInt(keyArray.length);
        output.write(keyArray);

        byte[] tokenArray = token.toByteArray();
        if (tokenArray.length == 0 || tokenArray.length >= 128)
        {
            // Murmur3 is max 8-bytes, RandomPartitioner is max 16-bytes
            throw new IllegalStateException("Invalid token length: " + tokenArray.length);
        }
        output.writeByte(tokenArray.length);
        output.write(tokenArray);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(filterKey, token);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        final PartitionKeyFilter that = (PartitionKeyFilter) other;
        return filterKey.equals(that.filterKey)
               && token.equals(that.token);
    }
}
