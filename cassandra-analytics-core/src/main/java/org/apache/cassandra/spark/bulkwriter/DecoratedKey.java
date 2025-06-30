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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.spark.bulkwriter.util.FastByteOperations;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import javax.annotation.Nonnull;

public class DecoratedKey implements Comparable<DecoratedKey>, Serializable
{
    @Nonnull private final BigInteger token;
    @Nonnull private final ByteBuffer key;

    DecoratedKey(@Nonnull BigInteger token, @Nonnull ByteBuffer key)
    {
        this.token = token;
        this.key = key;
    }

    @Nonnull
    public BigInteger getToken()
    {
        return token;
    }

    @Nonnull
    public ByteBuffer getKey()
    {
        return key;
    }

    @Override
    public int compareTo(@Nonnull DecoratedKey that)
    {
        int cmp = token.compareTo(that.token);
        if (cmp != 0)
        {
            return cmp;
        }
        return FastByteOperations.compareUnsigned(key, that.key);
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

        DecoratedKey that = (DecoratedKey) other;
        return this.token.equals(that.token) && this.key.equals(that.key);
    }

    @Override
    public int hashCode()
    {
        return token.hashCode() ^ key.hashCode();
    }

    @Override
    public String toString()
    {
        String keystring = ByteBufferUtils.toHexString(getKey());
        return "SBW-DecoratedKey(" + getToken() + ", " + keystring + ")";
    }
}
