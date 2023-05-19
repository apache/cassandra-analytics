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

package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.function.Function;

/**
 * Encapsulates partitioner ranges and the ability to compute the hash for a given key. Supports:
 *
 * <ul>
 *     <li>RandomPartitioner
 *     <li>Murmur3Partitioner
 * </ul>
 */
public enum Partitioner
{
    RandomPartitioner(BigInteger.ZERO, BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE), key -> {
        // Random partitioner hash using MD5
        MessageDigest md;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (Exception exception)
        {
            throw new RuntimeException(exception);
        }
        md.reset();
        md.update(key);
        return new BigInteger(md.digest()).abs();
    }),
    Murmur3Partitioner(BigInteger.valueOf(2).pow(63).negate(), BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE), key -> {
        // Otherwise Murmur3 hash
        long value = MurmurHash.hash(key, key.position(), key.remaining(), 0)[0];
        value = value == Long.MIN_VALUE ? Long.MAX_VALUE : value;  // Normalize
        return BigInteger.valueOf(value);
    });

    /**
     * Returns the {@link Partitioner} type based on the class name. It matches the fully qualified class name, or
     * class name.
     *
     * @param partitionerClass the class name or the fully qualified class name
     * @return the {@link Partitioner} type based on the class name
     */
    public static Partitioner from(String partitionerClass)
    {
        switch (partitionerClass)
        {
            case "org.apache.cassandra.dht.Murmur3Partitioner":
            case "Murmur3Partitioner":
                return Partitioner.Murmur3Partitioner;

            case "org.apache.cassandra.dht.RandomPartitioner":
            case "RandomPartitioner":
                return Partitioner.RandomPartitioner;

            default:
                throw new UnsupportedOperationException("Unexpected partitioner: " + partitionerClass);
        }
    }

    private final BigInteger minToken;
    private final BigInteger maxToken;
    private final Function<ByteBuffer, BigInteger> hash;

    Partitioner(BigInteger minToken, BigInteger maxToken, Function<ByteBuffer, BigInteger> hash)
    {
        this.minToken = minToken;
        this.maxToken = maxToken;
        this.hash = hash;
    }

    public BigInteger minToken()
    {
        return minToken;
    }

    public BigInteger maxToken()
    {
        return maxToken;
    }

    public BigInteger hash(ByteBuffer key)
    {
        return hash.apply(key);
    }

    @Override
    public String toString()
    {
        return "org.apache.cassandra.dht." + super.toString();
    }
}
