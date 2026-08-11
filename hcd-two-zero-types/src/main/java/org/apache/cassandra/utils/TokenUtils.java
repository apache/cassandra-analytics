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

package org.apache.cassandra.utils;

import java.math.BigInteger;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;

// TODO(lantoniak): Remove after upgrade to Cassandra Analytics 0.4.0.
public class TokenUtils
{
    protected TokenUtils()
    {

    }

    public static BigInteger tokenToBigInteger(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return BigInteger.valueOf((long) token.getTokenValue());
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }

    public static Token bigIntegerToToken(final IPartitioner partitioner, final BigInteger token)
    {
        if (partitioner instanceof Murmur3Partitioner)
        {
            return new Murmur3Partitioner.LongToken(token.longValue());
        }
        if (partitioner instanceof RandomPartitioner)
        {
            return new RandomPartitioner.BigIntegerToken(token);
        }

        throw new UnsupportedOperationException("Unexpected partitioner type: " + partitioner.getClass().getName());
    }

    public static long tokenToLong(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return (long) token.getTokenValue();
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue().longValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }
}
