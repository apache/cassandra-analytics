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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.math.BigInteger;

import org.apache.cassandra.bridge.TokenRange;

public final class SSTableTokenBounds implements Serializable
{
    private static final long serialVersionUID = 2026042501L;

    // Murmur3 partitioner only: tokens fit in a long. RandomPartitioner is not supported here.
    private static final BigInteger MIN_TOKEN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger MAX_TOKEN = BigInteger.valueOf(Long.MAX_VALUE);

    private final long firstToken;
    private final long lastToken;

    public SSTableTokenBounds(long firstToken, long lastToken)
    {
        this.firstToken = firstToken;
        this.lastToken = lastToken;
    }

    public long firstToken()
    {
        return firstToken;
    }

    public long lastToken()
    {
        return lastToken;
    }

    public boolean overlaps(TokenRange range)
    {
        BigInteger first = BigInteger.valueOf(firstToken);
        BigInteger last = BigInteger.valueOf(lastToken);
        // firstToken > lastToken represents a ring wrap-around (the same convention as
        // RangeUtils.calculateTokenRanges); model it as the two segments [first, MAX] and
        // [MIN, last] and report overlap if the query range hits either.
        if (firstToken > lastToken)
        {
            return range.isConnected(TokenRange.closed(first, MAX_TOKEN))
                || range.isConnected(TokenRange.closed(MIN_TOKEN, last));
        }
        return range.isConnected(TokenRange.closed(first, last));
    }
}
