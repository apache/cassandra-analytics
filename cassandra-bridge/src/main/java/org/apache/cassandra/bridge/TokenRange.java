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

package org.apache.cassandra.bridge;

import java.io.Serializable;
import java.math.BigInteger;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is a simple implementation of a range between two {@link BigInteger} tokens.
 * It allows us avoid dependency on Guava's {@link com.google.common.collect.Range}&lt;{@link BigInteger}&gt;
 * in the interface of Cassandre Bridge (which had brought us nothing but grief in the past).
 */
public final class TokenRange implements Serializable
{
    public static final long serialVersionUID = 42L;

    // NOTE: Internally, ranges are always of the closed-open kind
    @NotNull
    private final BigInteger lowerBound;
    @NotNull
    private final BigInteger upperBound;

    private TokenRange(@NotNull BigInteger lowerBound, @NotNull BigInteger upperBound)
    {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @NotNull
    public BigInteger lowerEndpoint()
    {
        return lowerBound;
    }

    @NotNull
    public BigInteger upperEndpoint()
    {
        return upperBound.subtract(BigInteger.ONE);
    }

    @NotNull
    public static TokenRange singleton(@NotNull BigInteger value)
    {
        return new TokenRange(value, value.add(BigInteger.ONE));
    }

    @NotNull
    public static TokenRange open(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        return new TokenRange(lower.add(BigInteger.ONE), upper);
    }

    @NotNull
    public static TokenRange closed(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        return new TokenRange(lower, upper.add(BigInteger.ONE));
    }

    @NotNull
    public static TokenRange closedOpen(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        return new TokenRange(lower, upper);
    }

    @NotNull
    public static TokenRange openClosed(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        return new TokenRange(lower.add(BigInteger.ONE), upper.add(BigInteger.ONE));
    }

    @NotNull
    public static TokenRange merge(@NotNull TokenRange first, @NotNull TokenRange second)
    {
        return first.span(second);
    }

    public boolean isEmpty()
    {
        return lowerBound.compareTo(upperBound) >= 0;
    }

    public BigInteger size()
    {
        return upperBound.max(lowerBound).subtract(lowerBound);
    }

    public boolean contains(@NotNull BigInteger value)
    {
        return lowerBound.compareTo(value) <= 0 && value.compareTo(upperBound) < 0;
    }

    public boolean encloses(@NotNull TokenRange other)
    {
        return lowerBound.compareTo(other.lowerBound) <= 0 && other.upperBound.compareTo(upperBound) <= 0;
    }

    public boolean isConnected(@NotNull TokenRange other)
    {
        return lowerBound.compareTo(other.upperBound) < 0 && other.lowerBound.compareTo(upperBound) < 0;
    }

    @NotNull
    public TokenRange intersection(@NotNull TokenRange other)
    {
        return new TokenRange(lowerBound.max(other.lowerBound), upperBound.min(other.upperBound));
    }

    @NotNull
    public TokenRange span(@NotNull TokenRange other)
    {
        return new TokenRange(lowerBound.min(other.lowerBound), upperBound.max(other.upperBound));
    }

    @Override
    public boolean equals(@Nullable Object other)
    {
        return other instanceof TokenRange
            && this.lowerBound.equals(((TokenRange) other).lowerBound)
            && this.upperBound.equals(((TokenRange) other).upperBound);
    }

    @Override
    public int hashCode()
    {
        return lowerBound.hashCode() ^ upperBound.hashCode();
    }

    @Override
    @NotNull
    public String toString()
    {
        return lowerBound + "‥" + upperBound;
    }
}
