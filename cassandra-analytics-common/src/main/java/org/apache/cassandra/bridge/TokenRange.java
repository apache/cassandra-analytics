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
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * This is a simple implementation of a range between two {@link BigInteger} tokens.
 * A range is responsible for the tokens between {@code (lower, upper]}, i.e. open-closed.
 *
 * It allows us to avoid dependency on Guava's {@link com.google.common.collect.Range}&lt;{@link BigInteger}&gt;
 * in the interface of Cassandre Bridge (which had brought us nothing but grief in the past).
 */
public final class TokenRange implements Serializable
{
    private static final long serialVersionUID = 6367860484115802919L;

    // NOTE: ranges are always of the open-closed kind in Cassandra, see org.apache.cassandra.dht.Range
    @NotNull
    private final BigInteger lowerBound;
    private final BigInteger firstEnclosedValue;
    @NotNull
    private final BigInteger upperBound;

    private TokenRange(@NotNull BigInteger lowerBound, @NotNull BigInteger upperBound)
    {
        if (lowerBound.compareTo(upperBound) > 0)
        {
            throw new IllegalArgumentException("The lower bound cannot be greater than the upper bound to compose a TokenRange. " +
                                               "lowerBound: " + lowerBound + "; upperBound: " + upperBound);
        }
        this.lowerBound = lowerBound;
        this.firstEnclosedValue = lowerBound.add(BigInteger.ONE);
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
        return upperBound;
    }

    @NotNull
    public BigInteger firstEnclosedValue()
    {
        return firstEnclosedValue;
    }

    @NotNull
    public static TokenRange singleton(@NotNull BigInteger value)
    {
        // express the single token range in the form of open-closed range
        return new TokenRange(value.subtract(BigInteger.ONE), value);
    }

    @NotNull
    public static TokenRange closed(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        // expressed [lower, upper] in the form of open-closed, (lower - 1, upper]
        return new TokenRange(lower.subtract(BigInteger.ONE), upper);
    }

    @NotNull
    public static TokenRange openClosed(@NotNull BigInteger lower, @NotNull BigInteger upper)
    {
        return new TokenRange(lower, upper);
    }

    @NotNull
    public static TokenRange merge(@NotNull TokenRange first, @NotNull TokenRange second)
    {
        return first.span(second);
    }

    public boolean isEmpty()
    {
        return upperBound.compareTo(lowerBound) == 0;
    }

    public BigInteger size()
    {
        return upperBound.subtract(lowerBound);
    }

    public boolean contains(@NotNull BigInteger value)
    {
        return value.compareTo(lowerBound) > 0 && value.compareTo(upperBound) <= 0;
    }

    public boolean encloses(@NotNull TokenRange other)
    {
        return other.lowerBound.compareTo(lowerBound) >= 0 && other.upperBound.compareTo(upperBound) <= 0;
    }

    public boolean isConnected(@NotNull TokenRange other)
    {
        return lowerBound.compareTo(other.upperBound) < 0 && upperBound.compareTo(other.lowerBound) > 0;
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

    /**
     * Defines the same equals behavior as in the Guava's Range.
     * For ranges {@code (0, 3)} and {@code [1, 2]}, althrough the values sets of the ranges are exactly the same,
     * they are not considered that equals to each other
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        TokenRange that = (TokenRange) o;
        return Objects.equals(lowerBound, that.lowerBound) && Objects.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowerBound, upperBound);
    }

    @Override
    @NotNull
    public String toString()
    {
        return "(" + lowerBound + ", " + upperBound + ']';
    }
}
