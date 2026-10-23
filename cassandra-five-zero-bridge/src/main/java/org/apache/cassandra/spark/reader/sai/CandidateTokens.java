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
package org.apache.cassandra.spark.reader.sai;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.utils.Preconditions;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable sorted set of exact partition tokens returned by SAI.
 *
 * <p>Murmur3 tokens are retained as primitive {@code long}s so a large SAI result does not create a per-token
 * object graph. Other partitioners use a {@link BigInteger} array as a generic fallback. SSTables take lightweight
 * {@link Slice}s over this shared set rather than allocating token ranges per candidate.</p>
 */
public final class CandidateTokens
{
    private static final int INITIAL_CAPACITY = 128;
    private static final CandidateTokens EMPTY = new CandidateTokens(new BigInteger[0]);
    private static final BigInteger LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    private final boolean murmur3;
    private final long[] murmur3Tokens;
    private final BigInteger[] genericTokens;

    private CandidateTokens(long[] murmur3Tokens)
    {
        this.murmur3 = true;
        this.murmur3Tokens = murmur3Tokens;
        this.genericTokens = null;
    }

    private CandidateTokens(BigInteger[] genericTokens)
    {
        this.murmur3 = false;
        this.murmur3Tokens = null;
        this.genericTokens = genericTokens;
    }

    @NotNull
    public static CandidateTokens empty()
    {
        return EMPTY;
    }

    @NotNull
    public static Builder builder(@NotNull IPartitioner partitioner)
    {
        return new Builder(partitioner instanceof Murmur3Partitioner);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int size()
    {
        return murmur3 ? murmur3Tokens.length : genericTokens.length;
    }

    public boolean contains(@NotNull BigInteger token)
    {
        if (murmur3)
        {
            if (token.compareTo(LONG_MIN_VALUE) < 0 || token.compareTo(LONG_MAX_VALUE) > 0)
            {
                return false;
            }
            return Arrays.binarySearch(murmur3Tokens, token.longValue()) >= 0;
        }
        return Arrays.binarySearch(genericTokens, token) >= 0;
    }

    /**
     * Returns the exact candidates in the inclusive token interval {@code [first, last]}.
     */
    @NotNull
    public Slice slice(@NotNull BigInteger first, @NotNull BigInteger last)
    {
        if (isEmpty() || first.compareTo(last) > 0)
        {
            return new Slice(this, 0, 0);
        }
        int from = lowerBound(first);
        int to = upperBound(last);
        return new Slice(this, from, Math.max(from, to));
    }

    private int lowerBound(BigInteger target)
    {
        int low = 0;
        int high = size();
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (compareAt(mid, target) < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }
        return low;
    }

    private int upperBound(BigInteger target)
    {
        int low = 0;
        int high = size();
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (compareAt(mid, target) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }
        return low;
    }

    private int compareAt(int index, BigInteger token)
    {
        if (murmur3)
        {
            if (token.compareTo(LONG_MIN_VALUE) < 0)
            {
                return 1;
            }
            if (token.compareTo(LONG_MAX_VALUE) > 0)
            {
                return -1;
            }
            return Long.compare(murmur3Tokens[index], token.longValue());
        }
        return genericTokens[index].compareTo(token);
    }

    private BigInteger tokenAt(int index)
    {
        return murmur3 ? BigInteger.valueOf(murmur3Tokens[index]) : genericTokens[index];
    }

    /**
     * View over a contiguous portion of the sorted candidate set.
     */
    public static final class Slice
    {
        private final CandidateTokens owner;
        private final int from;
        private final int to;

        private Slice(CandidateTokens owner, int from, int to)
        {
            this.owner = owner;
            this.from = from;
            this.to = to;
        }

        public boolean isEmpty()
        {
            return from == to;
        }

        public int size()
        {
            return to - from;
        }

        @NotNull
        public BigInteger first()
        {
            Preconditions.checkState(!isEmpty(), "Candidate token slice is empty");
            return owner.tokenAt(from);
        }

        @NotNull
        public BigInteger tokenAt(int index)
        {
            if (index < 0 || index >= size())
            {
                throw new IndexOutOfBoundsException();
            }
            return owner.tokenAt(from + index);
        }

        /**
         * Compares the candidate at {@code index} with the supplied token.
         */
        public int compareAt(int index, @NotNull BigInteger token)
        {
            if (index < 0 || index >= size())
            {
                throw new IndexOutOfBoundsException();
            }
            return owner.compareAt(from + index, token);
        }
    }

    public static final class Builder
    {
        private final boolean murmur3;
        private long[] murmur3Tokens;
        private BigInteger[] genericTokens;
        private int size;

        private Builder(boolean murmur3)
        {
            this.murmur3 = murmur3;
            if (murmur3)
            {
                murmur3Tokens = new long[INITIAL_CAPACITY];
            }
            else
            {
                genericTokens = new BigInteger[INITIAL_CAPACITY];
            }
        }

        public int size()
        {
            return size;
        }

        public void add(@NotNull BigInteger token)
        {
            if (murmur3)
            {
                long value = token.longValueExact();
                if (size > 0)
                {
                    int comparison = Long.compare(value, murmur3Tokens[size - 1]);
                    Preconditions.checkState(comparison >= 0, "SAI tokens must be supplied in sorted order");
                    if (comparison == 0)
                    {
                        return;
                    }
                }
                ensureCapacity();
                murmur3Tokens[size++] = value;
                return;
            }

            if (size > 0)
            {
                int comparison = token.compareTo(genericTokens[size - 1]);
                Preconditions.checkState(comparison >= 0, "SAI tokens must be supplied in sorted order");
                if (comparison == 0)
                {
                    return;
                }
            }
            ensureCapacity();
            genericTokens[size++] = token;
        }

        @NotNull
        public CandidateTokens build()
        {
            return murmur3
                   ? new CandidateTokens(Arrays.copyOf(murmur3Tokens, size))
                   : new CandidateTokens(Arrays.copyOf(genericTokens, size));
        }

        private void ensureCapacity()
        {
            if (murmur3 && size == murmur3Tokens.length)
            {
                murmur3Tokens = Arrays.copyOf(murmur3Tokens, size << 1);
            }
            else if (!murmur3 && size == genericTokens.length)
            {
                genericTokens = Arrays.copyOf(genericTokens, size << 1);
            }
        }
    }
}
