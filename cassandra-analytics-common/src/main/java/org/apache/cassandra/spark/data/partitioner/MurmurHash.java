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

import java.nio.ByteBuffer;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based lookup.
 * See http://murmurhash.googlepages.com/ for more details.
 *
 * hash32() and hash64() are MurmurHash 2.0.
 * hash3_x64_128() is MurmurHash 3.0.
 *
 * The C version of MurmurHash 2.0 found at that site was ported to Java by Andrzej Bialecki.
 */
public final class MurmurHash
{
    private MurmurHash()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static long block(ByteBuffer key, int offset, int index)
    {
        int blockOffset = offset + (index << 3);
        return ((long) key.get(blockOffset + 0) & 0xFF)
            + (((long) key.get(blockOffset + 1) & 0xFF) <<  8)
            + (((long) key.get(blockOffset + 2) & 0xFF) << 16)
            + (((long) key.get(blockOffset + 3) & 0xFF) << 24)
            + (((long) key.get(blockOffset + 4) & 0xFF) << 32)
            + (((long) key.get(blockOffset + 5) & 0xFF) << 40)
            + (((long) key.get(blockOffset + 6) & 0xFF) << 48)
            + (((long) key.get(blockOffset + 7) & 0xFF) << 56);
    }

    private static long rotate(long value, int shift)
    {
        return (value << shift) | (value >>> (64 - shift));
    }

    private static long mix(long value)
    {
        value ^= value >>> 33;
        value *= 0xFF51AFD7ED558CCDL;
        value ^= value >>> 33;
        value *= 0xC4CEB9FE1A85EC53L;
        value ^= value >>> 33;

        return value;
    }

    public static long[] hash(ByteBuffer key, int offset, int length, long seed)
    {
        int nblocks = length >> 4;  // Process as 128-bit blocks

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87C37B91114253D5L;
        long c2 = 0x4CF5AD432745937FL;

        // Body

        for (int block = 0; block < nblocks; block++)
        {
            long k1 = block(key, offset, block * 2 + 0);
            long k2 = block(key, offset, block * 2 + 1);

            k1 *= c1;
            k1 = rotate(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotate(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52DCE729;

            k2 *= c2;
            k2 = rotate(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotate(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495AB5;
        }

        // Tail

        // Advance offset to the unprocessed tail of the data
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15)
        {
            case 15: k2 ^= ((long) key.get(offset + 14)) << 48;
            case 14: k2 ^= ((long) key.get(offset + 13)) << 40;
            case 13: k2 ^= ((long) key.get(offset + 12)) << 32;
            case 12: k2 ^= ((long) key.get(offset + 11)) << 24;
            case 11: k2 ^= ((long) key.get(offset + 10)) << 16;
            case 10: k2 ^= ((long) key.get(offset +  9)) <<  8;
            case  9: k2 ^= ((long) key.get(offset +  8));
                     k2 *= c2;
                     k2  = rotate(k2, 33);
                     k2 *= c1;
                     h2 ^= k2;

            case  8: k1 ^= ((long) key.get(offset + 7)) << 56;
            case  7: k1 ^= ((long) key.get(offset + 6)) << 48;
            case  6: k1 ^= ((long) key.get(offset + 5)) << 40;
            case  5: k1 ^= ((long) key.get(offset + 4)) << 32;
            case  4: k1 ^= ((long) key.get(offset + 3)) << 24;
            case  3: k1 ^= ((long) key.get(offset + 2)) << 16;
            case  2: k1 ^= ((long) key.get(offset + 1))  << 8;
            case  1: k1 ^= ((long) key.get(offset));
                     k1 *= c1;
                     k1  = rotate(k1, 31);
                     k1 *= c2;
                     h1 ^= k1;

            default:  // Do nothing
        }

        // Finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = mix(h1);
        h2 = mix(h2);

        h1 += h2;
        h2 += h1;

        return new long[]{h1, h2};
    }

    protected static long invRShiftXor(long value, int shift)
    {
        long output = 0;
        long i = 0;
        while (i * shift < 64)
        {
            long c = (0xffffffffffffffffL << (64 - shift)) >>> (shift * i);
            long partOutput = value & c;
            value ^= partOutput >>> shift;
            output |= partOutput;
            i += 1;
        }
        return output;
    }

    protected static long invRotl64(long v, int n)
    {
        return ((v >>> n) | (v << (64 - n)));
    }

    protected static long invFmix(long k)
    {
        k = invRShiftXor(k, 33);
        k *= 0x9cb4b2f8129337dbL;
        k = invRShiftXor(k, 33);
        k *= 0x4f74430c22a54005L;
        k = invRShiftXor(k, 33);
        return k;
    }

    public static long[] inv_hash3_x64_128(long[] result)
    {
        long c1 = 0xa98409e882ce4d7dL;
        long c2 = 0xa81e14edd9de2c7fL;

        long k1 = 0;
        long k2 = 0;
        long h1 = result[0];
        long h2 = result[1];

        //----------
        // reverse finalization
        h2 -= h1;
        h1 -= h2;

        h1 = invFmix(h1);
        h2 = invFmix(h2);

        h2 -= h1;
        h1 -= h2;

        h1 ^= 16;
        h2 ^= 16;

        //----------
        // reverse body
        h2 -= 0x38495ab5;
        h2 *= 0xcccccccccccccccdL;
        h2 -= h1;
        h2 = invRotl64(h2, 31);
        k2 = h2;
        h2 = 0;

        k2 *= c1;
        k2 = invRotl64(k2, 33);
        k2 *= c2;

        h1 -= 0x52dce729;
        h1 *= 0xcccccccccccccccdL;
        //h1 -= h2;
        h1 = invRotl64(h1, 27);

        k1 = h1;

        k1 *= c2;
        k1 = invRotl64(k1, 31);
        k1 *= c1;

        // note that while this works for body block reversing the tail reverse requires `invTailReverse`
        k1 = Long.reverseBytes(k1);
        k2 = Long.reverseBytes(k2);

        return new long[] {k1, k2};
    }
}
