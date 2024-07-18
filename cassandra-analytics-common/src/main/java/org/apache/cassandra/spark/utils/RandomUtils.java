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

package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import com.google.common.net.InetAddresses;

import org.apache.cassandra.spark.data.partitioner.Partitioner;

public final class RandomUtils
{
    private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    public static final int MIN_COLLECTION_SIZE = 16;

    public static final Random RANDOM = new Random();

    private RandomUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static byte randomByte()
    {
        return randomBytes(1)[0];
    }

    public static byte[] randomBytes(int size)
    {
        byte[] bytes = new byte[size];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static ByteBuffer randomByteBuffer(int length)
    {
        return ByteBuffer.wrap(randomBytes(length));
    }

    public static int randomPositiveInt(int bound)
    {
        return RANDOM.nextInt(bound - 1) + 1;
    }

    public static int nextInt(final int startInclusive, final int endExclusive)
    {
        if (endExclusive < startInclusive)
        {
            throw new IllegalArgumentException("Start value must be smaller or equal to end value.");
        }
        if (startInclusive < 0)
        {
            throw new IllegalArgumentException("Both range values must be non-negative.");
        }
        if (startInclusive == endExclusive)
        {
            return startInclusive;
        }

        return startInclusive + RANDOM.nextInt(endExclusive - startInclusive);
    }

    public static BigInteger randomBigInteger(Partitioner partitioner)
    {
        BigInteger range = partitioner.maxToken().subtract(partitioner.minToken());
        int length = partitioner.maxToken().bitLength();
        BigInteger result = new BigInteger(length, RandomUtils.RANDOM);
        if (result.compareTo(partitioner.minToken()) < 0)
        {
            result = result.add(partitioner.minToken());
        }
        if (result.compareTo(range) >= 0)
        {
            result = result.mod(range).add(partitioner.minToken());
        }
        return result;
    }

    /**
     * Returns a random Type 1 (time-based) UUID.
     * <p>
     * Since Java does not natively support creation of Type 1 (time-based) UUIDs, and in order to avoid introducing
     * a dependency on {@code org.apache.cassandra.utils.UUIDGen}, we obtain a Type 4 (random) UUID and "fix" it.
     *
     * @return a random Type 1 (time-based) UUID
     */
    public static UUID getRandomTimeUUIDForTesting()
    {
        UUID uuid = UUID.randomUUID();
        return new UUID(uuid.getMostSignificantBits() ^ 0x0000000000005000L,   // Change UUID version from 4 to 1
                        uuid.getLeastSignificantBits() | 0x0000010000000000L);  // Always set multicast bit to 1
    }

    @SuppressWarnings("UnstableApiUsage")
    public static InetAddress randomInet()
    {
        return InetAddresses.fromInteger(RANDOM.nextInt());
    }

    public static String randomAlphanumeric(int minLengthInclusive, int maxLengthExclusive)
    {
        return randomAlphanumeric(RandomUtils.nextInt(minLengthInclusive, maxLengthExclusive));
    }

    public static String randomAlphanumeric(int maxLength)
    {
        StringBuilder sb = new StringBuilder(maxLength);
        IntStream.rangeClosed(0, maxLength)
                 .map(i -> RANDOM.nextInt(ALPHANUMERIC.length()))
                 .mapToObj(ALPHANUMERIC::charAt)
                 .forEach(sb::append);
        return sb.toString();
    }
}
