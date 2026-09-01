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
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import com.google.common.net.InetAddresses;

import org.apache.cassandra.spark.data.partitioner.Partitioner;

public final class RandomUtils
{
    public static final int MIN_COLLECTION_SIZE = 16;

    private RandomUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static byte randomByte(Random random)
    {
        return randomBytes(random, 1)[0];
    }

    public static byte[] randomBytes(Random random, int size)
    {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    public static ByteBuffer randomByteBuffer(Random random, int length)
    {
        return ByteBuffer.wrap(randomBytes(random, length));
    }

    public static int randomPositiveInt(Random random, int bound)
    {
        return random.nextInt(bound - 1) + 1;
    }

    public static int nextInt(Random random, int startInclusive, int endExclusive)
    {
        if (endExclusive <= startInclusive)
        {
            throw new IllegalArgumentException("Start value must be less than the end value.");
        }
        if (startInclusive < 0)
        {
            throw new IllegalArgumentException("Both range values must be non-negative.");
        }

        return startInclusive + random.nextInt(endExclusive - startInclusive);
    }

    public static BigInteger randomBigInteger(Random random, Partitioner partitioner)
    {
        BigInteger range = partitioner.maxToken().subtract(partitioner.minToken());
        int length = partitioner.maxToken().bitLength();
        BigInteger result = new BigInteger(length, random);
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
     * Returns a random Type 4 (random) UUID, built from the given {@link Random} so it is reproducible from a
     * fixed seed - unlike {@link UUID#randomUUID()}, which always draws from the JVM-wide {@link java.security.SecureRandom}.
     */
    public static UUID randomUuid(Random random)
    {
        byte[] randomBytes = new byte[16];
        random.nextBytes(randomBytes);
        randomBytes[6] &= 0x0f;  /* clear version        */
        randomBytes[6] |= 0x40;  /* set to version 4     */
        randomBytes[8] &= 0x3f;  /* clear variant        */
        randomBytes[8] |= (byte) 0x80;  /* set to IETF variant  */
        long mostSigBits = 0;
        for (int i = 0; i < 8; i++)
        {
            mostSigBits = (mostSigBits << 8) | (randomBytes[i] & 0xff);
        }
        long leastSigBits = 0;
        for (int i = 8; i < 16; i++)
        {
            leastSigBits = (leastSigBits << 8) | (randomBytes[i] & 0xff);
        }
        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Returns a random Type 1 (time-based) UUID.
     * <p>
     * Since Java does not natively support creation of Type 1 (time-based) UUIDs, and in order to avoid introducing
     * a dependency on {@code org.apache.cassandra.utils.UUIDGen}, we obtain a Type 4 (random) UUID and "fix" it.
     *
     * @return a random Type 1 (time-based) UUID
     */
    public static UUID getRandomTimeUUIDForTesting(Random random)
    {
        UUID uuid = randomUuid(random);
        return new UUID(uuid.getMostSignificantBits() ^ 0x0000000000005000L,   // Change UUID version from 4 to 1
                        uuid.getLeastSignificantBits() | 0x0000010000000000L);  // Always set multicast bit to 1
    }

    @SuppressWarnings("UnstableApiUsage")
    public static InetAddress randomInet(Random random)
    {
        return InetAddresses.fromInteger(random.nextInt());
    }

    public static String randomAlphanumeric(Random random, int minLengthInclusive, int maxLengthExclusive)
    {
        return randomAlphanumeric(random, RandomUtils.nextInt(random, minLengthInclusive, maxLengthExclusive));
    }

    public static String randomAlphanumeric(Random random, Set<String> alreadyExist)
    {
        return randomAlphanumeric(random, alreadyExist, 32);
    }

    public static String randomAlphanumeric(Random random, Set<String> alreadyExist, int length)
    {
        String str = randomAlphanumeric(random, length);
        while (alreadyExist.contains(str))
        {
            str = randomAlphanumeric(random, length);
        }
        return str;
    }

    public static String randomAlphanumeric(Random random)
    {
        return randomAlphanumeric(random, 32);
    }

    public static String randomAlphanumeric(Random random, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        IntStream.range(0, length)
                 .mapToObj(i -> randomAsciiAlphanumeric(random))
                 .forEach(sb::append);
        return sb.toString();
    }

    /**
     * @return random ascii character between 0x30...0x39 for numbers and 0x41...0x5A for uppercase letters
     */
    public static char randomAsciiAlphanumeric(Random random)
    {
        int c = random.nextInt(36);
        if (c < 10)
        {
            // return ascii number
            return (char) (c + 48);
        }
        // return ascii uppercase character
        return (char) ((c - 10) + 65);
    }
}
