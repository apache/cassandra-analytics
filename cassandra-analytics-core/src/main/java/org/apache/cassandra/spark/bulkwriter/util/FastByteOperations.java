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

package org.apache.cassandra.spark.bulkwriter.util;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;

import sun.misc.Unsafe;  // CHECKSTYLE IGNORE: Used with care to speed byte operations up

/**
 * Utility code to do optimized byte-array comparison.
 * This is borrowed and slightly modified from Guava's {@link UnsignedBytes}
 * class to be able to compare arrays that start at non-zero offsets.
 * NOTE: This was lifted from Cassandra 2.1 and reduced to the minimum number of
 * methods necessary for the Spark Bulk Writer. Tests in Cassandra, not ported here
 * as more copy/paste didn't seem to be valuable. Also tested via DecoratedKeyTest.
 */
public final class FastByteOperations
{
    private FastByteOperations()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Lexicographically compare two byte arrays
     *
     * @param buffer1 the first bytebuffer
     * @param buffer2 the second byteburrer
     * @return standard java comparison return value (-1 for &lt;, 0 for ==, and 1 for &gt;)
     */
    public static int compareUnsigned(ByteBuffer buffer1, ByteBuffer buffer2)
    {
        return BestHolder.BEST.compare(buffer1, buffer2);
    }

    public interface ByteOperations
    {
        int compare(ByteBuffer buffer1, ByteBuffer buffer2);
    }

    /**
     * Provides a lexicographical comparer implementation; either a Java
     * implementation or a faster implementation based on {@link Unsafe}.
     *
     * Uses reflection to gracefully fall back to the Java implementation if
     * {@code Unsafe} isn't available.
     */
    private static class BestHolder
    {
        static final String UNSAFE_COMPARER_NAME = FastByteOperations.class.getName() + "$UnsafeOperations";
        static final ByteOperations BEST = getBest();

        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java implementation if unable to do so
         */
        static ByteOperations getBest()
        {
            String arch = System.getProperty("os.arch");
            boolean unaligned = arch.equals("i386") || arch.equals("x86") || arch.equals("x86_64") || arch.equals("amd64");
            if (!unaligned)
            {
                return new PureJavaOperations();
            }
            try
            {
                Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

                // Yes, UnsafeComparer does implement Comparer<byte[]>
                @SuppressWarnings("unchecked")
                ByteOperations comparer = (ByteOperations) theClass.getConstructor().newInstance();
                return comparer;
            }
            catch (Throwable throwable)
            {
                // Ensure we really catch *everything*
                return new PureJavaOperations();
            }
        }

    }

    public static final class UnsafeOperations implements ByteOperations
    {
        static final Unsafe UNSAFE;
        /**
         * The offset to the first element in a byte array
         */
        static final long BYTE_ARRAY_BASE_OFFSET;
        static final long DIRECT_BUFFER_ADDRESS_OFFSET;

        static
        {
            UNSAFE = (Unsafe) AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try
                {
                    Field field = Unsafe.class.getDeclaredField("UNSAFE");
                    field.setAccessible(true);
                    return field.get(null);
                }
                catch (NoSuchFieldException | IllegalAccessException exception)
                {
                    // It doesn't matter what we throw; it's swallowed in getBest()
                    throw new Error();
                }
            });

            try
            {
                BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
                DIRECT_BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            }
            catch (Exception exception)
            {
                throw new AssertionError(exception);
            }

            // Sanity check - this should never fail
            if (UNSAFE.arrayIndexScale(byte[].class) != 1)
            {
                throw new AssertionError();
            }
        }

        static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

        public int compare(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            return compareTo(buffer1, buffer2);
        }

        static int compareTo(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            Object object1;
            long offset1;
            int length1;
            if (buffer1.hasArray())
            {
                object1 = buffer1.array();
                offset1 = BYTE_ARRAY_BASE_OFFSET + buffer1.arrayOffset();
            }
            else
            {
                object1 = null;
                offset1 = UNSAFE.getLong(buffer1, DIRECT_BUFFER_ADDRESS_OFFSET);
            }
            offset1 += buffer1.position();
            length1 = buffer1.remaining();
            return compareTo(object1, offset1, length1, buffer2);
        }

        static int compareTo(Object buffer1, long offset1, int length1, ByteBuffer buffer2)
        {
            Object object2;
            long offset2;

            int position = buffer2.position();
            int limit = buffer2.limit();
            if (buffer2.hasArray())
            {
                object2 = buffer2.array();
                offset2 = BYTE_ARRAY_BASE_OFFSET + buffer2.arrayOffset();
            }
            else
            {
                object2 = null;
                offset2 = UNSAFE.getLong(buffer2, DIRECT_BUFFER_ADDRESS_OFFSET);
            }
            int length2 = limit - position;
            offset2 += position;

            return compareTo(buffer1, offset1, length1, object2, offset2, length2);
        }

        /**
         * Lexicographically compare two arrays
         *
         * @param buffer1 left operand: a byte[] or null
         * @param buffer2 right operand: a byte[] or null
         * @param offset1 Where to start comparing in the left buffer
         *                (pure memory address if buffer1 is null, or relative otherwise)
         * @param offset2 Where to start comparing in the right buffer
         *                (pure memory address if buffer1 is null, or relative otherwise)
         * @param length1 How much to compare from the left buffer
         * @param length2 How much to compare from the right buffer
         * @return 0 if equal, < 0 if left is less than right, etc.
         */
        static int compareTo(Object buffer1, long offset1, int length1, Object buffer2, long offset2, int length2)
        {
            int minLength = Math.min(length1, length2);

            /*
             * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
             * time is no slower than comparing 4 bytes at a time even on 32-bit.
             * On the other hand, it is substantially faster on 64-bit.
             */
            int wordComparisons = minLength & ~7;
            for (int index = 0; index < wordComparisons; index += Longs.BYTES)
            {
                long long1 = UNSAFE.getLong(buffer1, offset1 + index);
                long long2 = UNSAFE.getLong(buffer2, offset2 + index);

                if (long1 != long2)
                {
                    return BIG_ENDIAN ? UnsignedLongs.compare(long1, long2)
                                      : UnsignedLongs.compare(Long.reverseBytes(long1), Long.reverseBytes(long2));
                }
            }

            for (int index = wordComparisons; index < minLength; index++)
            {
                int byte1 = UNSAFE.getByte(buffer1, offset1 + index) & 0xFF;
                int byte2 = UNSAFE.getByte(buffer2, offset2 + index) & 0xFF;
                if (byte1 != byte2)
                {
                    return byte1 - byte2;
                }
            }

            return length1 - length2;
        }
    }

    public static final class PureJavaOperations implements ByteOperations
    {
        public int compare(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            int end1 = buffer1.limit();
            int end2 = buffer2.limit();
            for (int index1 = buffer1.position(), index2 = buffer2.position(); index1 < end1 && index2 < end2; index1++, index2++)
            {
                int byte1 = buffer1.get(index1) & 0xFF;
                int byte2 = buffer2.get(index2) & 0xFF;
                if (byte1 != byte2)
                {
                    return byte1 - byte2;
                }
            }
            return buffer1.remaining() - buffer2.remaining();
        }
    }
}
