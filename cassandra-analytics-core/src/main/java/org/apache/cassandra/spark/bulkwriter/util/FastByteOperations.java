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
            // Note that s390x, aarch64, & ppc64le architectures are not officially supported and adding them here is only done out
            // of convenience for those that want to run C* on these architectures at their own risk (see #11214, #13326, & #13615)
            boolean unaligned = arch.equals("i386")
                                || arch.equals("x86")
                                || arch.equals("x86_64")
                                || arch.equals("amd64")
                                || arch.equals("s390x")
                                || arch.equals("aarch64")
                                || arch.equals("ppc64le");
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

    @SuppressWarnings("unused") // used via reflection
    public static final class UnsafeOperations implements ByteOperations
    {
        static final Unsafe theUnsafe;
        /**
         * The offset to the first element in a byte array.
         */
        static final long BYTE_ARRAY_BASE_OFFSET;
        static final long DIRECT_BUFFER_ADDRESS_OFFSET;

        static
        {
            theUnsafe = (Unsafe) AccessController.doPrivileged(
                      new PrivilegedAction<Object>()
                      {
                          @Override
                          public Object run()
                          {
                              try
                              {
                                  Field f = Unsafe.class.getDeclaredField("theUnsafe");
                                  f.setAccessible(true);
                                  return f.get(null);
                              }
                              catch (NoSuchFieldException e)
                              {
                                  // It doesn't matter what we throw;
                                  // it's swallowed in getBest().
                                  throw new Error();
                              }
                              catch (IllegalAccessException e)
                              {
                                  throw new Error();
                              }
                          }
                      });

            try
            {
                BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
                DIRECT_BUFFER_ADDRESS_OFFSET = theUnsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }

            // sanity check - this should never fail
            if (theUnsafe.arrayIndexScale(byte[].class) != 1)
            {
                throw new AssertionError();
            }
        }

        static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

        public int compare(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            return compareTo(buffer1, buffer2);
        }

        public static int compareTo(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            Object obj1;
            long offset1;
            int length1;
            if (buffer1.hasArray())
            {
                obj1 = buffer1.array();
                offset1 = BYTE_ARRAY_BASE_OFFSET + buffer1.arrayOffset();
            }
            else
            {
                obj1 = null;
                offset1 = theUnsafe.getLong(buffer1, DIRECT_BUFFER_ADDRESS_OFFSET);
            }
            offset1 += buffer1.position();
            length1 = buffer1.remaining();
            return compareTo(obj1, offset1, length1, buffer2);
        }

        public static int compareTo(Object buffer1, long offset1, int length1, ByteBuffer buffer)
        {
            Object obj2;
            long offset2;

            int position = buffer.position();
            int limit = buffer.limit();
            if (buffer.hasArray())
            {
                obj2 = buffer.array();
                offset2 = BYTE_ARRAY_BASE_OFFSET + buffer.arrayOffset();
            }
            else
            {
                obj2 = null;
                offset2 = theUnsafe.getLong(buffer, DIRECT_BUFFER_ADDRESS_OFFSET);
            }
            int length2 = limit - position;
            offset2 += position;

            return compareTo(buffer1, offset1, length1, obj2, offset2, length2);
        }

        /**
         * Lexicographically compare two arrays.
         *
         * @param buffer1 left operand: a byte[] or null
         * @param buffer2 right operand: a byte[] or null
         * @param memoryOffset1 Where to start comparing in the left buffer (pure memory address if buffer1 is null, or relative otherwise)
         * @param memoryOffset2 Where to start comparing in the right buffer (pure memory address if buffer1 is null, or relative otherwise)
         * @param length1 How much to compare from the left buffer
         * @param length2 How much to compare from the right buffer
         * @return 0 if equal, {@code < 0} if left is less than right, etc.
         */
        public static int compareTo(Object buffer1, long memoryOffset1, int length1,
                             Object buffer2, long memoryOffset2, int length2)
        {
            int minLength = Math.min(length1, length2);

            /*
             * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
             * time is no slower than comparing 4 bytes at a time even on 32-bit.
             * On the other hand, it is substantially faster on 64-bit.
             */
            int wordComparisons = minLength & ~7;
            for (int i = 0; i < wordComparisons; i += Longs.BYTES)
            {
                long lw = theUnsafe.getLong(buffer1, memoryOffset1 + i);
                long rw = theUnsafe.getLong(buffer2, memoryOffset2 + i);

                if (lw != rw)
                {
                    if (BIG_ENDIAN)
                    {
                        return Long.compareUnsigned(lw, rw);
                    }

                    return Long.compareUnsigned(Long.reverseBytes(lw), Long.reverseBytes(rw));
                }
            }

            for (int i = wordComparisons; i < minLength; i++)
            {
                int b1 = theUnsafe.getByte(buffer1, memoryOffset1 + i) & 0xFF;
                int b2 = theUnsafe.getByte(buffer2, memoryOffset2 + i) & 0xFF;
                if (b1 != b2)
                {
                    return b1 - b2;
                }
            }

            return length1 - length2;
        }
    }

    @SuppressWarnings("unused")
    public static final class PureJavaOperations implements ByteOperations
    {
        public int compare(ByteBuffer buffer1, ByteBuffer buffer2)
        {
            int end1 = buffer1.limit();
            int end2 = buffer2.limit();
            for (int i = buffer1.position(), j = buffer2.position(); i < end1 && j < end2; i++, j++)
            {
                int a = buffer1.get(i) & 0xff;
                int b = buffer2.get(j) & 0xff;
                if (a != b)
                {
                    return a - b;
                }
            }
            return buffer1.remaining() - buffer2.remaining();
        }
    }
}
