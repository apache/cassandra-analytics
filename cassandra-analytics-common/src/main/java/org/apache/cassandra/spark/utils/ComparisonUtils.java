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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;

import com.google.common.primitives.UnsignedBytes;

public final class ComparisonUtils
{
    private static final Comparator<Object> INET_COMPARATOR = (first, second) ->
            UnsignedBytes.lexicographicalComparator().compare(((Inet4Address) first).getAddress(),
                                                              ((Inet4Address) second).getAddress());

    private static final Comparator<Object> NESTED_COMPARATOR = new Comparator<Object>()
    {
        @SuppressWarnings({"unchecked", "rawtypes"})
        public int compare(Object first, Object second)
        {
            if (first instanceof Comparable && second instanceof Comparable)
            {
                return ((Comparable) first).compareTo(second);
            }
            else if (first instanceof Object[] && second instanceof Object[])
            {
                Object[] array1 = (Object[]) first;
                Object[] array2 = (Object[]) second;
                int position = 0;
                while (position < array1.length && position < array2.length)
                {
                    int comparison = NESTED_COMPARATOR.compare(array1[position], array2[position]);
                    if (comparison != 0)
                    {
                        return comparison;
                    }
                    position++;
                }
                return Integer.compare(array1.length, array2.length);
            }
            else if (first instanceof Map && second instanceof Map)
            {
                Map<?, ?> map1 = (Map<?, ?>) first;
                Map<?, ?> map2 = (Map<?, ?>) second;
                for (Object key : map1.keySet())
                {
                    int comparison = NESTED_COMPARATOR.compare(map1.get(key), map2.get(key));
                    if (comparison != 0)
                    {
                        return comparison;
                    }
                }
                return Integer.compare(map1.size(), map2.size());
            }
            else if (first instanceof Collection && second instanceof Collection)
            {
                return NESTED_COMPARATOR.compare(((Collection) first).toArray(new Object[0]), ((Collection) second).toArray(new Object[0]));
            }
            else if (first instanceof Inet4Address && second instanceof Inet4Address)
            {
                return INET_COMPARATOR.compare(first, second);
            }
            throw new IllegalStateException("Unexpected comparable type: " + first.getClass().getName());
        }
    };

    private ComparisonUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static boolean equals(Object[] first, Object[] second)
    {
        if (first == second)
        {
            return true;
        }
        if (first == null || second == null)
        {
            return false;
        }

        int length = first.length;
        if (second.length != length)
        {
            return false;
        }

        for (int index = 0; index < length; index++)
        {
            if (!equals(first[index], second[index]))
            {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    public static boolean equals(Object first, Object second)
    {
        if (first instanceof UUID && second instanceof String)  // Spark does not support UUID. We compare the string values.
        {
            return first.toString().equals(second);
        }
        else if (first instanceof ByteBuffer && second instanceof byte[])
        {
            return Arrays.equals(ByteBufferUtils.getArray((ByteBuffer) first), (byte[]) second);
        }
        else if (first instanceof InetAddress && second instanceof byte[])
        {
            return Arrays.equals(((InetAddress) first).getAddress(), (byte[]) second);
        }
        else if (first instanceof BigInteger && second instanceof BigDecimal)
        {
            // Compare the string values
            return first.toString().equals(second.toString());
        }
        else if (first instanceof Integer && second instanceof Date)
        {
            return first.equals((int) ((Date) second).toLocalDate().toEpochDay());
        }

        if (Objects.equals(first, second))
        {
            return true;
        }

        if (first instanceof BigDecimal && second instanceof BigDecimal)
        {
            return ((BigDecimal) first).compareTo((BigDecimal) second) == 0;
        }
        else if (first instanceof Collection && second instanceof Collection)
        {
            Object[] firstArray = ((Collection<Object>) first).toArray();
            Arrays.sort(firstArray, NESTED_COMPARATOR);
            Object[] secondArray = ((Collection<Object>) second).toArray();
            Arrays.sort(secondArray, NESTED_COMPARATOR);
            return equals(firstArray, secondArray);
        }
        else if (first instanceof Map && second instanceof Map)
        {
            Object[] firstKeys = ((Map<Object, Object>) first).keySet().toArray();
            Object[] secondKeys = ((Map<Object, Object>) second).keySet().toArray();
            if (firstKeys[0] instanceof Comparable)
            {
                Arrays.sort(firstKeys);
                Arrays.sort(secondKeys);
            }
            else if (firstKeys[0] instanceof Inet4Address)  // Inet4Address is not Comparable so do byte ordering
            {
                Arrays.sort(firstKeys, INET_COMPARATOR);
                Arrays.sort(secondKeys, INET_COMPARATOR);
            }
            if (equals(firstKeys, secondKeys))
            {
                Object[] firstValues = new Object[firstKeys.length];
                Object[] secondValues = new Object[secondKeys.length];
                IntStream.range(0, firstKeys.length).forEach(position -> {
                    firstValues[position] = ((Map<Object, Object>) first).get(firstKeys[position]);
                    secondValues[position] = ((Map<Object, Object>) second).get(secondKeys[position]);
                });
                return equals(firstValues, secondValues);
            }
            return false;
        }
        else if (first instanceof Object[] && second instanceof Object[])
        {
            return equals((Object[]) first, (Object[]) second);
        }

        return false;
    }
}
