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

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomUtilsTest
{
    public static final Set<Character> ALPHANUMERIC_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".chars()
                                                                                                  .mapToObj(e -> (char) e)
                                                                                                  .collect(Collectors.toSet());

    @Test
    public void testNextInt()
    {
        for (int i = 0; i < 1000; i++)
        {
            assertEquals(4, RandomUtils.nextInt(4, 5));
        }

        for (int i = 0; i < 1000; i++)
        {
            int r = RandomUtils.nextInt(4, 7);
            assertTrue(r >= 4);
            assertTrue(r < 7);
        }
    }

    @Test
    public void testNextIntThrows()
    {
        assertThrows(IllegalArgumentException.class, () -> RandomUtils.nextInt(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> RandomUtils.nextInt(-5, -2));
        assertThrows(IllegalArgumentException.class, () -> RandomUtils.nextInt(5, 5));
        assertThrows(IllegalArgumentException.class, () -> RandomUtils.nextInt(10, 5));
    }

    @Test
    public void testRandomAscii()
    {
        for (int i = 0; i < 1000000; i++)
        {
            assertTrue(ALPHANUMERIC_CHARS.contains(RandomUtils.randomAsciiAlphanumeric()));
        }
    }

    @Test
    public void testRandomString()
    {
        for (int i = 0; i < 1000; i++)
        {
            int len = RandomUtils.nextInt(20, 100);
            String str = RandomUtils.randomAlphanumeric(len);
            assertEquals(len, str.length());
            for (int j = 0; j < str.length(); j++)
            {
                assertTrue(ALPHANUMERIC_CHARS.contains(str.charAt(j)));
            }
        }
    }
}
