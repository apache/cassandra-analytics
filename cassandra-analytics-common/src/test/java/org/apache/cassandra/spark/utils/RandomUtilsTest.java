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

import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RandomUtilsTest
{
    public static final Set<Character> ALPHANUMERIC_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".chars()
                                                                                                  .mapToObj(e -> (char) e)
                                                                                                  .collect(Collectors.toSet());

    private final Random random = new Random();

    @Test
    public void testNextInt()
    {
        for (int i = 0; i < 1000; i++)
        {
            assertThat(RandomUtils.nextInt(random, 4, 5)).isEqualTo(4);
        }

        for (int i = 0; i < 1000; i++)
        {
            int r = RandomUtils.nextInt(random, 4, 7);
            assertThat(r >= 4).isTrue();
            assertThat(r < 7).isTrue();
        }
    }

    @Test
    public void testNextIntThrows()
    {
        assertThatThrownBy(() -> RandomUtils.nextInt(random, -1, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RandomUtils.nextInt(random, -5, -2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RandomUtils.nextInt(random, 5, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RandomUtils.nextInt(random, 10, 5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRandomAscii()
    {
        for (int i = 0; i < 1000000; i++)
        {
            assertThat(ALPHANUMERIC_CHARS.contains(RandomUtils.randomAsciiAlphanumeric(random))).isTrue();
        }
    }

    @Test
    public void testRandomString()
    {
        for (int i = 0; i < 1000; i++)
        {
            int len = RandomUtils.nextInt(random, 20, 100);
            String str = RandomUtils.randomAlphanumeric(random, len);
            assertThat(str.length()).isEqualTo(len);
            for (int j = 0; j < str.length(); j++)
            {
                assertThat(ALPHANUMERIC_CHARS.contains(str.charAt(j))).isTrue();
            }
        }
    }
}
