package org.apache.cassandra.spark.utils;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
