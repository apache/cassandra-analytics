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

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Utility methods for {@link Map}
 */
public final class MapUtils
{
    private MapUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Returns the lower-cased key using {@link Locale#ROOT}
     *
     * @param key the key
     * @return the lower-cased key using {@link Locale#ROOT}
     */
    public static String lowerCaseKey(String key)
    {
        return key != null ? key.toLowerCase(Locale.ROOT) : null;
    }

    public static String getOrThrow(Map<String, String> options, String key)
    {
        return getOrThrow(options, key, key);
    }

    public static String getOrThrow(Map<String, String> options, String key, String name)
    {
        return getOrThrow(options, key, throwable(name));
    }

    public static Supplier<RuntimeException> throwable(String name)
    {
        return () -> new RuntimeException(String.format("No %s specified", name));
    }

    public static String getOrThrow(Map<String, String> options, String key, Supplier<RuntimeException> throwable)
    {
        return getOrHandle(options, key, () -> {
            throw throwable.get();
        });
    }

    public static String getOrHandle(Map<String, String> options, String key, Supplier<String> handler)
    {
        String value = options.get(lowerCaseKey(key));
        if (value == null)
        {
            return handler.get();
        }
        return value;
    }

    /**
     * Returns the boolean value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @return the boolean value
     */
    public static boolean getBoolean(Map<String, String> options, String key, boolean defaultValue)
    {
        return getBoolean(options, key, defaultValue, null);
    }

    /**
     * Returns the boolean value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @param displayName  an optional name to display in the error message
     * @return the boolean value
     */
    public static boolean getBoolean(Map<String, String> options, String key, boolean defaultValue, String displayName)
    {
        String value = options.get(lowerCaseKey(key));
        // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings
        if (value == null)
        {
            return defaultValue;
        }
        else if (value.equalsIgnoreCase("true"))
        {
            return true;
        }
        else if (value.equalsIgnoreCase("false"))
        {
            return false;
        }
        displayName = displayName != null ? displayName : key;
        throw new IllegalArgumentException("Key " + displayName + " with value " + value + " is not a valid boolean string");
    }

    /**
     * Returns the int value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @return the int value
     */
    public static int getInt(Map<String, String> options, String key, int defaultValue)
    {
        return getInt(options, key, defaultValue, null);
    }

    /**
     * Returns the int value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @param displayName  an optional name to display in the error message
     * @return the int value
     */
    public static int getInt(Map<String, String> options, String key, int defaultValue, String displayName)
    {
        return getOptionalInt(options, key, displayName).orElse(defaultValue);
    }

    /**
     * Returns an {@link Optional} of int value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. An empty {@link Optional} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param displayName  an optional name to display in the error message
     * @return {@link Optional} of {@link Integer}
     */
    public static Optional<Integer> getOptionalInt(Map<String, String> options, String key, String displayName)
    {
        String value = options.get(lowerCaseKey(key));
        try
        {
            return value != null ? Optional.of(Integer.parseInt(value)) : Optional.empty();
        }
        catch (NumberFormatException exception)
        {
            displayName = displayName != null ? displayName : key;
            throw new IllegalArgumentException("Key " + displayName + " with value " + value + " is not a valid integer string.",
                                               exception);
        }
    }

    /**
     * Returns the long value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @return the long value
     */
    public static long getLong(Map<String, String> options, String key, long defaultValue)
    {
        String value = options.get(lowerCaseKey(key));
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    /**
     * Returns the enum variant for the given {@code key} and the {@code enumClass}. The {@code defaultValue} is returned
     * when the lookup misses.
     *
     * @param options       the map
     * @param key           the key to lookup
     * @param defaultValue  the default value
     * @param displayName   an optional name to display in the error message
     * @return the enum variant or the default value if the lookup misses
     * @param <T> enum type
     */
    public static <T extends Enum<T>> T getEnumOption(Map<String, String> options, String key, T defaultValue, String displayName)
    {
        String value = options.get(lowerCaseKey(key));
        try
        {
            return value != null ? Enum.valueOf(defaultValue.getDeclaringClass(), value) : defaultValue;
        }
        catch (IllegalArgumentException exception)
        {
            displayName = displayName != null ? displayName : key;
            throw new IllegalArgumentException("Key " + displayName + " with value " + value + " is not a valid Enum of type " + defaultValue.getClass() + ".",
                                               exception);
        }
    }

    /**
     * Returns the String value for the given {@code key} in the {@code options} map. The key is lower-cased before
     * accessing the map. The {@code defaultValue} is returned when the key doesn't match any element from the map.
     *
     * @param options      the map
     * @param key          the key to the map
     * @param defaultValue the default value
     * @return String value
     */
    public static String getOrDefault(Map<String, String> options, String key, String defaultValue)
    {
        return options.getOrDefault(lowerCaseKey(key), defaultValue);
    }

    /**
     * Method to check if key is present in {@code options} map.
     * @param options   the map
     * @param key       the key to the map
     * @return boolean
     */
    public static boolean containsKey(Map<String, String> options, String key)
    {
        return options.containsKey(lowerCaseKey(key));
    }
}
