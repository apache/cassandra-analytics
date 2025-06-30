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

import javax.annotation.Nonnull;

import static java.lang.String.format;

@SuppressWarnings("UnusedReturnValue")
public final class Preconditions
{
    public static boolean isNullOrEmpty(String str)
    {
        return str == null || str.isEmpty();
    }

    private Preconditions()
    {

    }

    public static void checkArgument(boolean expression)
    {
        if (!expression)
        {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean value,
                                     @Nonnull String error,
                                     Object... exceptionArguments)
    {
        if (!value)
        {
            throw new IllegalArgumentException(isEmpty(exceptionArguments) ? error : format(error, exceptionArguments));
        }
    }

    public static void checkState(boolean value,
                                  @Nonnull String error,
                                  Object... exceptionArguments)
    {
        if (!value)
        {
            throw new IllegalStateException(isEmpty(exceptionArguments) ? error : format(error, exceptionArguments));
        }
    }

    public static <T> T checkNotNull(T value)
    {
        if (value == null)
        {
            throw new NullPointerException();
        }
        return value;
    }

    public static <T> T checkNotNull(T value,
                                     @Nonnull String error,
                                     Object... exceptionArguments)
    {
        if (value == null)
        {
            throw new NullPointerException(isEmpty(exceptionArguments) ? error : format(error, exceptionArguments));
        }
        return value;
    }

    private static boolean isEmpty(Object... exceptionArguments)
    {
        return exceptionArguments == null || exceptionArguments.length == 0;
    }
}
