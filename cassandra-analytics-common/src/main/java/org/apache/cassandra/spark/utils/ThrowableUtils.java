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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ThrowableUtils
{
    private ThrowableUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Find root cause of throwable or this throwable if no prior cause
     *
     * @param throwable throwable
     * @return initial cause throwable
     */
    @NotNull
    public static Throwable rootCause(@NotNull Throwable throwable)
    {
        while (throwable.getCause() != null)
        {
            throwable = throwable.getCause();
        }
        return throwable;
    }

    /**
     * Find first throwable of type matching ofType parameter or null if not exists
     *
     * @param <T>       generic type of expected return value
     * @param throwable throwable
     * @param ofType    type of class expected
     * @return first throwable of type matching parameter ofType or null if cannot be found
     */
    @Nullable
    public static <T extends Throwable> T rootCause(@NotNull Throwable throwable, @NotNull Class<T> ofType)
    {
        while (throwable.getCause() != null)
        {
            if (ofType.isInstance(throwable))
            {
                return ofType.cast(throwable);
            }
            throwable = throwable.getCause();
        }

        return ofType.isInstance(throwable) ? ofType.cast(throwable) : null;
    }
}
