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

import java.util.concurrent.ThreadFactory;

import org.jetbrains.annotations.NotNull;

public final class ThreadUtil
{
    private ThreadUtil()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @NotNull
    public static ThreadFactory threadFactory(@NotNull String threadName)
    {
        return threadFactory(threadName, true);
    }

    @NotNull
    public static ThreadFactory threadFactory(@NotNull String threadName, boolean isDaemon)
    {
        return runnable -> newThread(runnable, threadName, isDaemon);
    }

    @NotNull
    private static Thread newThread(@NotNull Runnable runnable, @NotNull String threadName, boolean isDaemon)
    {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(isDaemon);
        return thread;
    }
}
