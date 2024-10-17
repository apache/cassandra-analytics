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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Interface to abstract async task execution. User can provide standard java.util.concurrent.ExecutorService to perform the execution or some other mechanism.
 */
public interface AsyncExecutor
{
    /**
     * Returns a new CompletableFuture that is asynchronously completed with the value from the supplied blocking action
     *
     * @param blockingAction a blocking action that returns the value to complete the CompletableFuture
     * @param <T>            result type returned by the future.
     * @return the new CompletableFuture
     */
    <T> CompletableFuture<T> submit(Supplier<T> blockingAction);

    <T> CompletableFuture<Void> submit(Runnable blockingAction);

    <T> CompletableFuture<Void> schedule(Runnable action, long delayMillis);

    static AsyncExecutor wrap(ExecutorService executorService)
    {
        return new ExecutorServiceBased(executorService);
    }

    class ExecutorServiceBased implements AsyncExecutor
    {
        private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1,
                                                                                                   new ThreadFactoryBuilder()
                                                                                                   .setNameFormat("scheduler-%d")
                                                                                                   .setDaemon(true)
                                                                                                   .build());

        private final ExecutorService executorService;

        public ExecutorServiceBased(ExecutorService executorService)
        {
            this.executorService = executorService;
        }

        public <T> CompletableFuture<T> submit(Supplier<T> supplier)
        {
            return CompletableFuture.supplyAsync(supplier, executorService);
        }

        public <T> CompletableFuture<Void> submit(Runnable blockingAction)
        {
            return CompletableFuture.runAsync(blockingAction, executorService);
        }

        public <T> CompletableFuture<Void> schedule(Runnable action, long delayMillis)
        {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            SCHEDULER.schedule(() -> {
                submit(action)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null)
                    {
                        future.completeExceptionally(throwable);
                    }
                    else
                    {
                        future.complete(aVoid);
                    }
                });
            }, delayMillis, TimeUnit.MILLISECONDS);
            return future;
        }
    }
}
