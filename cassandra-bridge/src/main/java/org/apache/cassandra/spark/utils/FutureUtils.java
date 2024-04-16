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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class FutureUtils
{
    public static final class FutureResult<T>
    {
        @Nullable
        public final T value;
        @Nullable
        public final Throwable throwable;

        private FutureResult(@Nullable T value, @Nullable Throwable throwable)
        {
            this.value = value;
            this.throwable = throwable;
        }

        public static <T> FutureResult<T> failed(@NotNull Throwable throwable)
        {
            return new FutureResult<>(null, throwable);
        }

        public static <T> FutureResult<T> success(@Nullable T value)
        {
            return new FutureResult<>(value, null);
        }

        @Nullable
        public T value()
        {
            return value;
        }

        public boolean isSuccess()
        {
            return throwable == null;
        }
    }

    private FutureUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Await all futures and combine into single result
     *
     * @param <T>                 result type returned by this method
     * @param futures             collection of futures
     * @param acceptPartialResult if false, fail the entire request if a single failure occurs, if true just log partial failures
     * @param onFailure           consumer of errors
     * @return result of all combined futures
     */
    public static <T> List<T> awaitAll(Collection<CompletableFuture<T>> futures,
                                       boolean acceptPartialResult,
                                       Consumer<Throwable> onFailure)
    {
        List<T> result = new ArrayList<>(futures.size());
        for (CompletableFuture<T> future : futures)
        {
            FutureResult<T> futureResult = await(future, onFailure);
            if (futureResult.throwable != null)
            {
                // Failed
                if (!acceptPartialResult)
                {
                    throw new RuntimeException(ThrowableUtils.rootCause(futureResult.throwable));
                }
            }
            else if (futureResult.value != null)
            {
                // Success
                result.add(futureResult.value);
            }
        }
        return result;
    }

    /**
     * Await a future and return result
     *
     * @param future the future
     * @param logger consumer to log errors
     * @param <T>    result type returned by this method
     * @return result of the future
     */
    @NotNull
    public static <T> FutureResult<T> await(CompletableFuture<T> future, Consumer<Throwable> logger)
    {
        try
        {
            return FutureResult.success(future.get());
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
        catch (ExecutionException exception)
        {
            logger.accept(exception);
            return FutureResult.failed(exception);
        }
    }

    /**
     * Combine futures into a single future that completes when all futures complete successfully, or fails if any future fails
     *
     * @param <T>     result type returned by this method
     * @param futures array of futures
     * @return a single future that combines all future results
     */
    public static <T> CompletableFuture<List<T>> combine(List<CompletableFuture<T>> futures)
    {
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                         .whenComplete((aVoid, aThrowable) -> {
                             if (aThrowable == null)
                             {
                                 try
                                 {
                                     // Combine future results into a single list: get is called, but it will not block as all the futures have completed
                                     result.complete(awaitAll(futures, false, throwable -> { }));
                                 }
                                 catch (Throwable throwable)
                                 {
                                     result.completeExceptionally(ThrowableUtils.rootCause(throwable));
                                 }
                             }
                             else
                             {
                                 result.completeExceptionally(aThrowable);
                             }
                         });
        return result;
    }

    /**
     * Makes a best-effort attempt to obtain results of a collection of completable futures
     * within the specified timeout, then combines and returns all received non-null values
     *
     * @param futures  list of futures to obtain results from
     * @param timeout  duration of the timeout to use
     * @param timeUnit units of the timeout
     * @return list on non-null values obtained
     */
    public static <T> List<T> bestEffortGet(List<CompletableFuture<T>> futures, long timeout, TimeUnit timeUnit)
    {
        try
        {
            // As a barrier
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .get(timeout, timeUnit);
        }
        catch (InterruptedException | ExecutionException | TimeoutException exception)
        {
            // Do nothing, cancel later
        }
        return futures.stream()
                      .map(future -> {
                          if (future.isDone())
                          {
                              // Convert exception into null and ignore later
                              return future.exceptionally(t -> null)
                                      .join();
                          }
                          else
                          {
                              future.cancel(true);
                              return null;
                          }
                      })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());
    }
}
