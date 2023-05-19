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

package org.apache.cassandra.spark.bulkwriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;

public class MockScheduledExecutorService extends ScheduledThreadPoolExecutor
{
    private Runnable command;
    private boolean stopped = false;
    private long period;
    private TimeUnit timeUnit;
    List<MockScheduledFuture<?>> futures = new ArrayList<>();

    public MockScheduledExecutorService()
    {
        super(0);
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command,
                                                  long initialDelay,
                                                  long period,
                                                  @NotNull TimeUnit unit)
    {
        this.period = period;
        this.timeUnit = unit;
        this.command = command;
        return new MockScheduledFuture<Void>(command);
    }

    @Override
    public ScheduledFuture<?> submit(Runnable command)
    {
        MockScheduledFuture<?> future = new MockScheduledFuture<>(command);
        futures.add(future);
        return future;
    }

    @NotNull
    @Override
    public List<Runnable> shutdownNow()
    {
        stopped = true;
        return new ArrayList<>();
    }

    @Override
    public void shutdown()
    {
        stopped = true;
    }

    public void assertFuturesCalled()
    {
        futures.forEach(MockScheduledFuture::assertGetCalled);
    }

    public void runCommand()
    {
        command.run();
    }

    public boolean isStopped()
    {
        return stopped;
    }

    public long getPeriod()
    {
        return period;
    }

    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    static class MockScheduledFuture<V> implements ScheduledFuture<V>
    {
        private final Callable<V> command;
        private boolean getCalled = false;

        MockScheduledFuture(Callable<V> command)
        {
            this.command = command;
        }

        MockScheduledFuture(Runnable command)
        {
            this.command = () -> {
                command.run();
                return null;
            };
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit)
        {
            return 0;
        }

        @Override
        public int compareTo(@NotNull Delayed that)
        {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return false;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException
        {
            getCalled = true;
            try
            {
                return command.call();
            }
            catch (Exception exception)
            {
                throw new ExecutionException(exception);
            }
        }

        @Override
        public V get(long timeout,
                     @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            getCalled = true;
            return null;
        }

        public void assertGetCalled()
        {
            assert getCalled;
        }
    }
}
