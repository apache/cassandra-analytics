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

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.util.ThreadUtil;

/**
 * Scheduler for simple and short tasks
 */
public class SimpleTaskScheduler implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTaskScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> scheduledTasks;
    private boolean isClosed;

    public SimpleTaskScheduler()
    {
        ThreadFactory tf = ThreadUtil.threadFactory("bulk-write-simple-task-scheduler");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(tf);
        this.scheduledTasks = new HashMap<>();
        this.isClosed = false;
    }

    public synchronized void schedule(String name, Duration delay, Runnable task)
    {
        if (isClosed() || isScheduled(name))
        {
            return;
        }

        ScheduledFuture<?> fut = scheduler.schedule(new NoThrow(name, task),
                                                    delay.toMillis(),
                                                    TimeUnit.MILLISECONDS);
        scheduledTasks.put(name, fut);
    }

    public synchronized void schedulePeriodic(String name, Duration interval, Runnable task)
    {
        if (isClosed() || isScheduled(name))
        {
            return;
        }

        ScheduledFuture<?> fut = scheduler.scheduleWithFixedDelay(new NoThrow(name, task),
                                                                  interval.toMillis(), // initial delay
                                                                  interval.toMillis(), // delay
                                                                  TimeUnit.MILLISECONDS);
        scheduledTasks.put(name, fut);
    }

    // return true if unscheduled; return false if unable to unschedule, typically it is unscheduled already
    @VisibleForTesting
    public synchronized boolean unschedule(String name)
    {
        if (isClosed())
        {
            return false;
        }

        ScheduledFuture<?> fut = scheduledTasks.remove(name);
        if (fut == null)
        {
            return false;
        }
        return fut.cancel(true);
    }

    /**
     * Close the resources at best effort. The action is uninterruptible, but the interruption status is restore.
     */
    public synchronized void close()
    {
        isClosed = true;
        scheduledTasks.values().forEach(fut -> fut.cancel(true));
        scheduler.shutdownNow();
        try
        {
            boolean terminated = scheduler.awaitTermination(2, TimeUnit.SECONDS);
            if (!terminated)
            {
                LOGGER.warn("Closing SimpleTaskScheduler times out");
            }
            else
            {
                LOGGER.info("SimpleTaskScheduler is closed");
            }
        }
        catch (InterruptedException ie)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception exception)
        {
            LOGGER.warn("Exception when closing scheduler", exception);
        }
    }

    private boolean isClosed()
    {
        if (isClosed)
        {
            LOGGER.debug("SimpleTaskScheduler is already closed");
        }
        return isClosed;
    }

    private boolean isScheduled(String name)
    {
        boolean isScheduled = scheduledTasks.containsKey(name);
        if (isScheduled)
        {
            LOGGER.debug("The task has been scheduled already. task={}", name);
        }
        return isScheduled;
    }

    // A Runnable wrapper that does not throw exceptions. Therefore, it gets executed again by scheduler
    private static class NoThrow implements Runnable
    {
        private final String name;
        private final Runnable beat;

        NoThrow(String name, Runnable beat)
        {
            this.beat = beat;
            this.name = name;
        }

        @Override
        public void run()
        {
            try
            {
                beat.run();
            }
            catch (Exception exception)
            {
                LOGGER.warn("{} failed to run", name, exception);
            }
        }
    }
}
