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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleTaskSchedulerTest
{
    private SimpleTaskScheduler simpleTaskScheduler = new SimpleTaskScheduler();
    private String heartbeatName = "test-heartbeat";

    @AfterEach
    public void teardown()
    {
        simpleTaskScheduler.unschedule(heartbeatName);
    }

    @Test
    public void testSchedulePeriodicHeartbeat()
    {
        CountDownLatch latch = new CountDownLatch(10);
        long start = System.nanoTime();
        simpleTaskScheduler.schedulePeriodic(heartbeatName, 10, latch::countDown);
        Uninterruptibles.awaitUninterruptibly(latch);
        assertEquals(0, latch.getCount());
        assertTrue(System.nanoTime() > start + TimeUnit.MILLISECONDS.toNanos(10 * 10));
    }

    @Test
    public void testSchedulePeriodicSuppressThrows()
    {
        CountDownLatch latch = new CountDownLatch(10);
        long start = System.nanoTime();
        simpleTaskScheduler.schedulePeriodic(heartbeatName, 10, () -> {
            latch.countDown();
            throw new RuntimeException("It fails");
        });
        Uninterruptibles.awaitUninterruptibly(latch);
        assertEquals(0, latch.getCount());
        assertTrue(System.nanoTime() > start + TimeUnit.MILLISECONDS.toNanos(10 * 10));
    }
}
