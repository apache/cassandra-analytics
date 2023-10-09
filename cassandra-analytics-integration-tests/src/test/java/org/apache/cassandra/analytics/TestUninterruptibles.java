/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Like Uninterruptibles, but also checks for completion on timeout
 */
public class TestUninterruptibles
{
    private TestUninterruptibles()
    {
        throw new RuntimeException("Utility class should not have constructor");
    }

    /**
     * Waits for a latch to count down to zero or throws if the timeout occurs
     * @param latch the latch upon which to wait
     * @param timeout the time to wait
     * @param unit the time unit of the timeout parameter.
     */
    public static void awaitUninterruptiblyOrThrow(CountDownLatch latch, long timeout, TimeUnit unit)
    {
        if (Uninterruptibles.awaitUninterruptibly(latch, timeout, unit))
        {
            return;
        }
        throw new RuntimeException(String.format("Waited %d %s for latch to count down but did not complete.",
                                                 timeout, unit));
    }
}
