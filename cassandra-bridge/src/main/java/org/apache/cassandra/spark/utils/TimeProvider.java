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

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

/**
 * Provides time
  */
public interface TimeProvider
{
    /**
     * The {@code DEFAULT} time provider sets the reference time on class loading. The reference time is fixed for the
     * local JVM. If the same reference time for all executors is desired, you should avoid using the {@code DEFAULT}
     * and use {@link ReaderTimeProvider} or similar kind that allows to broadcast the reference time value from driver.
     * <p></p>
     * It should be used for testing only.
     */
    @VisibleForTesting
    TimeProvider DEFAULT = new TimeProvider()
    {
        private final int referenceEpochInSeconds = nowInSeconds();

        @Override
        public int referenceEpochInSeconds()
        {
            return referenceEpochInSeconds;
        }
    };

    /**
     * @return current time in seconds
     */
    default int nowInSeconds()
    {
        return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

    /**
     * Get the time value that is used as a reference. It should never change throughout the lifecycle of the provider
     * @return a fixed epoch time in seconds
     *
     * Note that the actual constant value returned is implementation dependent
     */
    int referenceEpochInSeconds();
}
