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

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

/**
 * This code is borrowed from the Cassandra code in {@code org.apache.cassandra.utils.TimeUUID} with slight
 * modifications. The code was copied over to remove the dependency we had on the cassandra client library.
 */
public final class UUIDs
{
    private static final long START_EPOCH = makeEpoch();
    private static final long CLOCK_SEQ_AND_NODE = makeClockSeqAndNode();

    private UUIDs()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static UUID startOf(long timestamp)
    {
        return new UUID(makeMSB(fromUnixTimestamp(timestamp)), CLOCK_SEQ_AND_NODE);
    }

    @VisibleForTesting
    static long fromUnixTimestamp(long timestamp)
    {
        return (timestamp - START_EPOCH) * 10000;
    }

    @VisibleForTesting
    static long makeMSB(long timestamp)
    {
        long msb = 0L;
        msb |= (0x00000000FFFFFFFFL & timestamp)  << 32;
        msb |= (0x0000FFFF00000000L & timestamp) >>> 16;
        msb |= (0x0FFF000000000000L & timestamp) >>> 48;
        msb |=  0x0000000000001000L;  // Sets the version to 1
        return msb;
    }

    private static long makeEpoch()
    {
        // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
        calendar.set(Calendar.YEAR, 1582);
        calendar.set(Calendar.MONTH, Calendar.OCTOBER);
        calendar.set(Calendar.DAY_OF_MONTH, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    private static long makeClockSeqAndNode()
    {
        SecureRandom secureRandom = new SecureRandom();
        long clock = secureRandom.nextLong();

        long lsb = 0;
        lsb |= 0x8000000000000000L;                  // Variant (2 bits)
        lsb |= (clock & 0x0000000000003FFFL) << 48;  // Clock sequence (14 bits)
        lsb |= secureRandom.nextLong();              // 6 bytes
        return lsb;
    }
}
