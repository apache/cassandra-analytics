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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.jetbrains.annotations.NotNull;

/**
 * CDC offset filter used to checkpoint Streaming queries and only read mutations within watermark window
 */
public final class CdcOffsetFilter implements Serializable
{
    @NotNull
    private final CdcOffset start;
    private final long maxAgeMicros;

    private CdcOffsetFilter(@NotNull CdcOffset start, @NotNull Duration watermarkWindow)
    {
        Preconditions.checkNotNull(start, "Start offset cannot be null");
        this.start = start;
        this.maxAgeMicros = start.getTimestampMicros() - TimeUnit.NANOSECONDS.toMicros(watermarkWindow.toNanos());
    }

    /**
     * Return true if mutation timestamp overlaps with watermark window
     *
     * @param timestampMicros mutation timestamp in micros
     * @return true if timestamp overlaps with range
     */
    public boolean overlaps(long timestampMicros)
    {
        return timestampMicros >= maxAgeMicros;
    }

    @NotNull
    public static CdcOffsetFilter of(@NotNull CdcOffset start, @NotNull Duration watermarkWindow)
    {
        return new CdcOffsetFilter(start, watermarkWindow);
    }

    public long maxAgeMicros()
    {
        return maxAgeMicros;
    }

    @NotNull
    public CdcOffset start()
    {
        return start;
    }

    public boolean overlaps(TokenRange tokenRange)
    {
        return false;
    }

    public boolean filter(ByteBuffer key)
    {
        return false;
    }

    public boolean filter(SparkSSTableReader reader)
    {
        return false;
    }
}
