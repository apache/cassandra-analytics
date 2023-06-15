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

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public final class TimestampOption implements Serializable
{
    private static final TimestampOption NOW = new TimestampOption(System.nanoTime() / 1000);
    private String timestampColumnName;
    private Long timeStampInMicroSeconds;

    private TimestampOption(String timestampColumnName)
    {
        this.timestampColumnName = timestampColumnName;
    }

    private TimestampOption(Long timeStampInMicroSeconds)
    {
        this.timeStampInMicroSeconds = timeStampInMicroSeconds;
    }

    public static TimestampOption from(String timestamp)
    {
        if (timestamp == null)
        {
            return NOW;
        }
        try
        {
            return new TimestampOption(Long.parseLong(timestamp));
        }
        catch (Exception e)
        {

            return new TimestampOption(timestamp);
        }
    }

    /**
     * Timestamp option for write with a constant timestamp. When same values for timestamp should be used for all rows in
     * a bulk write call use this option.
     *
     * @param timestampInMicroSeconds timestamp value in microseconds
     * @return timestamp option
     */
    public static String constant(long timestampInMicroSeconds)
    {
        return String.valueOf(timestampInMicroSeconds);
    }

    /**
     * Timestamp option for write with a constant timestamp. When same values for timestamp should be used for all rows in
     * a bulk write call use this option.
     *
     * @param duration timestamp value in Duration
     * @return timestamp option
     */
    public static String constant(Duration duration)
    {
        return String.valueOf(duration.get(ChronoUnit.MICROS));
    }

    /**
     * Timestamp option for writes with timestamp per Row. When different timestamp has to be used for different rows in
     * a bulk write call use this option. The RDD should have additional column with timestamp values in microseconds
     * for each row.
     *
     * @param timeStampColumnName column name which has timestamp values for each row
     * @return timestamp option
     */
    public static String perRow(String timeStampColumnName)
    {
        return timeStampColumnName;
    }

    public static TimestampOption now()
    {
        return NOW;
    }

    public String columnName()
    {
        return timestampColumnName;
    }

    public boolean withTimestamp()
    {
        return !this.equals(NOW)
                && (timestampColumnName != null || timeStampInMicroSeconds != null);
    }

    @Override
    public String toString()
    {
        if (timestampColumnName != null && !timestampColumnName.isEmpty())
        {
            return ":" + timestampColumnName;
        }
        if (timeStampInMicroSeconds != null)
        {
            return Long.toString(timeStampInMicroSeconds);
        }
        return null;
    }
}
