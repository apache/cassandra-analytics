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
import java.util.function.Function;

public final class TTLOption implements Serializable
{
    private static final TTLOption FOREVER = new TTLOption(0);
    private final String ttlColumnName;
    private final Integer ttlInSeconds;

    private TTLOption(String ttlColumnName)
    {
        this.ttlColumnName = ttlColumnName;
        this.ttlInSeconds = null;
    }

    private TTLOption(int ttlInSeconds)
    {
        this.ttlInSeconds = ttlInSeconds;
        this.ttlColumnName = null;
    }

    public static TTLOption from(String ttl)
    {
        if (ttl == null)
        {
            return FOREVER;
        }
        try
        {
            return new TTLOption(Integer.parseInt(ttl));
        }
        catch (Exception e)
        {

            return new TTLOption(ttl);
        }
    }

    /**
     * TTL option for write with a constant TTL. When same values for TTL should be used for all rows in a bulk write
     * call use this option.
     *
     * @param ttlInSeconds ttl value in seconds
     * @return TTLOption
     */
    public static String constant(int ttlInSeconds)
    {
        return String.valueOf(ttlInSeconds);
    }

    /**
     * TTL option for write with a constant TTL. When same values for TTL should be used for all rows in a bulk write
     * call use this option.
     *
     * @param duration ttl value in Duration
     * @return TTLOption
     */
    public static String constant(Duration duration)
    {
        return String.valueOf(duration.getSeconds());
    }

    /**
     * TTL option for writes with TTL per Row. When different TTL has to be used for different rows in a bulk write
     * call use this option. It expects the input RDD to supply the TTL values as an additional column at each row of
     * the RDD. The TTL value provider column is selected by {@code  ttlColumnName}
     *
     * @param ttlColumnName column name which has TTL values for each row
     * @return TTLOption
     */
    public static String perRow(String ttlColumnName)
    {
        return ttlColumnName;
    }

    public static TTLOption forever()
    {
        return FOREVER;
    }

    public String columnName()
    {
        return ttlColumnName;
    }

    public boolean withTTl()
    {
        return !this.equals(FOREVER)
                && (ttlColumnName != null || ttlInSeconds != null);
    }

    public String toCQLString(Function<String, String> maybeQuoteFunction)
    {
        if (ttlColumnName != null && !ttlColumnName.isEmpty())
        {
            return ":" + maybeQuoteFunction.apply(ttlColumnName);
        }
        if (ttlInSeconds != null)
        {
            return Integer.toString(ttlInSeconds);
        }
        return null;
    }
}
