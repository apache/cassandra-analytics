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

package org.apache.cassandra.bridge.type;

import java.io.Serializable;
import java.util.Objects;

/**
 * Analytics-internal Duration type to bridge CQL Duration type.
 */
public class InternalDuration implements Serializable
{
    private static final long serialVersionUID = -6867844786318937949L;
    public final int months;
    public final int days;
    public final long nanoseconds;

    public InternalDuration(int months, int days, long nanoseconds)
    {
        this.months = months;
        this.days = days;
        this.nanoseconds = nanoseconds;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        InternalDuration that = (InternalDuration) o;
        return months == that.months &&
               days == that.days &&
               nanoseconds == that.nanoseconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(months, days, nanoseconds);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        if (this.months < 0 || this.days < 0 || this.nanoseconds < 0L)
        {
            builder.append('-');
        }
        builder.append("mo").append(Math.abs(this.months));
        builder.append("d").append(Math.abs(this.days));
        builder.append("ns").append(Math.abs(this.nanoseconds));
        return builder.toString();
    }
}
