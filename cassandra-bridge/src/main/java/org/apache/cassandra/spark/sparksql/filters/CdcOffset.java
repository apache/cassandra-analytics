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

import java.io.IOException;
import java.io.Serializable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.execution.streaming.Offset;
import org.jetbrains.annotations.NotNull;

public class CdcOffset extends Offset implements Serializable, Comparable<CdcOffset>
{
    @VisibleForTesting
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private final long timestampMicros;

    @JsonCreator
    public CdcOffset(@JsonProperty("timestamp") long timestampMicros)
    {
        this.timestampMicros = timestampMicros;
    }

    public static CdcOffset fromJson(String json)
    {
        try
        {
            return MAPPER.readValue(json, CdcOffset.class);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public long getTimestampMicros()
    {
        return timestampMicros;
    }

    @Override
    public String json()
    {
        try
        {
            return MAPPER.writeValueAsString(this);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public String toString()
    {
        return json();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CdcOffset that = (CdcOffset) other;
        return timestampMicros == that.timestampMicros;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(timestampMicros)
               .toHashCode();
    }

    @Override
    public int compareTo(@NotNull CdcOffset that)
    {
        return Long.compare(this.timestampMicros, that.timestampMicros);
    }
}
