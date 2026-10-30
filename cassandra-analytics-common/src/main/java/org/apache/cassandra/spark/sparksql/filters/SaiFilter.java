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
import java.util.Arrays;
import java.util.Objects;

import org.apache.cassandra.spark.data.SaiIndex;
import org.jetbrains.annotations.NotNull;

/**
 * SAI predicate used as an SSTable pruning hint.
 */
public final class SaiFilter implements Serializable
{
    private static final long serialVersionUID = 1L;

    public enum Operator
    {
        EQ,
        LT,
        LTE,
        GT,
        GTE
    }

    @NotNull
    private final SaiIndex index;
    @NotNull
    private final Operator operator;
    @NotNull
    private final byte[] value;

    public SaiFilter(@NotNull SaiIndex index, @NotNull Operator operator, @NotNull byte[] value)
    {
        this.index = index;
        this.operator = operator;
        this.value = value;
    }

    @NotNull
    public SaiIndex index()
    {
        return index;
    }

    @NotNull
    public Operator operator()
    {
        return operator;
    }

    @NotNull
    public byte[] value()
    {
        return value;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (!(other instanceof SaiFilter))
        {
            return false;
        }
        SaiFilter that = (SaiFilter) other;
        return Objects.equals(index, that.index)
               && Objects.equals(operator, that.operator)
               && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return 31 * Objects.hash(index, operator) + Arrays.hashCode(value);
    }

    @Override
    public String toString()
    {
        return "SaiFilter{index=" + index + ", operator=" + operator + ", value=" + Arrays.toString(value) + "}";
    }
}
