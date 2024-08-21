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

package org.apache.cassandra.spark.data.converter.types.complex;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

public class Frozen implements SparkType
{
    private final SparkType inner;

    public Frozen(SparkSqlTypeConverter converter, CqlField.CqlType inner)
    {
        this.inner = converter.toSparkType(inner);
    }

    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return inner.toSparkSqlType(value, true);
    }

    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        return inner.sparkSqlRowValue(row, position);
    }

    public Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return inner.nativeSparkSqlRowValue(row, position);
    }

    public Object sparkSqlRowValue(Row row, int position)
    {
        return inner.sparkSqlRowValue(row, position);
    }

    public Object nativeSparkSqlRowValue(Row row, int position)
    {
        return inner.nativeSparkSqlRowValue(row, position);
    }

    public Object toTestRowType(Object value)
    {
        return inner.toTestRowType(value);
    }

    public boolean equalsTo(Object first, Object second)
    {
        return inner.equalsTo(first, second);
    }

    public int compareTo(Object first, Object second)
    {
        return inner.compareTo(first, second);
    }

    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return inner.dataType(bigNumberConfig);
    }
}
