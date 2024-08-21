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

package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Float implements SparkType
{
    public static final org.apache.cassandra.spark.data.converter.types.Float INSTANCE = new org.apache.cassandra.spark.data.converter.types.Float();

    private Float()
    {

    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.FloatType;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int position)
    {
        return row.getFloat(position);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getFloat(position);
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return CqlField.FLOAT_COMPARATOR.compare((java.lang.Float) first, (java.lang.Float) second);
    }
}
