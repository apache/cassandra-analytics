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

import java.nio.ByteBuffer;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public interface BinaryTraits extends SparkType
{
    @Override
    default Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return ByteBufferUtils.getArray((ByteBuffer) value); // byte[]
    }

    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.BinaryType;
    }

    default Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getBinary(position);
    }

    default Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getAs(position);
    }

    @Override
    default Object toTestRowType(Object value)
    {
        if (value instanceof byte[])
        {
            return ByteBuffer.wrap((byte[]) value);
        }
        return value;
    }

    @Override
    default int compareTo(Object first, Object second)
    {
        return CqlField.BYTE_ARRAY_COMPARATOR.compare((byte[]) first, (byte[]) second);
    }
}
