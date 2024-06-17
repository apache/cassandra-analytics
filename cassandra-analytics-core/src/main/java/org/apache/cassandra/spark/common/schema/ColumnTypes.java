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

package org.apache.cassandra.spark.common.schema;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;

@SuppressWarnings("unused")  // Extra types can become useful in the future
public final class ColumnTypes implements Serializable
{
    // Non-Nullable Types
    public static final ColumnType<String> STRING = new StringType();
    public static final ColumnType<Integer> INT = new IntegerType();
    public static final ColumnType<Long> LONG = new LongType();
    public static final ColumnType<ByteBuffer> BYTES = new BytesType();
    public static final ColumnType<String> STRING_UUID = new StringUuidType();
    public static final ColumnType<java.util.UUID> UUID = new UuidType();
    public static final ColumnType<Double> DOUBLE = new DoubleType();
    public static final ColumnType<Boolean> BOOLEAN = new BooleanType();
    public static final ColumnType<Date> TIMESTAMP = new TimestampType();

    // Nullable Types
    public static final ColumnType<String> NULLABLE_STRING = new StringType()
    {
        @Override
        public String getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<Integer> NULLABLE_INT = new IntegerType()
    {
        @Override
        public Integer getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<Long> NULLABLE_LONG = new LongType()
    {
        @Override
        public Long getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<ByteBuffer> NULLABLE_BYTES = new BytesType()
    {
        @Override
        public ByteBuffer getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<String> NULLABLE_STRING_UUID = new StringUuidType()
    {
        @Override
        public String getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<java.util.UUID> NULLABLE_UUID = new UuidType()
    {
        @Override
        public java.util.UUID getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<Double> NULLABLE_DOUBLE = new DoubleType()
    {
        @Override
        public Double getDefault()
        {
            return null;
        }
    };
    public static final ColumnType<Boolean> NULLABLE_BOOLEAN = new BooleanType()
    {
        @Override
        public Boolean getDefault()
        {
            return null;
        }
    };

    private ColumnTypes()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }
}
