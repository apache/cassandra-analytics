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

package org.apache.cassandra.bridge;

import java.lang.reflect.Method;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.TypeConverter;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.jetbrains.annotations.NotNull;

/**
 * Spark-specific implementation of TypeConverter that delegates to SparkSqlTypeConverter.
 * This implementation is contained within the bridge implementation to avoid leaking
 * Spark dependencies to the public API.
 */
final class SparkTypeConverterWrapper implements TypeConverter
{
    private final SparkSqlTypeConverter delegate;

    SparkTypeConverterWrapper(SparkSqlTypeConverter delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public Object convert(CqlField.CqlType cqlType, @NotNull Object value, boolean isFrozen)
    {
        try
        {
            Method method = delegate.getClass().getMethod("convert", CqlField.CqlType.class, Object.class, boolean.class);
            return method.invoke(delegate, cqlType, value, isFrozen);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to invoke convert method", e);
        }
    }
}
