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
import org.apache.cassandra.spark.data.SchemaConverter;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;

/**
 * Spark-specific implementation of SchemaConverter that delegates to SparkSqlTypeConverter.
 * This implementation is contained within the bridge implementation to avoid leaking
 * Spark dependencies to the public API.
 */
final class SparkSchemaConverter implements SchemaConverter
{
    private final SparkSqlTypeConverter delegate;

    SparkSchemaConverter(SparkSqlTypeConverter delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public Object getDataType(CqlField field, BigNumberConfig bigNumberConfig)
    {
        try
        {
            Method method = delegate.getClass().getMethod("sparkSqlType", CqlField.class, BigNumberConfig.class);
            return method.invoke(delegate, field, bigNumberConfig);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to invoke sparkSqlType method", e);
        }
    }
}
