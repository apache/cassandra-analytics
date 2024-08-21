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

package org.apache.cassandra.spark.data.converter;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.TypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;

public interface SparkSqlTypeConverter extends TypeConverter
{
    Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position);

    Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position);

    default Object sparkSqlRowValue(CqlField cqlField, GenericInternalRow row, int position)
    {
        return sparkSqlRowValue(cqlField.type(), row, position);
    }

    Object sparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position);

    default Object sparkSqlRowValue(CqlField cqlField, Row row, int position)
    {
        return sparkSqlRowValue(cqlField.type(), row, position);
    }

    Object sparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position);

    default DataType sparkSqlType(CqlField.CqlType cqlType)
    {
        return sparkSqlType(cqlType, BigNumberConfig.DEFAULT);
    }

    SparkType toSparkType(CqlField.CqlType cqlType);

    default DataType sparkSqlType(CqlField cqlField, BigNumberConfig bigNumberConfig)
    {
        return sparkSqlType(cqlField.type(), bigNumberConfig);
    }

    DataType sparkSqlType(CqlField.CqlType cqlType, BigNumberConfig bigNumberConfig);

    default Object toTestRowType(CqlField cqlField, Object value)
    {
        return toTestRowType(cqlField.type(), value);
    }

    default Object toTestRowType(CqlField.CqlType cqlType, Object value)
    {
        return toSparkType(cqlType).toTestRowType(value);
    }
}
