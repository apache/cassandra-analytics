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

/**
 * Interface defining how to map a Cassandra `CqlField.CqlType` to an equivalent `SparkType`.
 */
public interface SparkSqlTypeConverter extends TypeConverter
{
    // Implementations of SparkSqlTypeConverter must be named as such to load dynamically using the {@link CassandraBridgeFactory}
    String IMPLEMENTATION_FQCN = "org.apache.cassandra.spark.data.converter.SparkSqlTypeConverterImplementation";

    /**
     * @param cqlField Cassandra CQL field
     * @param row      SparkSQL `org.apache.spark.sql.catalyst.expressions.GenericInternalRow` row
     * @param position column position in the row.
     * @return the SparkSQL value at position in the row
     */
    default Object sparkSqlRowValue(CqlField cqlField, GenericInternalRow row, int position)
    {
        return sparkSqlRowValue(cqlField.type(), row, position);
    }

    /**
     * @param cqlType  Cassandra CQL type
     * @param row      SparkSQL `org.apache.spark.sql.catalyst.expressions.GenericInternalRow` row
     * @param position column position in the row.
     * @return the SparkSQL value at position in the row
     */
    Object sparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position);

    /**
     * @param cqlField Cassandra CQL field
     * @param row      SparkSQL `org.apache.spark.sql.Row` row
     * @param position column position in the row.
     * @return the SparkSQL value at position in the row
     */
    default Object sparkSqlRowValue(CqlField cqlField, Row row, int position)
    {
        return sparkSqlRowValue(cqlField.type(), row, position);
    }

    /**
     * @param cqlType  Cassandra CQL type
     * @param row      SparkSQL `org.apache.spark.sql.Row` row
     * @param position column position in the row.
     * @return the SparkSQL value at position in the row
     */
    Object sparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position);

    /**
     * Maps Cassandra CQL type to equivalent SparkType object.
     *
     * @param cqlType Cassandra CQL type
     * @return equivalent Spark `org.apache.cassandra.spark.data.converter.types.SparkType`
     */
    SparkType toSparkType(CqlField.CqlType cqlType);

    /**
     * Maps Cassandra CQL type to equivalent Spark DataType object.
     *
     * @param cqlType Cassandra CQL type
     * @return equivalent Spark `org.apache.spark.sql.types.DataType`
     */
    default DataType sparkSqlType(CqlField.CqlType cqlType)
    {
        return sparkSqlType(cqlType, BigNumberConfig.DEFAULT);
    }

    /**
     * Maps Cassandra CQL type to equivalent Spark DataType object.
     *
     * @param cqlField        Cassandra CQL field
     * @param bigNumberConfig BigNumberConfig if
     * @return equivalent Spark `org.apache.spark.sql.types.DataType`
     */
    default DataType sparkSqlType(CqlField cqlField, BigNumberConfig bigNumberConfig)
    {
        return sparkSqlType(cqlField.type(), bigNumberConfig);
    }

    /**
     * Maps Cassandra CQL type to equivalent Spark DataType object.
     *
     * @param cqlType         Cassandra CQL type
     * @param bigNumberConfig BigNumberConfig if
     * @return equivalent Spark `org.apache.spark.sql.types.DataType`
     */
    DataType sparkSqlType(CqlField.CqlType cqlType, BigNumberConfig bigNumberConfig);

    /**
     * Maps SparkSQL value back to type used in the test system to verify, used only in the test systsm.
     *
     * @param cqlField Cassandra CQL field
     * @param value    SparkSQL value
     * @return equivalent value used in test system.
     */
    default Object toTestRowType(CqlField cqlField, Object value)
    {
        return toTestRowType(cqlField.type(), value);
    }

    /**
     * Maps SparkSQL value back to type used in the test system to verify, used only in the test systsm.
     *
     * @param cqlType Cassandra CQL type
     * @param value   SparkSQL value
     * @return equivalent value used in test system.
     */
    default Object toTestRowType(CqlField.CqlType cqlType, Object value)
    {
        return toSparkType(cqlType).toTestRowType(value);
    }
}
