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

package org.apache.cassandra.spark.config;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Defines the properties of schema features. It requires to be implemented as enum
 */
public interface SchemaFeature
{
    /**
     * The {@link DataType} of the field
     *
     * @return DataType
     */
    DataType fieldDataType();

    /**
     * Generate a dynamic {@link DataType} based on {@link CqlTable} and the {@link StructType} Spark schema.
     * If a feature has a fixed {@link DataType}, the method does not need to be overridden.
     *
     * @param table    the CQL table schema
     * @param sparkSchema the Spark schema
     */
    default void generateDataType(CqlTable table, StructType sparkSchema)
    {
        // Do nothing
    }

    /**
     * Decorate the Spark row builder according to the feature
     *
     * @param builder the row builder
     * @return a new decorated builder
     */
    <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder);

    /**
     * The option name used in the Spark options
     *
     * @return option name
     */
    default String optionName()
    {
        return fieldName();
    }

    /**
     * The Spark {@link StructField} according to the feature
     *
     * @return struct field
     */
    default StructField field()
    {
        return new StructField(fieldName(), fieldDataType(), fieldNullable(), fieldMetadata());
    }

    /**
     * The name of the field
     *
     * @return field name
     */
    default String fieldName()
    {
        Preconditions.checkState(this instanceof Enum<?>, "Only implement this interface in enum");
        Enum<?> e = (Enum<?>) this;
        return e.name().toLowerCase();
    }

    /**
     * Define if the field is nullable
     *
     * @return true, if the field is nullable
     */
    default boolean fieldNullable()
    {
        return true;
    }

    /**
     * The metadata used for the field
     *
     * @return metadata
     */
    default Metadata fieldMetadata()
    {
        return Metadata.empty();
    }
}
