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

package org.apache.cassandra.spark.data;

import org.apache.cassandra.bridge.BigNumberConfig;

/**
 * Generic interface for converting Cassandra CQL types to target schema types.
 * This abstraction allows different implementations (e.g., Spark, other analytics engines)
 * without exposing implementation-specific dependencies in the public API.
 */
public interface SchemaConverter
{
    /**
     * Converts a Cassandra CQL field to the target schema type.
     *
     * @param field Cassandra CQL field
     * @param bigNumberConfig configuration for handling big numbers
     * @return target schema type (implementation-specific)
     */
    Object getDataType(CqlField field, BigNumberConfig bigNumberConfig);

    /**
     * Converts a Cassandra CQL type to the target schema type.
     *
     * @param cqlType Cassandra CQL type
     * @param bigNumberConfig configuration for handling big numbers
     * @return target schema type (implementation-specific)
     */
    default Object getDataType(CqlField.CqlType cqlType, BigNumberConfig bigNumberConfig)
    {
        return getDataType(new CqlField(false, false, false, "temp", cqlType, 0), bigNumberConfig);
    }

    /**
     * Converts a Cassandra CQL type to the target schema type using default big number configuration.
     *
     * @param cqlType Cassandra CQL type
     * @return target schema type (implementation-specific)
     */
    default Object getDataType(CqlField.CqlType cqlType)
    {
        return getDataType(cqlType, BigNumberConfig.DEFAULT);
    }
}
