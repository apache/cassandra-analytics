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

/**
 * Generic interface for mapping Cassandra CQL types to target-specific type objects.
 * This abstraction allows different implementations (e.g., Spark, other analytics engines)
 * without exposing implementation-specific dependencies in the public API.
 */
public interface TypeMapper
{
    /**
     * Maps a Cassandra CQL type to the target type system representation.
     *
     * @param cqlType Cassandra CQL type
     * @return target type representation (implementation-specific)
     */
    Object mapType(CqlField.CqlType cqlType);

    /**
     * Maps a Cassandra CQL field to the target type system representation.
     *
     * @param field Cassandra CQL field
     * @return target type representation (implementation-specific)
     */
    default Object mapType(CqlField field)
    {
        return mapType(field.type());
    }
}
