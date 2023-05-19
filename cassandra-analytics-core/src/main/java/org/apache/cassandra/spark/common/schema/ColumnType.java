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

public interface ColumnType<T> extends Serializable
{
    /**
     * Parses a value of this type from buffer. Value will be parsed from current position of the buffer. After
     * completion of the function, position will be moved by "length" bytes.
     *
     * @param buffer Buffer to parse column from
     * @param length Serialized value size in buffer is as big as length
     * @return value as Java type
     */
    T parseColumn(ByteBuffer buffer, int length);

    /**
     * Default value, in case column value doesn't exist for CQL row
     *
     * @return the default value for the column type
     */
    T getDefault();

    /**
     * Serialize into ByteBuffer and keeps the position at beginning of ByteBuffer
     *
     * @param value the value to serialize
     * @return A ByteBuffer containing the serialized value
     */
    ByteBuffer serialize(T value);
}
