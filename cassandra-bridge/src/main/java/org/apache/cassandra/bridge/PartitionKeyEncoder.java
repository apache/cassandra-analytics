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

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Serializes the partition key of a row in the same way as the {@link SSTableWriter} returned by
 * {@link CassandraBridge#getSSTableWriter}. The bulk writer uses it to compute the token of each row
 * before the row reaches the SSTable writer, so the two must agree byte for byte.
 * <p>
 * Implementations are thread-safe.
 */
public interface PartitionKeyEncoder
{
    /**
     * @param partitionKeyValues the values of the partition key columns, in partition key order, in the form that
     *                           {@link SSTableWriter#addRow} accepts
     * @return the serialized partition key
     * @throws IllegalArgumentException if a value is {@code null}, cannot be serialized for its column, or produces
     *                                  a partition key that Cassandra rejects, such as an empty key or one longer
     *                                  than 65535 bytes
     */
    ByteBuffer encode(List<Object> partitionKeyValues);
}
