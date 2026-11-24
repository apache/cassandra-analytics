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

/**
 * Converts column values, in the form that the {@link SSTableWriter} of {@link CassandraBridge#getSSTableWriter}
 * takes them, to values that the writer stores in the same way as Cassandra stores the CQL literals of the values.
 * <p>
 * Cassandra sorts the elements of a set and the entries of a map that are bound as a whole column value. Depending
 * on the version, it does not sort those of a set or map nested in a bound user-defined type, tuple, collection or
 * vector, which the writer would otherwise store in the order they iterate in. Queries by such a value would then
 * miss the row, as Cassandra looks it up by the sorted value.
 * <p>
 * Converted values also suit the codec that the writer uses for a column in descending clustering order, which
 * from Cassandra 5.0 on cannot serialize a user-defined type value, nor a collection of tuple or user-defined
 * type values.
 * <p>
 * Implementations are thread-safe.
 */
public interface CanonicalValueConverter
{
    /**
     * @param columnName name of a column of the table
     * @param value      value of the column, in the form that the SSTable writer takes it; it is not modified
     * @return the value to give the SSTable writer, which is {@code value} itself when it needs no conversion
     * @throws IllegalArgumentException if {@code value} cannot be serialized as a value of the column
     */
    Object convert(String columnName, Object value);
}
