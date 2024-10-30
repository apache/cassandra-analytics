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

package org.apache.cassandra.cdc.api;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cdc.msg.Value;

public interface CassandraSource
{
    CassandraSource DEFAULT = (keySpace, table, columnsToFetch, primaryKeyColumns) -> null;

    /**
     * Read values from Cassandra, instead of using the values from the commit log.
     * For now this is only necessary for unfrozen lists as the index serialized in the mutation is a timeuuid and not intelligible to downstream consumers.
     *
     * @param keyspace          name of the keyspace
     * @param table             name of the Table
     * @param columnsToFetch    lis of columns to fetch
     * @param primaryKeyColumns primary key columns to locate the row
     * @return list of values read from cassandra and the size should be the same as columnsToFetch. This method could
     * return null when read from cassandra fails.
     */
    List<ByteBuffer> readFromCassandra(String keyspace,
                                       String table,
                                       List<String> columnsToFetch,
                                       List<Value> primaryKeyColumns);
}
