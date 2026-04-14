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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Set;

import org.jetbrains.annotations.NotNull;

public class CassandraSchemaInfo implements SchemaInfo
{
    private final TableSchema tableSchema;
    private final Set<String> userDefinedTypeStatements;

    public CassandraSchemaInfo(TableSchema tableSchema, Set<String> userDefinedTypeStatements)
    {
        this.tableSchema = tableSchema;
        this.userDefinedTypeStatements = userDefinedTypeStatements;
    }

    /**
     * Reconstruct from BroadcastableSchemaInfo on executor.
     * Reconstructs TableSchema from BroadcastableTableSchema (no Sidecar calls needed).
     *
     * @param broadcastable the broadcastable schema info from broadcast
     */
    public CassandraSchemaInfo(BroadcastableSchemaInfo broadcastable)
    {
        this(new TableSchema(broadcastable.getBroadcastableTableSchema()),
             broadcastable.getUserDefinedTypeStatements());
    }

    @Override
    public TableSchema getTableSchema()
    {
        return tableSchema;
    }

    @Override
    @NotNull
    public Set<String> getUserDefinedTypeStatements()
    {
        return userDefinedTypeStatements;
    }
}
