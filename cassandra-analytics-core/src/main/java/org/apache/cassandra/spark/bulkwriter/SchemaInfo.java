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

import java.util.Collections;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

/**
 * Provides schema information for bulk write operations.
 * <p>
 * This interface does NOT extend Serializable. SchemaInfo instances are never serialized.
 * For broadcast to executors, {@link BroadcastableSchemaInfo} is used instead, and executors
 * reconstruct SchemaInfo instances (specifically {@link CassandraSchemaInfo}) from the broadcast data.
 */
public interface SchemaInfo
{
    TableSchema getTableSchema();

    @NotNull
    Set<String> getUserDefinedTypeStatements();

    /**
     * Returns the set of CREATE INDEX statements for the table, if any.
     *
     * @return set of CREATE INDEX CQL statements, empty if none
     */
    @NotNull
    default Set<String> getIndexStatements()
    {
        return Collections.emptySet();
    }
}
