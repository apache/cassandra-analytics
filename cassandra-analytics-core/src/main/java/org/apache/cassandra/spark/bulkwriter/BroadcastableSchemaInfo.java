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

import java.io.Serializable;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

/**
 * Broadcastable wrapper for schema information with ZERO transient fields to optimize Spark broadcasting.
 * <p>
 * Contains BroadcastableTableSchema (pre-computed schema data) and UDT statements.
 * Executors reconstruct CassandraSchemaInfo and TableSchema from these fields.
 * <p>
 * <b>Why ZERO transient fields matters:</b><br>
 * Spark's {@link org.apache.spark.util.SizeEstimator} uses reflection to estimate object sizes before broadcasting.
 * Each transient field forces SizeEstimator to inspect the field's type hierarchy, which is expensive.
 * Logger references are particularly costly due to their deep object graphs (appenders, layouts, contexts).
 * By eliminating ALL transient fields and Logger references, we:
 * <ul>
 *   <li>Minimize SizeEstimator reflection overhead during broadcast preparation</li>
 *   <li>Reduce broadcast variable serialization size</li>
 *   <li>Avoid accidental serialization of non-serializable objects</li>
 * </ul>
 */
public final class BroadcastableSchemaInfo implements Serializable
{
    private static final long serialVersionUID = -8727074052066841748L;

    // Essential fields broadcast to executors
    private final BroadcastableTableSchema broadcastableTableSchema;
    private final Set<String> userDefinedTypeStatements;

    /**
     * Creates a BroadcastableSchemaInfo from a source SchemaInfo.
     * Extracts BroadcastableTableSchema to avoid serializing Logger.
     *
     * @param source the source SchemaInfo (typically CassandraSchemaInfo)
     */
    public static BroadcastableSchemaInfo from(@NotNull SchemaInfo source)
    {
        return new BroadcastableSchemaInfo(
            BroadcastableTableSchema.from(source.getTableSchema()),
            source.getUserDefinedTypeStatements()
        );
    }

    private BroadcastableSchemaInfo(BroadcastableTableSchema broadcastableTableSchema,
                                   Set<String> userDefinedTypeStatements)
    {
        this.broadcastableTableSchema = broadcastableTableSchema;
        this.userDefinedTypeStatements = userDefinedTypeStatements;
    }

    public BroadcastableTableSchema getBroadcastableTableSchema()
    {
        return broadcastableTableSchema;
    }

    @NotNull
    public Set<String> getUserDefinedTypeStatements()
    {
        return userDefinedTypeStatements;
    }
}
