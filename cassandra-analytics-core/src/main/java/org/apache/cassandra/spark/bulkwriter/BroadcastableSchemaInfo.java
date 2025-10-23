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

import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * Broadcastable data wrapper for broadcasting with ZERO transient fields.
 * Only essential fields are broadcast; executors reconstruct CassandraSchemaInfo to rebuild TableSchema.
 * NO LOGGER - to avoid logger references in broadcast variable.
 */
public final class BroadcastableSchemaInfo implements Serializable
{
    // Essential fields broadcast to executors
    private final BulkSparkConf conf;
    private final StructType structType;
    private final Set<String> userDefinedTypeStatements;

    /**
     * Creates a BroadcastableSchemaInfo from a source SchemaInfo.
     * Executors will reconstruct CassandraSchemaInfo to rebuild TableSchema without Logger.
     *
     * @param source     the source SchemaInfo (typically CassandraSchemaInfo)
     * @param conf       the BulkSparkConf needed to reconstruct on executors
     * @param structType the DataFrame schema structure needed to reconstruct TableSchema
     */
    public static BroadcastableSchemaInfo from(@NotNull SchemaInfo source,
                                              @NotNull BulkSparkConf conf,
                                              @NotNull StructType structType)
    {
        return new BroadcastableSchemaInfo(
            conf,
            structType,
            source.getUserDefinedTypeStatements()
        );
    }

    private BroadcastableSchemaInfo(BulkSparkConf conf,
                                   StructType structType,
                                   Set<String> userDefinedTypeStatements)
    {
        this.conf = conf;
        this.structType = structType;
        this.userDefinedTypeStatements = userDefinedTypeStatements;
    }

    public BulkSparkConf getConf()
    {
        return conf;
    }

    public StructType getStructType()
    {
        return structType;
    }

    @NotNull
    public Set<String> getUserDefinedTypeStatements()
    {
        return userDefinedTypeStatements;
    }
}
