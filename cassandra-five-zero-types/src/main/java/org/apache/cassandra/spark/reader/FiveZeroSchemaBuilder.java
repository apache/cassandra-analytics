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

package org.apache.cassandra.spark.reader;

import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

/**
 * Cassandra 5.0 {@link SchemaBuilder} that adds Storage Attached Index (SAI) support to the shared, version-agnostic
 * builder. All SAI-specific schema logic lives here so the 4.0 builder stays free of it.
 * The 5.0 bridge instantiates this in place of {@link SchemaBuilder}.
 */
public class FiveZeroSchemaBuilder extends SchemaBuilder
{
    public FiveZeroSchemaBuilder(CqlTable table, Partitioner partitioner)
    {
        super(table, partitioner);
    }

    public FiveZeroSchemaBuilder(String createStmt,
                                 String keyspace,
                                 ReplicationFactor replicationFactor,
                                 Partitioner partitioner)
    {
        super(createStmt, keyspace, replicationFactor, partitioner);
    }

    public FiveZeroSchemaBuilder(String createStmt,
                                 String keyspace,
                                 ReplicationFactor replicationFactor,
                                 Partitioner partitioner,
                                 Function<CassandraTypes, Set<String>> udtStatementsProvider,
                                 @Nullable UUID tableId,
                                 Set<String> indexStatements,
                                 boolean enableCdc)
    {
        super(createStmt, keyspace, replicationFactor, partitioner, udtStatementsProvider, tableId, indexStatements, enableCdc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TableMetadata beforeTableRegistered(TableMetadata tableMetadata, @Nullable TableMetadata previousTable)
    {
        // buildSchema runs repeatedly per table within a JVM — the per-partition RecordWriter, the bloom-filter
        // rebuild, and read-path scanners all build the table index-less. If a previous build already registered
        // indexes and this build carries none, carry the previous indexes forward so they are not dropped.
        if (previousTable != null && !previousTable.indexes.isEmpty() && tableMetadata.indexes.isEmpty())
        {
            return tableMetadata.unbuild()
                                .indexes(previousTable.indexes)
                                .build();
        }

        return tableMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTableRegistered(Schema schema, TableMetadata registeredTable)
    {
        // Attach any SAI definitions supplied as CREATE INDEX statements to the just-registered table, within the same
        // atomic schema update. Non-SAI (legacy 2i) and empty statement sets are ignored; idempotent if the table
        // already carries indexes (e.g. preserved by {@link #beforeTableRegistered}).
        StorageAttachedIndexApplier.applyTo(schema, registeredTable.keyspace, registeredTable.name, indexStatements());
    }
}
