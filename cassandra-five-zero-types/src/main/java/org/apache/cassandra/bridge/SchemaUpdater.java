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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;

public class SchemaUpdater
{
    private SchemaUpdater()
    {
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.transform(SchemaTransformations.addKeyspace(keyspaceMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.transform(SchemaTransformations.addTable(tableMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, Types userTypes)
    {
        schema.transform(SchemaTransformations.addTypes(userTypes, true));
    }

    /**
     * Replaces the metadata of an existing keyspace with metadata that holds fewer tables.
     *
     * <p>Cassandra 4.0's {@code Schema.load} added or reloaded, whereas
     * {@link SchemaTransformations#addKeyspace} only adds and otherwise throws
     * {@code AlreadyExistsException}, so a caller that means to replace needs a transformation of its own.
     *
     * <p>{@code Schema.alterKeyspace} gives every dropped table to {@code Keyspace.dropCf}, which interrupts
     * compactions and recycles commit log segments: machinery that a client-mode process never started, and
     * that throws while {@code CompactionManager} initializes with no compaction threads.
     * {@code Keyspace.isInitialized()} gates that work, so clear the flag for the commit and only the metadata
     * changes, which is all the bridge's mirrored schema holds. {@code Schema.reload} drops the table's
     * metadata reference either way, so deserialization throws {@code UnknownTableException} again. The column
     * family store of the removed table stays with the keyspace instance, and {@code Keyspace.initCf} reloads
     * that store if the table returns.
     */
    public static void removeTables(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        // Keyspace.setInitialized and unsetInitialized synchronize on Schema.instance, and so does
        // CassandraSchema.update; hold the same monitor, so no other schema change sees the cleared flag
        synchronized (Schema.instance)
        {
            Keyspace.unsetInitialized();
            try
            {
                schema.transform(st -> st.withAddedOrUpdated(keyspaceMetadata));
            }
            finally
            {
                Keyspace.setInitialized();
            }
        }
    }

    public static void updateTable(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.transform(st -> st.withAddedOrUpdated(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata))));
    }
}
