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

import java.util.function.Supplier;

import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Cassandra 6.0 replaces {@code Schema.transform} with {@code SchemaProvider.submit}, which commits through
 * Transactional Cluster Metadata. 6.0 also adds {@code compatibleWith(ClusterMetadata)} to
 * {@code SchemaTransformation}, so it is no longer a functional interface and {@link #updateTable} needs an
 * anonymous class; its compatibility check copies the one {@link SchemaTransformations} uses.
 */
public class SchemaUpdater
{
    private SchemaUpdater()
    {
    }

    /**
     * Preserves keyspace instances across schema commits made by an offline SSTable writer.
     * Readers use the same lock to avoid observing a committed schema before its instances are ready.
     */
    public static <T> T withKeyspaceInstances(Supplier<T> action)
    {
        synchronized (Schema.instance)
        {
            ClusterMetadata before = ClusterMetadata.current();
            try
            {
                return action.get();
            }
            finally
            {
                ClusterMetadata after = ClusterMetadata.current();
                if (after.schema != before.schema)
                {
                    after.schema.initializeKeyspaceInstances(before.schema, false);
                }
            }
        }
    }

    /**
     * Commits a schema transformation, then creates the keyspace instances that the transformation adds.
     *
     * <p>A server builds those instances from {@code SchemaListener}, which every commit notifies. An offline
     * caller runs {@code StubClusterMetadataService}, whose {@code commit} notifies no listener, so
     * {@code getKeyspaceInstance} would return null for every keyspace. Cassandra 5.0 needed no such step,
     * because {@code Keyspace.open} created the instance on demand through the removed
     * {@code Schema.maybeAddKeyspaceInstance}. Call what the listener calls, with the listener's own arguments.
     */
    public static ClusterMetadata submit(SchemaProvider schema, SchemaTransformation transformation)
    {
        ClusterMetadata before = ClusterMetadata.current();
        ClusterMetadata after = schema.submit(transformation);
        after.schema.initializeKeyspaceInstances(before.schema, false);
        return after;
    }

    /**
     * Creates the instance for a single keyspace, leaving every other keyspace's instance untouched. Needed when a
     * keyspace has metadata but no instance. Cassandra 6.0's {@code Keyspace.openWithoutSSTables} only reads the
     * instance rather than creating it.
     *
     * <p>Targeting one keyspace avoids rebuilding all of them, which re-registers metrics and can throw
     * stack-trace-filling exceptions. A bulk job builds schema once per keyspace, so this only shows up in tests that
     * open many distinct keyspaces in one JVM.
     */
    public static void openKeyspaceInstance(String keyspaceName)
    {
        DistributedSchema current = ClusterMetadata.current().schema;
        DistributedSchema before = new DistributedSchema(current.getKeyspaces().without(keyspaceName));
        current.initializeKeyspaceInstances(before, false);
    }

    public static void load(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata)
    {
        submit(schema, SchemaTransformations.addKeyspace(keyspaceMetadata, false));
    }

    public static void load(SchemaProvider schema, TableMetadata tableMetadata)
    {
        submit(schema, SchemaTransformations.addTable(tableMetadata, false));
    }

    public static void load(SchemaProvider schema, Types userTypes)
    {
        submit(schema, SchemaTransformations.addTypes(userTypes, true));
    }

    /**
     * Replaces the metadata of an existing keyspace with metadata that holds fewer tables.
     *
     * <p>{@link SchemaTransformations#addKeyspace} only adds, throwing {@code AlreadyExistsException} otherwise, so
     * replacing needs its own transformation. {@link #submit} is also wrong here: it reports the table as altered,
     * which calls {@code Keyspace.dropCf} and initializes {@code CompactionManager}, throwing in client mode where
     * concurrent_compactors is zero. {@link #openKeyspaceInstance} reports the keyspace as created instead, leaving
     * the removed table without a column family store and touching no compaction machinery.
     */
    public static void removeTables(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.submit(replace(keyspaceMetadata));
        openKeyspaceInstance(keyspaceMetadata.name);
    }

    public static void updateTable(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        submit(schema, replace(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata))));
    }

    private static SchemaTransformation replace(KeyspaceMetadata keyspaceMetadata)
    {
        return new SchemaTransformation()
        {
            @Override
            public Keyspaces apply(ClusterMetadata metadata)
            {
                return metadata.schema.getKeyspaces().withAddedOrUpdated(keyspaceMetadata);
            }

            @Override
            public boolean compatibleWith(ClusterMetadata metadata)
            {
                return metadata.directory.commonSerializationVersion.isAtLeast(Version.V0);
            }
        };
    }
}
