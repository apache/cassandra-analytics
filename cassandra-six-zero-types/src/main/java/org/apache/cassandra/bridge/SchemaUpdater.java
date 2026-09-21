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

import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
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
     * Creates a keyspace instance for every keyspace of the current cluster metadata. Use this only when a
     * keyspace has metadata but no instance, which happens when code outside this class commits it; Cassandra
     * 6.0's {@code Keyspace.openWithoutSSTables} only reads the instance and no longer creates it.
     *
     * <p>Passing an empty previous schema replaces every existing instance, dropping the column family stores
     * it held, so prefer {@link #submit}, which keeps the instances of each commit.
     */
    public static void openKeyspaceInstances()
    {
        ClusterMetadata.current().schema.initializeKeyspaceInstances(DistributedSchema.empty(), false);
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
     * <p>Cassandra 4.0's {@code Schema.load} added or reloaded, whereas {@link SchemaTransformations#addKeyspace}
     * only adds and otherwise throws {@code AlreadyExistsException}, so a caller that means to replace needs a
     * transformation of its own. {@link #submit} is also the wrong follow-up here: it reports the table in
     * {@code Keyspaces.diff().altered}, which makes {@code DistributedSchema.initializeKeyspaceInstances} call
     * {@code Keyspace.dropCf} and so initialize {@code CompactionManager}, which throws in client mode where
     * concurrent_compactors is zero. Rebuild the instances from the committed metadata instead, which leaves the
     * removed table without a column family store and touches no compaction machinery.
     */
    public static void removeTables(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.submit(replace(keyspaceMetadata));
        openKeyspaceInstances();
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
