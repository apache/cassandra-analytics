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

import java.util.function.UnaryOperator;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;

/**
 * Applies schema mutations for the bridge. The {@code static} entry points ({@code load}/{@code updateTable})
 * are used by the shared {@code cassandra-four-zero-types} schema classes and by the bridge's
 * {@code SSTableTombstoneWriter}; they express every mutation in terms of two primitives that delegate to a
 * registered {@link #instance}:
 * <ul>
 *   <li>{@link #apply(Schema, SchemaTransformation)} — apply a prebuilt {@link SchemaTransformation};</li>
 *   <li>{@link #applyKeyspacesOp(Schema, UnaryOperator)} — apply an in-place edit of the keyspace graph
 *       (its lambda can't be written in shared source because the {@link SchemaTransformation} SAM differs
 *       across distributions).</li>
 * </ul>
 *
 * <p>This class is the default (Apache C* 5.0) implementation, mutating through {@code Schema.transform(...)}.
 * A distribution whose mutation path differs (e.g. one that replaced {@code transform} with a
 * {@code ClusterMetadata}-based submit) registers a subclass via {@link #setInstance(SchemaUpdater)} that
 * overrides the two {@code do*} primitives.
 */
public class SchemaUpdater
{
    private static volatile SchemaUpdater instance = new SchemaUpdater();

    protected SchemaUpdater()
    {
    }

    /** Registers the distribution-specific implementation. */
    public static void setInstance(SchemaUpdater impl)
    {
        instance = impl;
    }

    // ------------------------------------------------------------------------------------------------------
    // Static entry points (call sites use these).
    // ------------------------------------------------------------------------------------------------------

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        apply(schema, SchemaTransformations.addKeyspace(keyspaceMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        apply(schema, SchemaTransformations.addTable(tableMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, Types userTypes)
    {
        apply(schema, SchemaTransformations.addTypes(userTypes, true));
    }

    public static void updateTable(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        applyKeyspacesOp(schema, keyspaces ->
                keyspaces.withAddedOrUpdated(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata))));
    }

    /** Applies a prebuilt {@link SchemaTransformation}. */
    public static void apply(Schema schema, SchemaTransformation transformation)
    {
        instance.doApply(schema, transformation);
    }

    /** Applies an in-place edit of the keyspace graph. */
    public static void applyKeyspacesOp(Schema schema, UnaryOperator<Keyspaces> keyspacesOp)
    {
        instance.doApplyKeyspacesOp(schema, keyspacesOp);
    }

    // ------------------------------------------------------------------------------------------------------
    // Overridable defaults (Apache C* 5.0 behavior).
    // ------------------------------------------------------------------------------------------------------

    protected void doApply(Schema schema, SchemaTransformation transformation)
    {
        schema.transform(transformation);
    }

    protected void doApplyKeyspacesOp(Schema schema, UnaryOperator<Keyspaces> keyspacesOp)
    {
        schema.transform(keyspaces -> keyspacesOp.apply(keyspaces));
    }
}
