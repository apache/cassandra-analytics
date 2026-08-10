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

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tools.JsonTransformer;

/**
 * Facade for the handful of {@code cassandra-all} calls whose shape or behavior differs across Cassandra
 * distributions. The call sites live in the shared {@code cassandra-four-zero-types} classes
 * ({@code CassandraSchema}, {@code AbstractSchemaBuilder}, {@code CqlUdt}) and the bridge
 * ({@code CassandraBridgeImplementation}); they invoke the {@code static} entry points below, which delegate
 * to a registered {@link #instance}.
 *
 * <p>This class is the default (Apache C* 4.0/5.0) implementation — its bodies inline the expressions the call
 * sites previously used inline. A distribution whose {@code cassandra-all} API differs registers a subclass
 * via {@link #setInstance(SchemaVersionApi)} that overrides only the divergent {@code do*} methods, so the
 * large shared classes are consumed unchanged (no same-FQN overlay, no drift gate).
 */
public class SchemaVersionApi
{
    private static volatile SchemaVersionApi instance = new SchemaVersionApi();

    protected SchemaVersionApi()
    {
    }

    /**
     * Registers the distribution-specific implementation. Called once, before any schema access, from the
     * bridge's client initialization.
     */
    public static void setInstance(SchemaVersionApi impl)
    {
        instance = impl;
    }

    // ------------------------------------------------------------------------------------------------------
    // Static entry points (call sites use these) -> delegate to the registered instance.
    // ------------------------------------------------------------------------------------------------------

    public static Schema schemaInstance()
    {
        return instance.doSchemaInstance();
    }

    public static void openKeyspaceInstance(String keyspaceName)
    {
        instance.doOpenKeyspaceInstance(keyspaceName);
    }

    public static void reopenKeyspaceInstance(String keyspaceName)
    {
        instance.doReopenKeyspaceInstance(keyspaceName);
    }

    public static void initColumnFamily(Schema schema, String keyspaceName, TableMetadata table)
    {
        instance.doInitColumnFamily(schema, keyspaceName, table);
    }

    public static TableMetadata.Builder tableMetadataBuilder(CreateTableStatement.Raw createTable,
                                                             String keyspace,
                                                             Types types)
    {
        return instance.doTableMetadataBuilder(createTable, keyspace, types);
    }

    public static void writeSSTableJson(ISSTableScanner scanner,
                                        Stream<UnfilteredRowIterator> partitions,
                                        TableMetadataRef metadata,
                                        OutputStream output) throws IOException
    {
        instance.doWriteSSTableJson(scanner, partitions, metadata, output);
    }

    // ------------------------------------------------------------------------------------------------------
    // Overridable defaults (Apache C* behavior).
    // ------------------------------------------------------------------------------------------------------

    /**
     * Returns the active schema. Always return the same instance on every call: callers lock on this object,
     * so handing back a new one each time would break that locking.
     */
    protected Schema doSchemaInstance()
    {
        return Schema.instance;
    }

    /** Ensures the keyspace's runtime instance exists in the schema. */
    protected void doOpenKeyspaceInstance(String keyspaceName)
    {
        Keyspace.openWithoutSSTables(keyspaceName);
    }

    /**
     * Re-asserts the keyspace's runtime instance immediately before post-build validation. A no-op by default;
     * a distribution whose schema mutations can transiently clear keyspace instances overrides this.
     */
    protected void doReopenKeyspaceInstance(String keyspaceName)
    {
        // no-op by default
    }

    /** Initializes the column-family store for a table whose keyspace instance is already open. */
    protected void doInitColumnFamily(Schema schema, String keyspaceName, TableMetadata table)
    {
        schema.getKeyspaceInstance(keyspaceName)
              .initCf(TableMetadataRef.forOfflineTools(table), false);
    }

    /** Prepares a CREATE TABLE statement and returns its {@link TableMetadata.Builder}. */
    protected TableMetadata.Builder doTableMetadataBuilder(CreateTableStatement.Raw createTable,
                                                           String keyspace,
                                                           Types types)
    {
        return createTable.keyspace(keyspace).prepare(null).builder(types);
    }

    /** Serializes the given SSTable {@code partitions} to JSON on {@code output}. */
    protected void doWriteSSTableJson(ISSTableScanner scanner,
                                      Stream<UnfilteredRowIterator> partitions,
                                      TableMetadataRef metadata,
                                      OutputStream output) throws IOException
    {
        JsonTransformer.toJson(scanner, partitions, false, metadata.get(), output);
    }
}
