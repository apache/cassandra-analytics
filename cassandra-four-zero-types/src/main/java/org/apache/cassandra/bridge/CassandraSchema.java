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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaBuilder;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;

public final class CassandraSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSchema.class);

    private CassandraSchema()
    {
        throw new IllegalStateException("Do not instantiate!");
    }

    /**
     * Update cassandra schema with synchronization
     *
     * @param updater updates schema
     */
    public static void update(Consumer<Schema> updater)
    {
        Schema schema = SchemaVersionApi.schemaInstance();
        synchronized (schema)
        {
            updater.accept(schema);
        }
    }

    /**
     * Update cassandra schema and return a result with synchronization
     *
     * @param <T> type of the returned value
     * @param updater updates schema and return a result
     * @return a new value depending on the updater
     */
    public static <T> T apply(Function<Schema, T> updater)
    {
        Schema schema = SchemaVersionApi.schemaInstance();
        synchronized (schema)
        {
            return updater.apply(schema);
        }
    }

    public static Types buildTypes(String keyspace,
                                   Set<String> udtStmts)
    {
        List<CreateTypeStatement.Raw> typeStatements = new ArrayList<>(udtStmts.size());
        for (String udt : udtStmts)
        {
            try
            {
                typeStatements.add((CreateTypeStatement.Raw) CQLFragmentParser.parseAnyUnhandled(CqlParser::query, udt));
            }
            catch (RecognitionException e)
            {
                LOGGER.error("Failed to parse type expression '{}'", udt);
                throw new IllegalStateException(e);
            }
        }
        Types.RawBuilder typesBuilder = Types.rawBuilder(keyspace);
        for (CreateTypeStatement.Raw st : typeStatements)
        {
            st.addToRawBuilder(typesBuilder);
        }
        return typesBuilder.build();
    }

    public static TableMetadata buildTableMetadata(String keyspace,
                                                   String createStmt,
                                                   Types types,
                                                   Partitioner partitioner,
                                                   @Nullable UUID tableId,
                                                   boolean enableCdc)
    {
        CreateTableStatement.Raw createTable = CQLFragmentParser.parseAny(CqlParser::createTableStatement, createStmt, "CREATE TABLE");
        TableMetadata.Builder builder = SchemaVersionApi.tableMetadataBuilder(createTable, keyspace, types)
                                                        .partitioner(CassandraTypesImplementation.getPartitioner(partitioner));

        if (tableId != null)
        {
            builder.id(TableId.fromUUID(tableId));
        }

        TableMetadata tableMetadata = builder.build();
        if (tableMetadata.params.cdc == enableCdc)
        {
            return tableMetadata;
        }
        else
        {
            return tableMetadata.unbuild()
                                .params(tableMetadata.params.unbuild()
                                                            .cdc(enableCdc)
                                                            .build())
                                .build();
        }
    }

    public static boolean keyspaceExists(Schema schema, String keyspace)
    {
        return getKeyspace(schema, keyspace).isPresent();
    }

    public static boolean tableExists(Schema schema, String keyspace, String table)
    {
        return getTable(schema, keyspace, table).isPresent();
    }

    public static Optional<Keyspace> getKeyspace(Schema schema, String keyspace)
    {
        return Optional.ofNullable(schema.getKeyspaceInstance(keyspace));
    }

    public static Optional<KeyspaceMetadata> getKeyspaceMetadata(Schema schema, String keyspace)
    {
        return getKeyspace(schema, keyspace).map(Keyspace::getMetadata);
    }

    public static Optional<TableMetadata> getTable(String keyspace, String table)
    {
        return getTable(SchemaVersionApi.schemaInstance(), keyspace, table);
    }

    public static Optional<TableMetadata> getTable(Schema schema, String keyspace, String table)
    {
        return Optional.ofNullable(schema.getTableMetadata(keyspace, table));
    }

    public static boolean has(Schema schema, CqlTable cqlTable)
    {
        return has(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean has(Schema schema, String keyspace, String table)
    {
        return keyspaceExists(schema, keyspace) && tableExists(schema, keyspace, table);
    }

    // cdc

    public static boolean isCdcEnabled(Schema schema, CqlTable cqlTable)
    {
        return isCdcEnabled(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean isCdcEnabled(String keyspace, String table)
    {
        return isCdcEnabled(SchemaVersionApi.schemaInstance(), keyspace, table);
    }

    public static boolean isCdcEnabled(Schema schema, String keyspace, String table)
    {
        KeyspaceMetadata ks = schema.getKeyspaceMetadata(keyspace);
        if (ks == null)
        {
            return false;
        }
        TableMetadata tb = ks.getTableOrViewNullable(table);
        return tb != null && tb.params.cdc;
    }

    // maps keyspace -> set of table names
    public static Map<String, Set<String>> cdcEnabledTables(Schema schema)
    {
        return new SchemaBridge(schema)
               .getKeyspaces()
               .stream()
               .collect(Collectors.toMap(Function.identity(),
                                         keyspace -> cdcEnabledTables(schema, keyspace)));
    }

    public static Set<String> cdcEnabledTables(Schema schema, String keyspace)
    {
        return Objects.requireNonNull(schema.getKeyspaceMetadata(keyspace))
               .tables.stream()
                      .filter(t -> t.params.cdc)
                      .map(f -> f.name)
                      .collect(Collectors.toSet());
    }

    public static void updateCdcSchema(@NotNull Set<CqlTable> cdcTables,
                                       @NotNull Partitioner partitioner,
                                       @NotNull TableIdLookup tableIdLookup)
    {
        updateCdcSchema(SchemaVersionApi.schemaInstance(), cdcTables, partitioner, tableIdLookup);
    }

    public static void maybeUpdateSchema(Schema schema,
                                         Partitioner partitioner,
                                         CqlTable cqlTable,
                                         @Nullable UUID tableId,
                                         boolean enableCdc)
    {
        String keyspace = cqlTable.keyspace();
        String table = cqlTable.table();
        Optional<TableMetadata> currTable = getTable(schema, keyspace, table);
        if (!currTable.isPresent())
        {
            throw notExistThrowable(keyspace, table);
        }

        Set<String> udts = cqlTable.udts()
                                   .stream()
                                   .map(f -> f.createStatement(CassandraTypesImplementation.INSTANCE, keyspace))
                                   .collect(Collectors.toSet());
        TableMetadata updatedTable = buildTableMetadata(keyspace,
                                                        cqlTable.createStatement(),
                                                        buildTypes(keyspace, udts),
                                                        partitioner,
                                                        tableId != null ? tableId : currTable.get().id.asUUID(),
                                                        enableCdc);
        if (updatedTable.equals(currTable.get()))
        {
            // no changes
            return;
        }

        update(s -> {
            Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(s, keyspace);
            Optional<TableMetadata> tableOpt = getTable(s, keyspace, table);
            if (!ks.isPresent() || !tableOpt.isPresent())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (updatedTable.equals(tableOpt.get()))
            {
                // no changes
                return;
            }

            LOGGER.info("Schema change detected, updating table schema keyspace={} table={} cdc={}", keyspace, cqlTable.table(), enableCdc);
            SchemaUpdater.updateTable(s, ks.get(), updatedTable);
        });
    }

    public static void updateCdcSchema(@NotNull Schema schema,
                                       @NotNull Set<CqlTable> cdcTables,
                                       @NotNull Partitioner partitioner,
                                       @NotNull TableIdLookup tableIdLookup)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Updating CDC schema tables='{}'",
                    cdcTables.stream()
                             .map(t -> String.format("%s.%s", t.keyspace(), t.table()))
                             .collect(Collectors.joining(",")));
        }

        Set<TableIdentifier> currentlyCdcEnabled = currentlyCdcEnabledTables(schema);

        for (CqlTable table : cdcTables)
        {
            table.udts().forEach(udt -> CassandraTypesImplementation.INSTANCE.updateUDTs(table.keyspace(), udt));

            UUID tableId = tableIdLookup.lookup(table.keyspace(), table.table());
            boolean previouslyCdcEnabled = currentlyCdcEnabled.contains(TableIdentifier.of(table.keyspace(), table.table()));
            if (previouslyCdcEnabled)
            {
                // maybeUpdateSchema logs on its own when it actually performs an update.
                CassandraSchema.maybeUpdateSchema(schema, partitioner, table, tableId, table.cdc());
            }
            else if (CassandraSchema.has(schema, table))
            {
                // table exists but wasn't tracked as cdc-enabled (e.g. a non-CDC table the
                // caller included in cdcTables anyway) — update if schema changed.
                CassandraSchema.maybeUpdateSchema(schema, partitioner, table, tableId, table.cdc());
            }
            else
            {
                // new table — register with the CDC flag from the create statement
                LOGGER.info("Registering new table keyspace={} table={} cdc={}", table.keyspace(), table.table(), table.cdc());
                new SchemaBuilder(table, partitioner, tableId, table.cdc());
                if (tableId != null && table.cdc())
                {
                    // verify CDC-enabled tables are correctly initialized
                    TableId tableIdAfter = TableId.fromUUID(tableId);
                    Preconditions.checkNotNull(schema.getTableMetadata(tableIdAfter), "Table not initialized in the schema");
                    Preconditions.checkArgument(Objects.requireNonNull(schema.getKeyspaceInstance(table.keyspace())).hasColumnFamilyStore(tableIdAfter),
                                                "ColumnFamilyStore not initialized in the schema");
                    Preconditions.checkArgument(CassandraSchema.isCdcEnabled(schema, table),
                                                "CDC not enabled for table: " + table.keyspace() + "." + table.table());
                }
            }
        }
        disableCdcOnStaleTables(schema, currentlyCdcEnabled, cdcTables);
    }

    private static Set<TableIdentifier> currentlyCdcEnabledTables(Schema schema)
    {
        return CassandraSchema.cdcEnabledTables(schema)
                              .entrySet()
                              .stream()
                              .flatMap(e -> e.getValue().stream().map(table -> TableIdentifier.of(e.getKey(), table)))
                              .collect(Collectors.toSet());
    }

    /**
     * Disables CDC on every table in {@code currentlyCdcEnabled} that is not CDC-enabled in
     * {@code cdcTables} (dropped, or CDC disabled in its CREATE TABLE).
     */
    private static void disableCdcOnStaleTables(Schema schema, Set<TableIdentifier> currentlyCdcEnabled, Set<CqlTable> cdcTables)
    {
        Set<TableIdentifier> stillCdcEnabled = cdcTables.stream()
                                                        .filter(CqlTable::cdc)
                                                        .map(t -> TableIdentifier.of(t.keyspace(), t.table()))
                                                        .collect(Collectors.toSet());
        Set<TableIdentifier> stale = new HashSet<>(currentlyCdcEnabled);
        stale.removeAll(stillCdcEnabled);

        stale.forEach(id -> {
            LOGGER.warn("Disabling CDC on table keyspace={} table={}", id.keyspace(), id.table());
            CassandraSchema.disableCdc(schema, id.keyspace(), id.table());
        });
    }

    /**
     * Removes tables from {@code Schema.instance} that were previously registered via
     * {@link #updateCdcSchema} but are no longer needed — e.g. a non-CDC table that no longer
     * shares partition-key structure with any CDC-enabled table in its keyspace after a schema
     * change. See {@code CdcBridge#unregisterNonCdcTables} for the full rationale.
     *
     * <p>Idempotent: a table not currently registered is silently skipped. Refuses (skips, with
     * a warning) to unregister any table that is currently CDC-enabled — the caller is
     * responsible for only requesting removal of tables it has determined are safe, but this is
     * a last-line defense against silently dropping schema CDC still needs.
     *
     * @param tables the tables to unregister
     */
    public static void unregisterNonCdcTables(@NotNull Set<TableIdentifier> tables)
    {
        unregisterNonCdcTables(SchemaVersionApi.schemaInstance(), tables);
    }

    public static void unregisterNonCdcTables(@NotNull Schema schema, @NotNull Set<TableIdentifier> tables)
    {
        for (TableIdentifier id : tables)
        {
            String keyspace = id.keyspace();
            String table = id.table();
            try
            {
                unregisterNonCdcTable(schema, keyspace, table);
            }
            catch (RuntimeException e)
            {
                // Don't let one bad table abort unregistration of the rest of the batch.
                LOGGER.warn("Failed to unregister table keyspace={} table={}", keyspace, table, e);
            }
        }
    }

    private static void unregisterNonCdcTable(@NotNull Schema schema, @NotNull String keyspace, @NotNull String table)
    {
        Optional<TableMetadata> tableMetadata = getTable(schema, keyspace, table);
        if (!tableMetadata.isPresent())
        {
            // already unregistered (or never was) — nothing to do
            return;
        }

        if (tableMetadata.get().params.cdc)
        {
            LOGGER.warn("Refusing to unregister CDC-enabled table keyspace={} table={}", keyspace, table);
            return;
        }

        update(s -> {
            Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(s, keyspace);
            Optional<TableMetadata> tableOpt = getTable(s, keyspace, table);
            if (!ks.isPresent() || !tableOpt.isPresent())
            {
                // unregistered by a concurrent call, or keyspace itself is gone
                return;
            }
            if (tableOpt.get().params.cdc)
            {
                // became CDC-enabled since the check above (race with a concurrent
                // updateCdcSchema) — do not remove it
                return;
            }

            LOGGER.info("Unregistering table no longer at risk of a batch with a CDC-enabled table keyspace={} table={}", keyspace, table);
            // Only remove the table's schema metadata (so it goes back to throwing
            // UnknownTableException on deserialization) — this bridge never performs real
            // writes/compactions on the tables it mirrors, so a full Keyspace.dropCf() would
            // pull in unrelated production machinery (e.g. lazily initializing
            // CompactionManager's thread pools) with no corresponding benefit here.
            SchemaUpdater.load(s, ks.get().withSwapped(ks.get().tables.without(table)));
        });
    }

    public static void enableCdc(Schema schema, CqlTable cqlTable)
    {
        enableCdc(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static void enableCdc(Schema schema,
                                 String keyspace,
                                 String table)
    {
        updateCdc(schema, keyspace, table, true);
    }

    public static void disableCdc(Schema schema, CqlTable cqlTable)
    {
        disableCdc(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static void disableCdc(Schema schema,
                                  String keyspace,
                                  String table)
    {
        updateCdc(schema, keyspace, table, false);
    }

    public static void updateCdc(Schema schema,
                                 String keyspace,
                                 String table,
                                 boolean enableCdc)
    {
        if (!has(schema, keyspace, table))
        {
            throw new IllegalArgumentException("Keyspace/table not initialized: " + keyspace + "/" + table);
        }

        Optional<TableMetadata> tb = getTable(schema, keyspace, table);
        if (!tb.isPresent())
        {
            throw notExistThrowable(keyspace, table);
        }
        if (tb.get().params.cdc == enableCdc)
        {
            // nothing to update
            return;
        }

        update(s -> {
            Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(s, keyspace);
            Optional<TableMetadata> tableOpt = getTable(s, keyspace, table);
            if (!ks.isPresent() || !tableOpt.isPresent())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (tableOpt.get().params.cdc == enableCdc)
            {
                // nothing to update
                return;
            }

            TableMetadata updatedTable = tableOpt.get().unbuild()
                                                 .params(tableOpt.get().params.unbuild().cdc(enableCdc).build())
                                                 .build();

            LOGGER.info("{} CDC for table keyspace={} table={}",
                        updatedTable.params.cdc ? "Enabling" : "Disabling", keyspace, table);
            SchemaUpdater.updateTable(s, ks.get(), updatedTable);
        });
    }

    private static IllegalStateException notExistThrowable(String keyspace, String table)
    {
        return new IllegalStateException("Keyspace/table doesn't exist: " + keyspace + "/" + table);
    }
}
