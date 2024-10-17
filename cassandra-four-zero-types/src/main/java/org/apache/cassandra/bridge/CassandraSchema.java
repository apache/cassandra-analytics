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
import org.apache.cassandra.cql3.CQLFragmentParser;
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
import org.jetbrains.annotations.NotNull;

import org.apache.cassandra.cql3.CqlParser;

import org.apache.cassandra.cdc.api.TableIdLookup;
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
        synchronized (Schema.instance)
        {
            updater.accept(Schema.instance);
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
        synchronized (Schema.instance)
        {
            return updater.apply(Schema.instance);
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
        TableMetadata.Builder builder = CQLFragmentParser.parseAny(CqlParser::createTableStatement, createStmt, "CREATE TABLE")
                                                         .keyspace(keyspace)
                                                         .prepare(null)
                                                         .builder(types)
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
        return getTable(Schema.instance, keyspace, table);
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
        return isCdcEnabled(Schema.instance, keyspace, table);
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
        return Schema.instance.getKeyspaces()
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
        updateCdcSchema(Schema.instance, cdcTables, partitioner, tableIdLookup);
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

            LOGGER.info("Schema change detected, updating new table schema keyspace={} table={}", keyspace, cqlTable.table());
            s.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        });
    }

    public static void updateCdcSchema(@NotNull Schema schema,
                                       @NotNull Set<CqlTable> cdcTables,
                                       @NotNull Partitioner partitioner,
                                       @NotNull TableIdLookup tableIdLookup)
    {
        LOGGER.info("Updating CDC schema tables='{}'",
                    cdcTables.stream()
                             .map(t -> String.format("%s.%s", t.keyspace(), t.table()))
                             .collect(Collectors.joining(",")));
        Map<String, Set<String>> cdcEnabledTables = CassandraSchema.cdcEnabledTables(schema);
        for (CqlTable table : cdcTables)
        {
            table.udts().forEach(udt -> CassandraTypesImplementation.INSTANCE.updateUDTs(table.keyspace(), udt));

            UUID tableId = tableIdLookup.lookup(table.keyspace(), table.table());
            if (cdcEnabledTables.containsKey(table.keyspace()) && cdcEnabledTables.get(table.keyspace()).contains(table.table()))
            {
                // table has cdc enabled already, update schema if it has changed
                LOGGER.info("CDC already enabled keyspace={} table={}", table.keyspace(), table.table());
                cdcEnabledTables.get(table.keyspace()).remove(table.table());
                CassandraSchema.maybeUpdateSchema(schema, partitioner, table, tableId, true);
                Preconditions.checkArgument(CassandraSchema.isCdcEnabled(schema, table),
                                            "CDC not enabled for table: " + table.keyspace() + "." + table.table());
                continue;
            }

            if (CassandraSchema.has(schema, table))
            {
                // update schema if changed for existing table
                LOGGER.info("Enabling CDC on existing table keyspace={} table={}", table.keyspace(), table.table());
                CassandraSchema.maybeUpdateSchema(schema, partitioner, table, tableId, true);
                Preconditions.checkArgument(CassandraSchema.isCdcEnabled(schema, table),
                                            "CDC not enabled for table: " + table.keyspace() + "." + table.table());
                continue;
            }

            // new table so initialize table with cdc = true
            LOGGER.info("Adding new CDC enabled table keyspace={} table={}", table.keyspace(), table.table());
            new SchemaBuilder(table, partitioner, tableId, true);
            if (tableId != null)
            {
                // verify TableMetadata and ColumnFamilyStore initialized in Schema
                TableId tableIdAfter = TableId.fromUUID(tableId);
                Preconditions.checkNotNull(schema.getTableMetadata(tableIdAfter), "Table not initialized in the schema");
                Preconditions.checkArgument(Objects.requireNonNull(schema.getKeyspaceInstance(table.keyspace())).hasColumnFamilyStore(tableIdAfter),
                                            "ColumnFamilyStore not initialized in the schema");
                Preconditions.checkArgument(CassandraSchema.isCdcEnabled(schema, table),
                                            "CDC not enabled for table: " + table.keyspace() + "." + table.table());
            }
        }
        // existing table no longer with cdc = true, so disable
        cdcEnabledTables.forEach((ks, tables) -> tables.forEach(table -> {
            LOGGER.warn("Disabling CDC on table keyspace={} table={}", ks, table);
            CassandraSchema.disableCdc(schema, ks, table);
        }));
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
            s.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        });
    }

    private static IllegalStateException notExistThrowable(String keyspace, String table)
    {
        return new IllegalStateException("Keyspace/table doesn't exist: " + keyspace + "/" + table);
    }
}
