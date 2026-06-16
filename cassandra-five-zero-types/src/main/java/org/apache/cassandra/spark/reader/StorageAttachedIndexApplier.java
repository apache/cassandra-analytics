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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.bridge.SchemaUpdater;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.spark.utils.CqlUtils;

/**
 * Registers Storage Attached Index (SAI) definitions onto a table's metadata in the in-JVM Cassandra schema.
 * <p>
 * SAI is a Cassandra 5.0+ feature, so this is invoked only by the 5.0 bridge, after the shared {@link SchemaBuilder}
 * has registered the (index-less) table. Idempotent: a no-op if the registered table already carries indexes (the
 * shared builder preserves previously-registered indexes across repeated index-less rebuilds within a JVM).
 */
public final class StorageAttachedIndexApplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageAttachedIndexApplier.class);

    private StorageAttachedIndexApplier()
    {
    }

    /**
     * Applies any Storage Attached Index definitions in {@code indexStatements} to the already-registered table.
     * Non-SAI (legacy 2i) and empty index sets are ignored. Idempotent: if the registered table already carries
     * indexes (e.g. a prior call within the same JVM applied them), this is a no-op.
     *
     * @param keyspace        the keyspace of the table
     * @param table           the table name
     * @param indexStatements the CREATE INDEX statements associated with the table (may be empty or null)
     */
    public static void maybeApply(String keyspace, String table, Set<String> indexStatements)
    {
        if (indexStatements == null || indexStatements.isEmpty())
        {
            return;
        }

        List<String> saiStatements = indexStatements.stream()
                                                    .filter(CqlUtils::isSaiIndex)
                                                    .collect(Collectors.toList());
        if (saiStatements.isEmpty())
        {
            return;
        }

        CassandraSchema.update(schema -> {
            TableMetadata current = schema.getTableMetadata(keyspace, table);
            if (current == null)
            {
                throw new IllegalStateException("SAI index application found no registered table metadata for "
                                                + keyspace + '.' + table);
            }

            if (!current.indexes.isEmpty())
            {
                // Indexes already registered for this table (buildSchema is invoked repeatedly within a JVM).
                return;
            }

            KeyspaceMetadata keyspaceMetadata = schema.getKeyspaceMetadata(keyspace);
            Keyspaces keyspaces = Keyspaces.of(keyspaceMetadata.withSwapped(Tables.of(current)));
            ClientState state = ClientState.forInternalCalls();
            for (String saiStatement : saiStatements)
            {
                CreateIndexStatement.Raw raw = CQLFragmentParser.parseAny(CqlParser::createIndexStatement,
                                                                          saiStatement, "CREATE INDEX");
                keyspaces = raw.prepare(state).apply(keyspaces);
            }

            TableMetadata withIndexes = keyspaces.get(keyspace)
                                                 .flatMap(ks -> ks.tables.get(table))
                                                 .orElseThrow(() -> new IllegalStateException(
                                                         "SAI index application produced no table metadata for "
                                                         + keyspace + '.' + table));
            SchemaUpdater.updateTable(schema, keyspaceMetadata, withIndexes);
            LOGGER.info("Applied {} SAI index(es) to table metadata keyspace={} table={}",
                        saiStatements.size(), keyspace, table);
        });
    }
}
