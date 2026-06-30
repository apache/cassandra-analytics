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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static java.util.Collections.emptySet;
import static org.apache.cassandra.spark.reader.SchemaBuilder.rfToMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link StorageAttachedIndexApplier}, exercising the in-JVM schema mutation it performs.
 */
public class StorageAttachedIndexApplierTest
{
    @Test
    public void testMaybeApplyRegistersSaiIndexOnRegisteredTable()
    {
        String keyspace = uniqueKeyspace("registers");
        registerIndexlessTable(keyspace, "tbl");
        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.isEmpty()).isTrue();

        StorageAttachedIndexApplier.maybeApply(keyspace, "tbl", ImmutableSet.of(
                "CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace + ".tbl (b) USING 'StorageAttachedIndex';"));

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, "tbl");
        assertThat(metadata.indexes.isEmpty()).isFalse();
        assertThat(metadata.indexes.get("tbl_b_idx")).isPresent();
    }

    @Test
    public void testMaybeApplyIsIdempotent()
    {
        String keyspace = uniqueKeyspace("idempotent");
        registerIndexlessTable(keyspace, "tbl");

        Set<String> sai = ImmutableSet.of(
                "CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace + ".tbl (b) USING 'StorageAttachedIndex';");
        StorageAttachedIndexApplier.maybeApply(keyspace, "tbl", sai);
        // Second call is a no-op because the registered table already carries indexes.
        StorageAttachedIndexApplier.maybeApply(keyspace, "tbl", sai);

        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.get("tbl_b_idx")).isPresent();
    }

    @Test
    public void testMaybeApplyIgnoresNonSaiIndexes()
    {
        String keyspace = uniqueKeyspace("nonsai");
        registerIndexlessTable(keyspace, "tbl");

        // A legacy 2i statement is filtered out, so nothing is applied and the table stays index-less.
        StorageAttachedIndexApplier.maybeApply(keyspace, "tbl", ImmutableSet.of(
                "CREATE INDEX tbl_b_legacy ON " + keyspace + ".tbl (b);"));

        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.isEmpty()).isTrue();
    }

    @Test
    public void testMaybeApplyIsNoOpForNullOrEmpty()
    {
        // Neither requires a registered table; both must return without error.
        StorageAttachedIndexApplier.maybeApply("any_ks", "any_tbl", null);
        StorageAttachedIndexApplier.maybeApply("any_ks", "any_tbl", emptySet());
    }

    @Test
    public void testMaybeApplyThrowsWhenTableNotRegistered()
    {
        String keyspace = uniqueKeyspace("missing");
        assertThatThrownBy(() -> StorageAttachedIndexApplier.maybeApply(keyspace, "absent", ImmutableSet.of(
                "CREATE CUSTOM INDEX absent_idx ON " + keyspace + ".absent (b) USING 'StorageAttachedIndex';")))
                .isInstanceOf(IllegalStateException.class);
    }

    private static String uniqueKeyspace(String suffix)
    {
        return "sai_applier_" + suffix;
    }

    /**
     * Registers a keyspace and an index-less table ({@code (a int PRIMARY KEY, b int)}) into the in-JVM schema,
     * mirroring how the shared {@link SchemaBuilder} registers the table before the 5.0 bridge applies SAI.
     */
    private static void registerIndexlessTable(String keyspace, String table)
    {
        CassandraBridgeImplementation.setup();
        ReplicationFactor replicationFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                    ImmutableMap.of("replication_factor", 1));
        KeyspaceMetadata keyspaceMetadata = KeyspaceMetadata.create(keyspace, KeyspaceParams.create(true, rfToMap(replicationFactor)));
        Schema.instance.transform(SchemaTransformations.addKeyspace(keyspaceMetadata, false));
        Keyspace.openWithoutSSTables(keyspace);

        String createTableStatement = "CREATE TABLE " + keyspace + "." + table + " (a int PRIMARY KEY, b int)";
        TableMetadata tableMetadata = CQLFragmentParser
                .parseAny(CqlParser::createTableStatement, createTableStatement, "CREATE TABLE")
                .keyspace(keyspace)
                .prepare(null)
                .builder(Types.none())
                .build();
        KeyspaceMetadata registered = Schema.instance.getKeyspaceMetadata(keyspace);
        Schema.instance.transform(st -> st.withAddedOrUpdated(registered.withSwapped(registered.tables.with(tableMetadata))));
    }
}
