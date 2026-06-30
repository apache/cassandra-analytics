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
import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

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
    public void testApplyToRegistersSaiIndexOnRegisteredTable()
    {
        String keyspace = uniqueKeyspace("registers");
        registerIndexlessTable(keyspace, "tbl");
        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.isEmpty()).isTrue();

        applyIndexes(keyspace, "tbl", ImmutableSet.of(
                "CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace + ".tbl (b) USING 'StorageAttachedIndex';"));

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, "tbl");
        assertThat(metadata.indexes.isEmpty()).isFalse();
        assertThat(metadata.indexes.get("tbl_b_idx")).isPresent();
    }

    @Test
    public void testApplyToIsIdempotent()
    {
        String keyspace = uniqueKeyspace("idempotent");
        registerIndexlessTable(keyspace, "tbl");

        Set<String> sai = ImmutableSet.of(
                "CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace + ".tbl (b) USING 'StorageAttachedIndex';");
        applyIndexes(keyspace, "tbl", sai);
        // Second call is a no-op because the registered table already carries indexes.
        applyIndexes(keyspace, "tbl", sai);

        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.get("tbl_b_idx")).isPresent();
    }

    @Test
    public void testApplyToIgnoresNonSaiIndexes()
    {
        String keyspace = uniqueKeyspace("nonsai");
        registerIndexlessTable(keyspace, "tbl");

        // A legacy 2i statement is filtered out, so nothing is applied and the table stays index-less.
        applyIndexes(keyspace, "tbl", ImmutableSet.of(
                "CREATE INDEX tbl_b_legacy ON " + keyspace + ".tbl (b);"));

        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.isEmpty()).isTrue();
    }

    @Test
    public void testApplyToIsNoOpForNullOrEmpty()
    {
        // Neither requires a registered table; both must return without error.
        applyIndexes("any_ks", "any_tbl", null);
        applyIndexes("any_ks", "any_tbl", emptySet());
    }

    @Test
    public void testApplyToThrowsWhenTableNotRegistered()
    {
        String keyspace = uniqueKeyspace("missing");
        assertThatThrownBy(() -> applyIndexes(keyspace, "absent", ImmutableSet.of(
                "CREATE CUSTOM INDEX absent_idx ON " + keyspace + ".absent (b) USING 'StorageAttachedIndex';")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testBuildSchemaAttachesSaiIndexAtomically()
    {
        // End-to-end: the 5.0 bridge's buildSchema must register the table already carrying its SAI index,
        // i.e. the index is applied within the same schema update (no index-less window).
        CassandraBridgeImplementation.setup();
        CassandraBridgeImplementation bridge = new CassandraBridgeImplementation();
        String keyspace = uniqueKeyspace("buildschema");
        bridge.buildSchema("CREATE TABLE " + keyspace + ".tbl (a int PRIMARY KEY, b int)",
                           keyspace,
                           new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                 ImmutableMap.of("replication_factor", 1)),
                           Partitioner.Murmur3Partitioner,
                           emptySet(),
                           null,
                           ImmutableSet.of("CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace
                                           + ".tbl (b) USING 'StorageAttachedIndex';"),
                           false);

        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.get("tbl_b_idx")).isPresent();
    }

    @Test
    public void testIndexLessRebuildPreservesPreviouslyRegisteredIndexes()
    {
        CassandraBridgeImplementation.setup();
        CassandraBridgeImplementation bridge = new CassandraBridgeImplementation();
        String keyspace = uniqueKeyspace("preserve");
        String create = "CREATE TABLE " + keyspace + ".tbl (a int PRIMARY KEY, b int)";
        ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                     ImmutableMap.of("replication_factor", 1));

        bridge.buildSchema(create, keyspace, rf, Partitioner.Murmur3Partitioner, emptySet(), null,
                           ImmutableSet.of("CREATE CUSTOM INDEX tbl_b_idx ON " + keyspace
                                           + ".tbl (b) USING 'StorageAttachedIndex';"),
                           false);
        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.get("tbl_b_idx")).isPresent();

        // Rebuild with no index statements — must still carry the SAI index afterwards.
        bridge.buildSchema(create, keyspace, rf, Partitioner.Murmur3Partitioner, emptySet(), null, emptySet(), false);
        assertThat(Schema.instance.getTableMetadata(keyspace, "tbl").indexes.get("tbl_b_idx")).isPresent();
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

    /** Applies SAI in its own atomic schema update, mirroring how the 5.0 bridge invokes the applier. */
    private static void applyIndexes(String keyspace, String table, Set<String> indexStatements)
    {
        CassandraSchema.update(schema -> StorageAttachedIndexApplier.applyTo(schema, keyspace, table, indexStatements));
    }
}
