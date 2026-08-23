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

import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;

public class CassandraSchemaTests
{
    public static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testUpdateCdcSchema()
    {
        SchemaProvider schema = Schema.instance;
        CassandraSchema.updateCdcSchema(schema, Collections.emptySet(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);

        final TestSchema testSchema1 = TestSchema.builder(BRIDGE)
                                                 .withPartitionKey("a", BRIDGE.bigint())
                                                 .withClusteringKey("b", BRIDGE.text())
                                                 .withColumn("c", BRIDGE.timeuuid())
                                                 .withCdc(true)
                                                 .build();
        final CqlTable cqlTable1 = testSchema1.buildTable();

        final TestSchema testSchema2 = TestSchema.builder(BRIDGE)
                                                 .withPartitionKey("pk", BRIDGE.uuid())
                                                 .withClusteringKey("ck", BRIDGE.aInt())
                                                 .withColumn("val", BRIDGE.blob())
                                                 .withCdc(true)
                                                 .build();
        final CqlTable cqlTable2 = testSchema2.buildTable();

        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isFalse();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1, cqlTable2), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isTrue();

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1, cqlTable2), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isTrue();

        CassandraSchema.disableCdc(schema, cqlTable2);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();

        CassandraSchema.disableCdc(schema, cqlTable1);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isFalse();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();

        CassandraSchema.enableCdc(schema, cqlTable1);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();

        CassandraSchema.enableCdc(schema, cqlTable2);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isTrue();

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable1)).isFalse();
        assertThat(CassandraSchema.isCdcEnabled(schema, cqlTable2)).isFalse();
    }

    @Test
    public void testUnregisterNonCdcTables()
    {
        SchemaProvider schema = Schema.instance;

        TestSchema nonCdcSchema = TestSchema.builder(BRIDGE)
                                            .withPartitionKey("a", BRIDGE.uuid())
                                            .withColumn("b", BRIDGE.text())
                                            .build();
        CqlTable nonCdcTable = nonCdcSchema.buildTable();
        TableIdentifier nonCdcId = TableIdentifier.of(nonCdcTable.keyspace(), nonCdcTable.table());

        TestSchema cdcSchema = TestSchema.builder(BRIDGE)
                                         .withKeyspace(nonCdcTable.keyspace())
                                         .withPartitionKey("a", BRIDGE.uuid())
                                         .withColumn("b", BRIDGE.text())
                                         .withCdc(true)
                                         .build();
        CqlTable cdcTable = cdcSchema.buildTable();
        TableIdentifier cdcId = TableIdentifier.of(cdcTable.keyspace(), cdcTable.table());

        // register both tables (as if they'd been found to share partition-key structure)
        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(nonCdcTable, cdcTable), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.has(schema, nonCdcTable.keyspace(), nonCdcTable.table())).isTrue();
        assertThat(CassandraSchema.has(schema, cdcTable.keyspace(), cdcTable.table())).isTrue();

        // a later refresh determines nonCdcTable is no longer at risk — unregister it
        CassandraSchema.unregisterNonCdcTables(schema, ImmutableSet.of(nonCdcId));
        assertThat(CassandraSchema.has(schema, nonCdcTable.keyspace(), nonCdcTable.table())).isFalse();
        // the CDC-enabled table must be completely unaffected
        assertThat(CassandraSchema.has(schema, cdcTable.keyspace(), cdcTable.table())).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cdcTable)).isTrue();

        // idempotent: unregistering an already-unregistered table is a no-op, not an error
        CassandraSchema.unregisterNonCdcTables(schema, ImmutableSet.of(nonCdcId));
        assertThat(CassandraSchema.has(schema, nonCdcTable.keyspace(), nonCdcTable.table())).isFalse();

        // refuses to unregister a table that is currently CDC-enabled
        CassandraSchema.unregisterNonCdcTables(schema, ImmutableSet.of(cdcId));
        assertThat(CassandraSchema.has(schema, cdcTable.keyspace(), cdcTable.table())).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cdcTable)).isTrue();

        // unregistering an unknown table (never registered) is a no-op, not an error
        CassandraSchema.unregisterNonCdcTables(schema, ImmutableSet.of(TableIdentifier.of("unknown_ks", "unknown_table")));

        // the table comes back at risk: registering it again reuses the column family store that the
        // metadata-only removal left with the keyspace instance
        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(nonCdcTable, cdcTable), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertThat(CassandraSchema.has(schema, nonCdcTable.keyspace(), nonCdcTable.table())).isTrue();
        assertThat(CassandraSchema.isCdcEnabled(schema, cdcTable)).isTrue();
    }
}
