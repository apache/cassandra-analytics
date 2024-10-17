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
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraSchemaTests
{
    public static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testUpdateCdcSchema()
    {
        Schema schema = Schema.instance;
        CassandraSchema.updateCdcSchema(schema, Collections.emptySet(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);

        final TestSchema testSchema1 = TestSchema.builder(BRIDGE)
                                                 .withPartitionKey("a", BRIDGE.bigint())
                                                 .withClusteringKey("b", BRIDGE.text())
                                                 .withColumn("c", BRIDGE.timeuuid())
                                                 .build();
        final CqlTable cqlTable1 = testSchema1.buildTable();

        final TestSchema testSchema2 = TestSchema.builder(BRIDGE)
                                                 .withPartitionKey("pk", BRIDGE.uuid())
                                                 .withClusteringKey("ck", BRIDGE.aInt())
                                                 .withColumn("val", BRIDGE.blob())
                                                 .build();
        final CqlTable cqlTable2 = testSchema2.buildTable();

        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1, cqlTable2), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1, cqlTable2), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.disableCdc(schema, cqlTable2);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.disableCdc(schema, cqlTable1);
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.enableCdc(schema, cqlTable1);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.enableCdc(schema, cqlTable2);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(cqlTable1), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertTrue(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));

        CassandraSchema.updateCdcSchema(schema, ImmutableSet.of(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable1));
        assertFalse(CassandraSchema.isCdcEnabled(schema, cqlTable2));
    }
}
