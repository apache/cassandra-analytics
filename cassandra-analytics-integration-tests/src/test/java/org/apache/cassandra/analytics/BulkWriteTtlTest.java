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

package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.spark.bulkwriter.TTLOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class BulkWriteTtlTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true)
    public void testTableDefaultTtl()
    {
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        String keyspace = "spark_test";
        String table = "test_default_ttl";
        cluster.schemaChange("  CREATE KEYSPACE " + keyspace + " WITH replication = "
                             + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
                             + "      AND durable_writes = true;");
        String qualifiedTableName = keyspace + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualifiedTableName + " (\n"
                             + "          id BIGINT PRIMARY KEY,\n"
                             + "          course BLOB,\n"
                             + "          marks BIGINT\n"
                             + "     )  WITH default_time_to_live = 1;"
        );
        waitUntilSidecarPicksUpSchemaChange(keyspace);
        boolean addTTLColumn = false;
        boolean addTimestampColumn = false;
        IntegrationTestJob.builder((recordNum) -> generateCourse(recordNum, null, null),
                                   getWriteSchema(addTTLColumn, addTimestampColumn))
                          .withTable(table)
                          .withSidecarPort(server.actualPort())
                          .withExtraWriterOptions(Collections.emptyMap())
                          .shouldRead(false)
                          .run();
        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("select * from " + qualifiedTableName, ConsistencyLevel.ALL);
        Assertions.assertFalse(result.hasNext());
    }

    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true)
    public void testTtlOptionConstant()
    {
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        String keyspace = "spark_test";
        String table = "test_ttl_constant";
        cluster.schemaChange("  CREATE KEYSPACE " + keyspace + " WITH replication = "
                             + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
                             + "      AND durable_writes = true;");
        String qualifiedTableName = keyspace + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualifiedTableName + " (\n"
                             + "          id BIGINT PRIMARY KEY,\n"
                             + "          course BLOB,\n"
                             + "          marks BIGINT\n"
                             + "     );"
        );
        waitUntilSidecarPicksUpSchemaChange(keyspace);
        Map<String, String> writerOptions = ImmutableMap.of(WriterOptions.TTL.name(), TTLOption.constant(1));
        boolean addTTLColumn = false;
        boolean addTimestampColumn = false;
        IntegrationTestJob.builder((recordNum) -> generateCourse(recordNum, null, null),
                                   getWriteSchema(addTTLColumn, addTimestampColumn))
                          .withTable(table)
                          .withSidecarPort(server.actualPort())
                          .withExtraWriterOptions(writerOptions)
                          .shouldRead(false)
                          .run();
        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("select * from " + qualifiedTableName, ConsistencyLevel.ALL);
        Assertions.assertFalse(result.hasNext());
    }

    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true)
    public void testTtlOptionPerRow()
    {
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        String keyspace = "spark_test";
        String table = "test_ttl_per_row";
        cluster.schemaChange("  CREATE KEYSPACE " + keyspace + " WITH replication = "
                             + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
                             + "      AND durable_writes = true;");
        String qualifiedTableName = keyspace + '.' + table;
        cluster.schemaChange("CREATE TABLE " + qualifiedTableName + " (\n"
                             + "          id BIGINT PRIMARY KEY,\n"
                             + "          course BLOB,\n"
                             + "          marks BIGINT\n"
                             + "     );"
        );
        waitUntilSidecarPicksUpSchemaChange(keyspace);
        Map<String, String> writerOptions = ImmutableMap.of(WriterOptions.TTL.name(), TTLOption.perRow("ttl"));
        boolean addTTLColumn = true;
        boolean addTimestampColumn = false;
        IntegrationTestJob.builder((recordNum) -> generateCourse(recordNum, 1, null),
                                   getWriteSchema(addTTLColumn, addTimestampColumn))
                          .withTable(table)
                          .withSidecarPort(server.actualPort())
                          .withExtraWriterOptions(writerOptions)
                          .shouldRead(false)
                          .run();
        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("select * from " + qualifiedTableName, ConsistencyLevel.ALL);
        Assertions.assertFalse(result.hasNext());
    }

    @SuppressWarnings("SameParameterValue")
    private static StructType getWriteSchema(boolean addTTLColumn, boolean addTimestampColumn)
    {
        StructType schema = new StructType()
                            .add("id", LongType, false)
                            .add("course", BinaryType, false)
                            .add("marks", LongType, false);
        if (addTTLColumn)
        {
            schema = schema.add("ttl", IntegerType, false);
        }
        if (addTimestampColumn)
        {
            schema = schema.add("timestamp", LongType, false);
        }
        return schema;
    }

    @NotNull
    @SuppressWarnings("SameParameterValue")
    private static Row generateCourse(long recordNumber, Integer ttl, Long timestamp)
    {
        String courseNameString = String.valueOf(recordNumber);
        int courseNameStringLen = courseNameString.length();
        int courseNameMultiplier = 1000 / courseNameStringLen;
        byte[] courseName = dupStringAsBytes(courseNameString, courseNameMultiplier);
        ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNumber, courseName, recordNumber));
        if (ttl != null)
        {
            values.add(ttl);
        }
        if (timestamp != null)
        {
            values.add(timestamp);
        }
        return RowFactory.create(values.toArray());
    }

    private static byte[] dupStringAsBytes(String string, Integer times)
    {
        byte[] stringBytes = string.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(stringBytes.length * times);
        for (int time = 0; time < times; time++)
        {
            buffer.put(stringBytes);
        }
        return buffer.array();
    }
}
