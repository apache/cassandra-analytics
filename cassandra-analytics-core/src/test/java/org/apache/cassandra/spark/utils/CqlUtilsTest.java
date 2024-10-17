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

package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for {@link CqlUtils}
 */
public class CqlUtilsTest extends VersionRunner
{
    static String fullSchemaSample;

    @TempDir
    private static Path tempPath;

    @BeforeAll
    public static void setup() throws URISyntaxException, IOException
    {
        fullSchemaSample = loadFullSchemaSample();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void textExtractIndexCount(CassandraBridge bridge)
    {
        int indexCount = CqlUtils.extractIndexCount(fullSchemaSample, "cycling", "rank_by_year_and_name");
        assertEquals(3, indexCount);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractKeyspace(CassandraBridge bridge)
    {
        String keyspaceSchema = CqlUtils.extractKeyspaceSchema(fullSchemaSample, "keyspace");
        String tagEntityRelationV4KeyspaceSchema = CqlUtils.extractKeyspaceSchema(fullSchemaSample, "quoted_keyspace");
        String systemDistributedKeyspaceSchema = CqlUtils.extractKeyspaceSchema(fullSchemaSample, "system_distributed");
        String systemSchemaKeyspaceSchema = CqlUtils.extractKeyspaceSchema(fullSchemaSample, "system_schema");
        assertEquals("CREATE KEYSPACE keyspace "
                   + "WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
                   +                      "'datacenter1': '4', "
                   +                      "'datacenter2': '3' } AND DURABLE_WRITES = true;", keyspaceSchema);
        assertEquals("CREATE KEYSPACE \"quoted_keyspace\" "
                   + "WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
                   +                      "'datacenter1': '3', "
                   +                      "'datacenter2': '3' } AND DURABLE_WRITES = true;", tagEntityRelationV4KeyspaceSchema);
        assertEquals("CREATE KEYSPACE system_distributed "
                   + "WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', "
                   +                      "'replication_factor': '3' } AND DURABLE_WRITES = true;", systemDistributedKeyspaceSchema);
        assertEquals("CREATE KEYSPACE system_schema "
                   + "WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;", systemSchemaKeyspaceSchema);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractKeyspaceNames(CassandraBridge bridge)
    {
        Set<String> keyspaceNames = CqlUtils.extractKeyspaceNames(fullSchemaSample);
        assertEquals(3, keyspaceNames.size());
        Map<String, ReplicationFactor> rfMap = keyspaceNames
                .stream()
                .collect(Collectors.toMap(Function.identity(),
                                          keyspace -> CqlUtils.extractReplicationFactor(fullSchemaSample, keyspace)));
        assertTrue(rfMap.containsKey("keyspace"));
        assertTrue(rfMap.containsKey("quoted_keyspace"));
        assertEquals(4, rfMap.get("keyspace").getOptions().get("datacenter1").intValue());
        assertEquals(3, rfMap.get("keyspace").getOptions().get("datacenter2").intValue());
        assertEquals(3, rfMap.get("quoted_keyspace").getOptions().get("datacenter1").intValue());
        assertEquals(3, rfMap.get("quoted_keyspace").getOptions().get("datacenter2").intValue());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractReplicationFactor(CassandraBridge bridge)
    {
        ReplicationFactor keyspaceRf = CqlUtils.extractReplicationFactor(fullSchemaSample, "keyspace");
        assertNotNull(keyspaceRf);
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, keyspaceRf.getReplicationStrategy());
        assertEquals(7, keyspaceRf.getTotalReplicationFactor().intValue());
        assertEquals(ImmutableMap.of("datacenter1", 4, "datacenter2", 3), keyspaceRf.getOptions());

        ReplicationFactor tagEntityRelationV4Rf = CqlUtils.extractReplicationFactor(fullSchemaSample, "quoted_keyspace");
        assertNotNull(tagEntityRelationV4Rf);
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, tagEntityRelationV4Rf.getReplicationStrategy());
        assertEquals(6, tagEntityRelationV4Rf.getTotalReplicationFactor().intValue());
        assertEquals(ImmutableMap.of("datacenter1", 3, "datacenter2", 3), tagEntityRelationV4Rf.getOptions());

        ReplicationFactor systemDistributedRf = CqlUtils.extractReplicationFactor(fullSchemaSample, "system_distributed");
        assertNotNull(systemDistributedRf);
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, systemDistributedRf.getReplicationStrategy());
        assertEquals(3, systemDistributedRf.getTotalReplicationFactor().intValue());
        assertEquals(ImmutableMap.of("replication_factor", 3), systemDistributedRf.getOptions());

        ReplicationFactor systemSchemaRf = CqlUtils.extractReplicationFactor(fullSchemaSample, "system_schema");
        assertNotNull(systemSchemaRf);
        assertEquals(ReplicationFactor.ReplicationStrategy.LocalStrategy, systemSchemaRf.getReplicationStrategy());
        assertEquals(0, systemSchemaRf.getTotalReplicationFactor().intValue());
        assertEquals(ImmutableMap.of(), systemSchemaRf.getOptions());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEscapedColumnNames(CassandraBridge bridge)
    {
        String cleaned = CqlUtils.extractTableSchema(fullSchemaSample, "cycling", "rank_by_year_and_name_quoted_columns");
        assertEquals("CREATE TABLE cycling.rank_by_year_and_name_quoted_columns("
                   + "    race_year      int,"
                   + "    \"RACE_NAME\"    text,"
                   + "    rank           int,"
                   + "    \"cyclist_Name\" text,"
                   + "    PRIMARY KEY ((race_year, \"RACE_NAME\"), rank)) "
                   + "WITH CLUSTERING ORDER BY (rank ASC)"
                   + " AND bloom_filter_fp_chance = 0.01"
                   + " AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }"
                   + " AND default_time_to_live = 0"
                   + " AND min_index_interval = 128"
                   + " AND max_index_interval = 2048;", cleaned);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractTableSchemaCase1(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table ("
                         + "key blob, "
                         + "column1 text, "
                         + "\"C0\" counter static, "
                         + "\"C1\" counter static, "
                         + "\"C2\" counter static, "
                         + "\"C3\" counter static, "
                         + "\"C4\" counter static, "
                         + "value counter, "
                         + "PRIMARY KEY (key, column1) "
                         + ") WITH COMPACT STORAGE "
                         + "AND CLUSTERING ORDER BY (column1 ASC) "
                         + "AND bloom_filter_fp_chance = 0.1 "
                         + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} "
                         + "AND comment = '' "
                         + "AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} "
                         + "AND compression = {'chunk_length_in_kb': '64', "
                         +                    "'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} "
                         + "AND crc_check_chance = 1.0 "
                         + "AND dclocal_read_repair_chance = 0.1 "
                         + "AND default_time_to_live = 0 "
                         + "AND gc_grace_seconds = 864000 "
                         + "AND max_index_interval = 2048 "
                         + "AND memtable_flush_period_in_ms = 0 "
                         + "AND min_index_interval = 128  "
                         + "AND read_repair_chance = 0.0 "
                         + "AND speculative_retry = '99p';";
        String expectedCreateStmt = "CREATE TABLE keyspace.table ("
                                  + "key blob, "
                                  + "column1 text, "
                                  + "\"C0\" counter static, "
                                  + "\"C1\" counter static, "
                                  + "\"C2\" counter static, "
                                  + "\"C3\" counter static, "
                                  + "\"C4\" counter static, "
                                  + "value counter, "
                                  + "PRIMARY KEY (key, column1) ) WITH CLUSTERING ORDER BY (column1 ASC)"
                                  + " AND bloom_filter_fp_chance = 0.1"
                                  + " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.DeflateCompressor'}"
                                  + " AND default_time_to_live = 0"
                                  + " AND max_index_interval = 2048 AND min_index_interval = 128;";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
        assertEquals(expectedCreateStmt, actualCreateStmt);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testFailsWithUnbalancedParenthesis(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table (key blob, c0 text, c1 text, PRIMARY KEY (key);";

        try
        {
            CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
            fail("Expected RuntimeException when parentheses are unbalanced");
        }
        catch (RuntimeException exception)
        {
            assertEquals("Found unbalanced parentheses in table schema "
                       + "CREATE TABLE keyspace.table (key blob, c0 text, c1 text, PRIMARY KEY (key);",
                         exception.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractTableSchemaCase2(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table ("
                         + "key blob, "
                         + "column1 text, "
                         + "\"C0\" blob, "
                         + "\"C1\" blob, "
                         + "\"C2\" blob, "
                         + "\"C4\" blob, "
                         + "value counter, "
                         + "PRIMARY KEY (key, column1) "
                         + ") WITH bloom_filter_fp_chance = 0.1 "
                         + "AND cdc = false "
                         + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} "
                         + "AND comment = '' "
                         + "AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} "
                         + "AND compression = {'chunk_length_in_kb': '64', "
                         +                    "'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} "
                         + "AND crc_check_chance = 1.0 "
                         + "AND dclocal_read_repair_chance = 0.1 "
                         + "AND default_time_to_live = 100 "
                         + "AND gc_grace_seconds = 864000 "
                         + "AND max_index_interval = 2048 "
                         + "AND memtable_flush_period_in_ms = 0 "
                         + "AND min_index_interval = 128 "
                         + "AND read_repair_chance = 0.0 "
                         + "AND speculative_retry = '99p';";
        String expectedCreateStmt = "CREATE TABLE keyspace.table ("
                                  + "key blob, "
                                  + "column1 text, "
                                  + "\"C0\" blob, "
                                  + "\"C1\" blob, "
                                  + "\"C2\" blob, "
                                  + "\"C4\" blob, "
                                  + "value counter, "
                                  + "PRIMARY KEY (key, column1) ) WITH"
                                  + " bloom_filter_fp_chance = 0.1"
                                  + " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.DeflateCompressor'}"
                                  + " AND default_time_to_live = 100 AND max_index_interval = 2048 "
                                  + "AND min_index_interval = 128;";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
        assertEquals(expectedCreateStmt, actualCreateStmt);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEscapedTableName(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE ks.\\\"tb\\\" (\\n"
                         + "\\\"key\\\" text,\\n"
                         + " \\\"id1\\\" text,\\n"
                         + " \\\"id2\\\" text,\\n"
                         + " \\\"id3\\\" text,\\n"
                         + " created timestamp,\\n"
                         + " id4 uuid,\\n metadata blob,\\n"
                         + " PRIMARY KEY ((\\\"key\\\", \\\"id1\\\"), \\\"id2\\\", \\\"id3\\\")\\n) "
                         + "WITH CLUSTERING ORDER BY (\\\"id2\\\" DESC, \\\"id3\\\" ASC)\\n"
                         + "    AND read_repair_chance = 0.0\\n"
                         + "    AND dclocal_read_repair_chance = 0.1\\n"
                         + "    AND gc_grace_seconds = 864000\\n"
                         + "    AND bloom_filter_fp_chance = 0.1\\n"
                         + "    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\\n"
                         + "    AND comment = ''\\n"
                         + "    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', "
                         +                        "'max_threshold' : 32, "
                         +                        "'min_threshold' : 4 }\\n"
                         + "    AND compression = { 'chunk_length_in_kb' : 64, "
                         +                         "'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }\\n"
                         + "    AND default_time_to_live = 0\\n"
                         + "    AND speculative_retry = '99p'\\n"
                         + "    AND min_index_interval = 128\\n"
                         + "    AND max_index_interval = 2048\\n"
                         + "    AND crc_check_chance = 1.0\\n"
                         + "    AND memtable_flush_period_in_ms = 0;";
        String expectedCreateStmt = "CREATE TABLE ks.\"tb\" ("
                                  + "\"key\" text, "
                                  + "\"id1\" text, "
                                  + "\"id2\" text, "
                                  + "\"id3\" text, "
                                  + "created timestamp, "
                                  + "id4 uuid, "
                                  + "metadata blob, "
                                  + "PRIMARY KEY ((\"key\", \"id1\"), \"id2\", \"id3\")) "
                                  + "WITH CLUSTERING ORDER BY (\"id2\" DESC, \"id3\" ASC)"
                                  + " AND bloom_filter_fp_chance = 0.1"
                                  + " AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }"
                                  + " AND default_time_to_live = 0"
                                  + " AND min_index_interval = 128"
                                  + " AND max_index_interval = 2048;";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "ks", "tb");
        assertEquals(expectedCreateStmt, actualCreateStmt);
        CqlTable table = bridge.buildSchema(actualCreateStmt,
                                            "ks",
                                            new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                  ImmutableMap.of("datacenter1", 3)),
                                            Partitioner.Murmur3Partitioner,
                                            Collections.emptySet(),
                                            null, 0, false);
        assertEquals("ks", table.keyspace());
        assertEquals("tb", table.table());
        assertEquals("key", table.getField("key").name());
        assertEquals("id1", table.getField("id1").name());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractTableSchemaCase3(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table ("
                         + "key blob, "
                         + "column1 text, "
                         + "\"C0\" blob, "
                         + "\"C1\" blob, "
                         + "\"C2\" blob, "
                         + "\"C4\" blob, "
                         + "value counter, "
                         + "PRIMARY KEY ((key, column1), value) "
                         + ") WITH bloom_filter_fp_chance = 0.1 "
                         + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} "
                         + "AND comment = '' "
                         + "AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} "
                         + "AND compression = {'chunk_length_in_kb': '64', "
                         +                    "'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} "
                         + "AND crc_check_chance = 1.0 "
                         + "AND dclocal_read_repair_chance = 0.1 "
                         + "AND default_time_to_live = 0 "
                         + "AND gc_grace_seconds = 864000 "
                         + "AND max_index_interval = 2048 "
                         + "AND memtable_flush_period_in_ms = 0 "
                         + "AND min_index_interval = 128 "
                         + "AND read_repair_chance = 0.0 "
                         + "AND speculative_retry = '99p';";
        String expectedCreateStmt = "CREATE TABLE keyspace.table ("
                                  + "key blob, "
                                  + "column1 text, "
                                  + "\"C0\" blob, "
                                  + "\"C1\" blob, "
                                  + "\"C2\" blob, "
                                  + "\"C4\" blob, "
                                  + "value counter, "
                                  + "PRIMARY KEY ((key, column1), value) ) WITH"
                                  + " bloom_filter_fp_chance = 0.1"
                                  + " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.DeflateCompressor'}"
                                  + " AND default_time_to_live = 0"
                                  + " AND max_index_interval = 2048 AND min_index_interval = 128;";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
        assertEquals(expectedCreateStmt, actualCreateStmt);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testBasicExtractUDTs(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TYPE udt_keyspace.test_idt1 (a text, b bigint, c int, d float);\n"
                         + "CREATE TYPE udt_keyspace.test_idt2 (x boolean, y timestamp, z timeuuid);";
        Set<String> udts = CqlUtils.extractUdts(schemaStr, "udt_keyspace");
        assertEquals(2, udts.size());
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.test_idt1 (a text, b bigint, c int, d float);"));
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.test_idt2 (x boolean, y timestamp, z timeuuid);"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractUDTs(CassandraBridge bridge)
    {
        String schema = "\"CREATE TYPE some_keyspace.udt123 (\\n\" +\n"
                      + "                              \"    x uuid,\\n\" +\n"
                      + "                              \"    y text,\\n\" +\n"
                      + "                              \"    z uuid\\n\" +\n"
                      + "                              \");\\n\" +"
                      + "\"CREATE KEYSPACE udt_keyspace "
                      + "WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
                      +                      "'datacenter1': '3' } AND DURABLE_WRITES = true;\n"
                      + "CREATE TYPE udt_keyspace.type_with_time_zone (\n"
                      + "    time bigint,\n"
                      + "    \\\"timezoneOffsetMinutes\\\" int\n"
                      + ");\n"
                      + "CREATE TYPE udt_keyspace.type_1 (\n"
                      + "    \\\"x\\\" text,\n"
                      + "    \\\"y\\\" text,\n"
                      + "    z text,\n"
                      + "    \\\"a\\\" boolean\n"
                      + ");\n"
                      + "CREATE TYPE udt_keyspace.field_with_timestamp (\n"
                      + "    field text,\n"
                      + "    \\\"timeWithZone\\\" frozen<udt_keyspace.type_with_time_zone>\n"
                      + ");\n"
                      + "CREATE TYPE udt_keyspace.type_with_frozen_fields (\n"
                      + "    \\\"f1\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f2\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f3\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f4\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f5\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f6\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f7\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f8\\\" frozen<udt_keyspace.field_with_timestamp>,\n"
                      + "    \\\"f9\\\" text,\n"
                      + "    \\\"f10\\\" frozen<map<bigint, frozen<map<text, boolean>>>>\n"
                      + ");\n"
                      + "CREATE TYPE another_keyspace.some_udt (\n"
                      + "    x uuid,\n"
                      + "    y text,\n"
                      + "    z uuid\n"
                      + ");\n"
                      + "CREATE TYPE another_keyspace.another_udt (\n"
                      + "    a uuid,\n"
                      + "    b text,\n"
                      + "    c uuid\n"
                      + ");";
        Set<String> udts = CqlUtils.extractUdts(schema, "udt_keyspace");
        assertEquals(4, udts.size());
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.type_with_frozen_fields ("
                               + "    \"f1\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f2\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f3\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f4\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f5\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f6\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f7\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f8\" frozen<udt_keyspace.field_with_timestamp>,"
                               + "    \"f9\" text,"
                               + "    \"f10\" frozen<map<bigint, frozen<map<text, boolean>>>>"
                               + ");"));
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.type_with_time_zone ("
                               + "    time bigint,"
                               + "    \"timezoneOffsetMinutes\" int"
                               + ");"));
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.type_1 ("
                               + "    \"x\" text,"
                               + "    \"y\" text,"
                               + "    z text,"
                               + "    \"a\" boolean"
                               + ");"));
        assertTrue(udts.contains("CREATE TYPE udt_keyspace.field_with_timestamp ("
                               + "    field text,"
                               + "    \"timeWithZone\" frozen<udt_keyspace.type_with_time_zone>"
                               + ");"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractKeyspacesUDTs(CassandraBridge bridge)
    {
        String schemaTxt = "\"CREATE KEYSPACE keyspace_with_udts WITH REPLICATION = {"
                         + " 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy',"
                         + " 'datacenter1': '3' } AND DURABLE_WRITES = true;\\n\\n"
                         + "CREATE TYPE keyspace_with_udts.type_with_time_zone (\\n"
                         + "    time bigint,\\n"
                         + "    \\\"timezoneOffsetMinutes\\\" int\\n);"
                         + "CREATE TYPE keyspace_with_udts.field_with_timestamp (\\n"
                         + "    field text,\\n"
                         + "    \\\"timeWithZone\\\" frozen<keyspace_with_udts.type_with_time_zone>\\n);\\n\\n"
                         + "CREATE TYPE keyspace_with_udts.type_with_frozen_fields (\\n"
                         + "    \\\"f1\\\" frozen<keyspace_with_udts.field_with_timestamp>,\\n"
                         + "    \\\"f2\\\" frozen<keyspace_with_udts.field_with_timestamp>,\\n"
                         + "    \\\"f3\\\" frozen<map<bigint, int>>\\n);"
                         + "CREATE KEYSPACE ks1 WITH REPLICATION = {"
                         + " 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy',"
                         + " 'datacenter1': '3' } AND DURABLE_WRITES = true;\\n\\n"
                         + "CREATE TYPE ks1.type_with_time_zone (\\n"
                         + "    time bigint,\\n"
                         + "    \\\"timezoneOffsetMinutes\\\" int\\n);\\n\\n"
                         + "CREATE TYPE ks1.type_1 (\\n"
                         + "    \\\"f1\\\" text,\\n"
                         + "    \\\"f2\\\" text,\\n"
                         + "    \\\"f3\\\" text\\n);\\n\\n"
                         + "CREATE TYPE ks1.type_2 (\\n"
                         + "    \\\"f1\\\" text,\\n"
                         + "    \\\"f2\\\" text,\\n"
                         + "    \\\"f3\\\" text,\\n"
                         + "    f4 text\\n);\\n\\n"
                         + "CREATE TYPE ks1.field_with_timestamp (\\n"
                         + "    field text,\\n"
                         + "    \\\"timeWithZone\\\" frozen<ks1.type_with_time_zone>\\n);";
        Set<String> udts = CqlUtils.extractUdts(schemaTxt, "ks1");
        assertEquals(4, udts.size());
        String udtStr = String.join("\n", udts);
        assertTrue(udtStr.contains("ks1.type_with_time_zone"));
        assertTrue(udtStr.contains("ks1.type_1"));
        assertTrue(udtStr.contains("ks1.type_2"));
        assertTrue(udtStr.contains("ks1.field_with_timestamp"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractTableSchemaCase4(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table (value text PRIMARY KEY);";
        String expectedCreateStmt = "CREATE TABLE keyspace.table (value text PRIMARY KEY);";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
        assert expectedCreateStmt.equals(actualCreateStmt);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testParseClusteringKeySchema(CassandraBridge bridge)
    {
        String schemaTxt = "CREATE TABLE ks1.tb1 (\n"
                         + "    namespace int,\n"
                         + "    user_id text,\n"
                         + "    dc_id int,\n"
                         + "    ping_timestamp timestamp,\n"
                         + "    PRIMARY KEY ((namespace, user_id), dc_id)\n"
                         + ") WITH CLUSTERING ORDER BY (dc_id ASC)\n"
                         + "    AND additional_write_policy = '99p'\n"
                         + "    AND bloom_filter_fp_chance = 0.1\n"
                         + "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n"
                         + "    AND cdc = false\n"
                         + "    AND comment = ''\n"
                         + "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', "
                         +                                "'max_threshold': '32', "
                         +                                "'min_threshold': '4'}\n"
                         + "    AND compression = {'chunk_length_in_kb': '16', "
                         +                        "'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                         + "    AND crc_check_chance = 1.0\n"
                         + "    AND default_time_to_live = 0\n"
                         + "    AND extensions = {}\n"
                         + "    AND gc_grace_seconds = 864000\n"
                         + "    AND max_index_interval = 256\n"
                         + "    AND memtable_flush_period_in_ms = 0\n"
                         + "    AND min_index_interval = 64\n"
                         + "    AND read_repair = 'BLOCKING'\n"
                         + "    AND speculative_retry = 'MIN(99p,15ms)';";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaTxt, "ks1", "tb1");
        bridge.buildSchema(actualCreateStmt, "ks1",
                           new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                 ImmutableMap.of("datacenter1", 3)),
                           Partitioner.Murmur3Partitioner,
                           Collections.emptySet(), null, 0, false);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testExtractClusteringKey(CassandraBridge bridge)
    {
        assertEquals("CLUSTERING ORDER BY (c ASC)",
                     CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                              + " WITH CLUSTERING ORDER BY (c ASC) AND additional_write_policy = '99p';"));
        assertEquals("CLUSTERING ORDER BY (c ASC)",
                     CqlUtils.extractClustering("WITH CLUSTERING ORDER BY (c ASC)"));
        assertEquals("CLUSTERING ORDER BY (c ASC)",
                     CqlUtils.extractClustering("WITH CLUSTERING ORDER BY (c ASC);"));
        assertEquals("CLUSTERING ORDER BY (c ASC)",
                     CqlUtils.extractClustering("**** WITH CLUSTERING ORDER BY (c ASC)  AND ****     AND   ******* AND '***';"));
        assertEquals("CLUSTERING ORDER BY (a DESC, b ASC, c ASC)",
                     CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                              + " WITH CLUSTERING ORDER BY (a DESC, b ASC, c ASC) AND additional_write_policy = '99p'"
                                              + " AND bloom_filter_fp_chance = 0.1 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}"
                                              + " AND cdc = false AND comment = '' AND compaction = {'class':"
                                              + " 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32',"
                                              + " 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '16', 'class':"
                                              + " 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0"
                                              + " AND default_time_to_live = 0 AND extensions = {} AND gc_grace_seconds = 864000"
                                              + " AND max_index_interval = 256 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 64"
                                              + " AND read_repair = 'BLOCKING' AND speculative_retry = 'MIN(99p,15ms)';"));
        assertEquals("CLUSTERING ORDER BY (a DESC, b ASC, c ASC)",
                     CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                              + " WITH CLUSTERING ORDER BY (a DESC, b ASC, c ASC) AND additional_write_policy = '99p'"
                                              + " AND bloom_filter_fp_chance = 0.1 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}"
                                              + " AND cdc = false AND comment = '' AND compaction = {'class':"
                                              + " 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32',"
                                              + " 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '16', 'class':"
                                              + " 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0"
                                              + " AND default_time_to_live = 0 AND extensions = {} AND gc_grace_seconds = 864000"
                                              + " AND max_index_interval = 256 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 64"
                                              + " AND read_repair = 'BLOCKING' AND speculative_retry = 'MIN(99p,15ms)'"));
        assertEquals("CLUSTERING ORDER BY (a DESC, b ASC, c ASC)",
                     CqlUtils.extractClustering("WITH CLUSTERING ORDER BY (a DESC, b ASC, c ASC)"));
        assertEquals("CLUSTERING ORDER BY (a ASC)",
                     CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c))"
                                              + " WITH CLUSTERING ORDER BY (a ASC) AND speculative_retry = 'MIN(99p,15ms);"));

        assertNull(CqlUtils.extractClustering(""));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c);"));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                            + " AND additional_write_policy = '99p';"));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                            + " AND additional_write_policy = '99p'"));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                            + " AND additional_write_policy = '99p' AND bloom_filter_fp_chance = 0.1"
                                            + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND cdc = false AND comment = ''"
                                            + " AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',"
                                            + " 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '16',"
                                            + " 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0"
                                            + " AND default_time_to_live = 0 AND extensions = {} AND gc_grace_seconds = 864000 AND"
                                            + " max_index_interval = 256 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 64"
                                            + " AND read_repair = 'BLOCKING' AND speculative_retry = 'MIN(99p,15ms)';"));
        assertNull(CqlUtils.extractClustering("CREATE TABLE ks1.tb1 (a int, b text, c int, d timestamp, PRIMARY KEY ((a, b), c)"
                                            + " AND additional_write_policy = '99p' AND bloom_filter_fp_chance = 0.1"
                                            + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND cdc = false AND comment = ''"
                                            + " AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',"
                                            + " 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '16',"
                                            + " 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0"
                                            + " AND default_time_to_live = 0 AND extensions = {} AND gc_grace_seconds = 864000 AND"
                                            + " max_index_interval = 256 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 64"
                                            + " AND read_repair = 'BLOCKING' AND speculative_retry = 'MIN(99p,15ms)'"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testClusteringOrderByIsRetained(CassandraBridge bridge)
    {
        String schemaStr = "CREATE TABLE keyspace.table (id bigint, version bigint PRIMARY KEY (id, version)) "
                           + "WITH CLUSTERING ORDER BY (id DESC, version DESC) AND foo = 1;";
        String expectedCreateStmt = "CREATE TABLE keyspace.table (id bigint, version bigint PRIMARY KEY (id, version)) "
                                    + "WITH CLUSTERING ORDER BY (id DESC, version DESC);";
        String actualCreateStmt = CqlUtils.extractTableSchema(schemaStr, "keyspace", "table");
        assertEquals(expectedCreateStmt, actualCreateStmt);
    }

    @Test
    public void testExtractCdcFlag()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("cdc")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH cdc = true;",
                                                      Collections.singletonList("cdc")).contains("cdc = true"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH cdc = false;",
                                                      Collections.singletonList("cdc")).contains("cdc = false"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH cdc = true"
                                                    + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("cdc")).contains("cdc = true"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH cdc = false"
                                                    + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("cdc")).contains("cdc = false"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                    + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND cdc = true;",
                                                      Collections.singletonList("cdc")).contains("cdc = true"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1 "
                                                    + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND cdc = false;",
                                                      Collections.singletonList("cdc")).contains("cdc = false"));
    }

    @Test
    public void testExtractDefaultTtlOption()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("default_time_to_live")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH default_time_to_live = 1;",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 1"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH default_time_to_live = 2;",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 2"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH default_time_to_live = 3"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 3"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH default_time_to_live = 4"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 4"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND default_time_to_live = 5;",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 5"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1 "
                                                      + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND default_time_to_live = 6;",
                                                      Collections.singletonList("default_time_to_live")).contains("default_time_to_live = 6"));
    }

    @Test
    public void testExtractBloomFilterFalsePositiveChance()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("bloom_filter_fp_chance")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1;",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.1"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.2;",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.2"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.3"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.3"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.4"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.4"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND bloom_filter_fp_chance = 0.5;",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.5"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1 "
                                                      + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND bloom_filter_fp_chance = 0.6;",
                                                      Collections.singletonList("bloom_filter_fp_chance")).contains("bloom_filter_fp_chance = 0.6"));
    }


    @Test
    public void testExtractCompression()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("compression")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH compression = { 'fake_option': '0.1' };",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.1' }"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH compression = { 'fake_option': '0.2' };",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.2' }"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH compression = { 'fake_option': '0.3' }"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.3' }"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH compression = { 'fake_option': '0.4' }"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.4' }"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}"
                                                      + " AND compression = { 'fake_option': '0.5' };",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.5' }"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} "
                                                      + " AND compression = { 'fake_option': '0.6' };",
                                                      Collections.singletonList("compression")).contains("compression = { 'fake_option': '0.6' }"));
    }

    @Test
    public void testExtractMinIndexInterval()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("min_index_interval")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH min_index_interval = 1;",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 1"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH min_index_interval = 2;",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 2"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH min_index_interval = 3"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 3"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH min_index_interval = 4"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 4"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND min_index_interval = 5;",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 5"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1 "
                                                      + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND min_index_interval = 6;",
                                                      Collections.singletonList("min_index_interval")).contains("min_index_interval = 6"));
    }

    @Test
    public void testExtractMaxIndexInterval()
    {
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int);",
                                                      Collections.singletonList("max_index_interval")).isEmpty());
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH max_index_interval = 1;",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 1"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH max_index_interval = 2;",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 2"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH max_index_interval = 3"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 3"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH max_index_interval = 4"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'};",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 4"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"
                                                      + " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND max_index_interval = 5;",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 5"));
        assertTrue(CqlUtils.extractOverrideProperties("CREATE TABLE k.t (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1 "
                                                      + "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND max_index_interval = 6;",
                                                      Collections.singletonList("max_index_interval")).contains("max_index_interval = 6"));
    }

    private static String loadFullSchemaSample() throws IOException
    {
        Path fullSchemaSampleFile = ResourceUtils.writeResourceToPath(CqlUtilsTest.class.getClassLoader(), tempPath, "cql/fullSchema.cql");
        return FileUtils.readFileToString(fullSchemaSampleFile.toFile(), StandardCharsets.UTF_8);
    }
}
