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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.quicktheories.QuickTheory.qt;

public class SchemaTests extends VersionRunner
{
    public static final String SCHEMA = "CREATE TABLE backup_test.sbr_test (\n"
                                      + "    account_id uuid,\n"
                                      + "    balance bigint,\n"
                                      + "    name text,\n"
                                      + "    PRIMARY KEY(account_id)\n"
                                      + ") WITH bloom_filter_fp_chance = 0.1\n"
                                      + "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n"
                                      + "    AND comment = 'Created by: jberragan'\n"
                                      + "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}\n"
                                      + "    AND compression = {'chunk_length_in_kb': '64', "
                                      +                        "'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
                                      + "    AND crc_check_chance = 1.0\n"
                                      + "    AND default_time_to_live = 0\n"
                                      + "    AND gc_grace_seconds = 864000\n"
                                      + "    AND max_index_interval = 2048\n"
                                      + "    AND memtable_flush_period_in_ms = 0\n"
                                      + "    AND min_index_interval = 128\n;";


    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testBuild(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        List<CqlField> fields = table.fields();
        assertNotNull(fields);
        assertEquals(3, fields.size());
        assertEquals("account_id", fields.get(0).name());
        assertEquals("balance", fields.get(1).name());
        assertEquals("name", fields.get(2).name());
        assertEquals(SCHEMA, table.createStatement());
        assertEquals(3, table.replicationFactor().getOptions().get("DC1").intValue());
        assertEquals(3, table.replicationFactor().getOptions().get("DC2").intValue());
        assertNull(table.replicationFactor().getOptions().get("DC3"));
        assertEquals(1, table.numPartitionKeys());
        assertEquals(0, table.numClusteringKeys());
        assertEquals(0, table.numStaticColumns());
        assertEquals(2, table.numValueColumns());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEquality(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table1 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        CqlTable table2 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        assertNotSame(table1, table2);
        assertNotEquals(null, table2);
        assertNotEquals(null, table1);
        assertNotEquals(new ArrayList<>(), table1);
        assertEquals(table1, table1);
        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSameKeyspace(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table1 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        CqlTable table2 = bridge.buildSchema(SCHEMA.replace("sbr_test", "sbr_test2"), "backup_test", replicationFactor);
        assertNotSame(table1, table2);
        assertEquals("sbr_test2", table2.table());
        assertEquals("sbr_test", table1.table());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testHasher(CassandraBridge bridge)
    {
        // Casts to (ByteBuffer) required when compiling with Java 8
        assertEquals(BigInteger.valueOf(6747049197585865300L),
                     bridge.hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(992393994949L).flip()));
        assertEquals(BigInteger.valueOf(7071430368280192841L),
                     bridge.hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(999).flip()));
        assertEquals(new BigInteger("28812675363873787366858706534556752548"),
                     bridge.hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(34828288292L).flip()));
        assertEquals(new BigInteger("154860613751552680515987154638148676974"),
                     bridge.hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(1929239).flip()));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUUID(CassandraBridge bridge)
    {
        assertEquals(1, bridge.getTimeUUID().version());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCollections(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.collection_test (account_id uuid PRIMARY KEY, balance bigint, names set<text>);";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(createStatement, "backup_test", replicationFactor);
        assertEquals(CqlField.CqlType.InternalType.Set, table.getField("names").type().internalType());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSetClusteringKey(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test_set_ck (pk uuid, ck frozen<set<text>>, PRIMARY KEY (pk, ck));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        bridge.buildSchema(createStatement, "backup_test", replicationFactor);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testListClusteringKey(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test_list_ck (pk uuid, ck frozen<list<bigint>>, PRIMARY KEY (pk, ck));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        bridge.buildSchema(createStatement, "backup_test", replicationFactor);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testMapClusteringKey(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test_map_ck (pk uuid, ck frozen<map<uuid, timestamp>>, PRIMARY KEY (pk, ck));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        bridge.buildSchema(createStatement, "backup_test", replicationFactor);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNativeUnsupportedColumnMetaData(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test (account_id uuid, transactions counter, PRIMARY KEY(account_id));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        assertThrows(UnsupportedOperationException.class,
                     () -> bridge.buildSchema(createStatement, "backup_test", replicationFactor)
        );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUnsupportedInnerType(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test (account_id uuid, transactions counter, PRIMARY KEY(account_id));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        assertThrows(UnsupportedOperationException.class,
                     () -> bridge.buildSchema(createStatement, "backup_test", replicationFactor)
        );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUnsupportedUdt(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test (account_id uuid, transactions frozen<testudt>, PRIMARY KEY (account_id));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        assertThrows(UnsupportedOperationException.class,
                     () -> bridge.buildSchema(createStatement, "backup_test", replicationFactor, Partitioner.Murmur3Partitioner,
                    ImmutableSet.of("CREATE TYPE backup_test.testudt(birthday timestamp, count bigint, length counter);"))
        );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCollectionMatcher(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher("set<%s>", "set", type, bridge));
        qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher("list<%s>", "list", type, bridge));
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((first, second) -> {
            testMatcher("map<%s,%s>", "map", first, second, bridge);
            testMatcher("map<%s , %s>", "map", first, second, bridge);
        });
        qt().forAll(TestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher(type.cqlName(), null, null, bridge));
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((first, second) -> {
            testMatcher("tuple<%s,%s>", "tuple", first, second, bridge);
            testMatcher("tuple<%s , %s>", "tuple", first, second, bridge);
        });
    }

    private void testMatcher(String pattern, String collection, CqlField.NativeType type, CassandraBridge bridge)
    {
        testMatcher(pattern, collection, type, null, bridge);
    }

    private void testMatcher(String pattern, String collection, CqlField.NativeType first, CqlField.NativeType second, CassandraBridge bridge)
    {
        boolean isMap = second != null;
        String string;
        if (first == null && second == null)
        {
            string = pattern;
        }
        else if (second == null)
        {
            string = String.format(pattern, first);
        }
        else
        {
            string = String.format(pattern, first, second);
        }

        Matcher matcher = CassandraTypes.COLLECTION_PATTERN.matcher(string);
        assertEquals(collection != null && first != null, matcher.matches());
        if (matcher.matches())
        {
            assertNotNull(collection);
            assertNotNull(first);
            assertEquals(collection, matcher.group(1));
            String[] types = CassandraTypes.splitInnerTypes(matcher.group(2));
            assertEquals(first, bridge.nativeType(types[0].toUpperCase()));
            if (isMap)
            {
                assertEquals(second, bridge.nativeType(types[1].toUpperCase()));
            }
        }
        else
        {
            // Raw CQL3 data type
            bridge.nativeType(pattern.toUpperCase());
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testFrozenMatcher(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> testFrozen("frozen<set<%s>>", CqlField.CqlSet.class, type, bridge));
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> testFrozen("frozen<list<%s>>", CqlField.CqlList.class, type, bridge));
        qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge)).checkAssert((first, second) -> {
            testFrozen("frozen<map<%s,%s>>", CqlField.CqlMap.class, first, second, bridge);
            testFrozen("frozen<map<%s , %s>>", CqlField.CqlMap.class, first, second, bridge);
        });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNestedFrozenSet(CassandraBridge bridge)
    {
        String pattern = "map<text, frozen<set<bigint>>>";
        CqlField.CqlType type = bridge.parseType(pattern);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeType);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertSame(key, bridge.text());
        CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        CqlField.CqlSet inner = (CqlField.CqlSet) value.inner();
        assertSame(inner.type(), bridge.bigint());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNestedFrozenMap(CassandraBridge bridge)
    {
        String pattern = "map<text, frozen<map<bigint, text>>>";
        CqlField.CqlType type = bridge.parseType(pattern);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeType);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertSame(key, bridge.text());
        CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        CqlField.CqlMap inner = (CqlField.CqlMap) value.inner();
        assertSame(inner.keyType(), bridge.bigint());
        assertSame(inner.valueType(), bridge.text());
    }

    private void testFrozen(String pattern,
                            Class<? extends CqlField.CqlCollection> collectionType,
                            CqlField.CqlType innerType, CassandraBridge bridge)
    {
        testFrozen(pattern, collectionType, innerType, null, bridge);
    }

    private void testFrozen(String pattern,
                            Class<? extends CqlField.CqlCollection> collectionType,
                            CqlField.CqlType first,
                            @Nullable CqlField.CqlType second, CassandraBridge bridge)
    {
        pattern = second != null ? String.format(pattern, first, second) : String.format(pattern, first);
        CqlField.CqlType type = bridge.parseType(pattern);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlFrozen);
        CqlField.CqlFrozen frozen = (CqlField.CqlFrozen) type;
        CqlField.CqlCollection inner = (CqlField.CqlCollection) frozen.inner();
        assertNotNull(inner);
        assertTrue(collectionType.isInstance(inner));
        assertEquals(first, inner.type());
        if (second != null)
        {
            CqlField.CqlMap map = (CqlField.CqlMap) inner;
            assertEquals(second, map.valueType());
        }
    }

    /* User-Defined Types */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUdts(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        String keyspace = "udt_keyspace";
        String udtName = "udt_name";
        CqlTable table = bridge.buildSchema("CREATE TABLE " + keyspace + ".udt_test (\n"
                                          + "    account_id uuid PRIMARY KEY,\n"
                                          + "    balance bigint,\n"
                                          + "    info frozen<" + udtName + ">,\n"
                                          + "    name text\n"
                                          + ");", keyspace, replicationFactor, Partitioner.Murmur3Partitioner,
                                            ImmutableSet.of("CREATE TYPE " + keyspace + "." + udtName + " (\n"
                                                          + "  birthday timestamp,\n"
                                                          + "  nationality text,\n"
                                                          + "  weight float,\n"
                                                          + "  height int\n"
                                                          + ");"));
        assertEquals(1, table.udts().size());
        CqlField.CqlUdt udt = table.udts().stream().findFirst().get();
        assertEquals(udtName, udt.name());
        List<CqlField> udtFields = udt.fields();
        assertEquals(4, udtFields.size());
        assertEquals(bridge.timestamp(), udtFields.get(0).type());
        assertEquals(bridge.text(), udtFields.get(1).type());
        assertEquals(bridge.aFloat(), udtFields.get(2).type());
        assertEquals(bridge.aInt(), udtFields.get(3).type());

        List<CqlField> fields = table.fields();
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        assertEquals(bridge.text(), fields.get(3).type());

        CqlField.CqlFrozen frozenField = (CqlField.CqlFrozen) fields.get(2).type();
        assertEquals(CqlField.CqlType.InternalType.Udt, frozenField.inner().internalType());

        CqlField.CqlUdt udtField = (CqlField.CqlUdt) frozenField.inner();
        assertEquals(bridge.timestamp(), udtField.field(0).type());
        assertEquals(bridge.text(), udtField.field(1).type());
        assertEquals(bridge.aFloat(), udtField.field(2).type());
        assertEquals(bridge.aInt(), udtField.field(3).type());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCollectionUdts(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        String keyspace = "collection_keyspace";
        String udtName = "basic_info";
        CqlTable table = bridge.buildSchema("CREATE TABLE " + keyspace + "." + udtName + " (\n"
                                          + "    account_id uuid PRIMARY KEY,\n"
                                          + "    balance bigint,\n"
                                          + "    info frozen<map<text, " + udtName + ">>,\n"
                                          + "    name text\n"
                                          + ");", "collection_keyspace", replicationFactor, Partitioner.Murmur3Partitioner,
                                            ImmutableSet.of("CREATE TYPE " + keyspace + "." + udtName + " (\n"
                                                         + "  birthday timestamp,\n"
                                                         + "  nationality text,\n"
                                                         + "  weight float,\n"
                                                         + "  height int\n"
                                                         + ");"));
        List<CqlField> fields = table.fields();
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        assertEquals(bridge.text(), fields.get(3).type());

        CqlField.CqlMap mapField = (CqlField.CqlMap) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(bridge.text(), mapField.keyType());
        CqlField.CqlFrozen valueType = (CqlField.CqlFrozen) mapField.valueType();
        CqlField.CqlUdt udtField = (CqlField.CqlUdt) valueType.inner();
        assertEquals(bridge.timestamp(), udtField.field(0).type());
        assertEquals(bridge.text(), udtField.field(1).type());
        assertEquals(bridge.aFloat(), udtField.field(2).type());
        assertEquals(bridge.aInt(), udtField.field(3).type());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testParseUdt(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor, Partitioner.Murmur3Partitioner,
                ImmutableSet.of("CREATE TYPE backup_test.tuple_test (a int, b bigint, c blob, d text)"));
        assertEquals(1, table.udts().size());
        CqlField.CqlUdt udt = table.udts().stream().findFirst().get();
        assertEquals("tuple_test", udt.name());
        List<CqlField> fields = udt.fields();
        assertEquals(4, fields.size());
        assertEquals(bridge.aInt(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(bridge.blob(), fields.get(2).type());
        assertEquals(bridge.text(), fields.get(3).type());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testParseTuple(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema("CREATE TABLE tuple_keyspace.tuple_test (\n"
                                          + "    account_id uuid PRIMARY KEY,\n"
                                          + "    balance bigint,\n"
                                          + "    info tuple<bigint, text, float, boolean>,"
                                          + "    name text\n"
                                          + ")", "tuple_keyspace", replicationFactor, Partitioner.Murmur3Partitioner);
        List<CqlField> fields = table.fields();
        assertEquals(4, fields.size());
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(bridge.text(), fields.get(3).type());

        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        CqlField.CqlTuple tuple = (CqlField.CqlTuple) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(bridge.bigint(), tuple.type(0));
        assertEquals(bridge.text(), tuple.type(1));
        assertEquals(bridge.aFloat(), tuple.type(2));
        assertEquals(bridge.bool(), tuple.type(3));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testComplexSchema(CassandraBridge bridge)
    {
        String keyspace = "complex_schema1";
        String type1 = "CREATE TYPE " + keyspace + ".field_with_timestamp (\n"
                     + "    field text,\n"
                     + "    \"timeWithZone\" frozen<" + keyspace + ".analytics_time_with_zone>\n"
                     + ");";
        String type2 = "CREATE TYPE " + keyspace + ".first_last_seen_fields_v1 (\n"
                     + "    \"firstSeen\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"lastSeen\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"firstTransaction\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"lastTransaction\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"firstListening\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"lastListening\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"firstReading\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"lastReading\" frozen<" + keyspace + ".field_with_timestamp>,\n"
                     + "    \"outputEvent\" text,\n"
                     + "    \"eventHistory\" frozen<map<bigint, frozen<map<text, boolean>>>>\n"
                     + ");";
        String type3 = "CREATE TYPE " + keyspace + ".analytics_time_with_zone (\n"
                     + "    time bigint,\n"
                     + "    \"timezoneOffsetMinutes\" int\n"
                     + ");";
        String type4 = "CREATE TYPE " + keyspace + ".first_last_seen_dimensions_v1 (\n"
                     + "    \"osMajorVersion\" text,\n"
                     + "    \"storeFrontId\" text,\n"
                     + "    platform text,\n"
                     + "    time_range text\n"
                     + ");";
        String tableStr = "CREATE TABLE " + keyspace + ".books_ltd_v3 (\n"
                        + "    \"consumerId\" text,\n"
                        + "    dimensions frozen<" + keyspace + ".first_last_seen_dimensions_v1>,\n"
                        + "    fields frozen<" + keyspace + ".first_last_seen_fields_v1>,\n"
                        + "    first_transition_time frozen<" + keyspace + ".analytics_time_with_zone>,\n"
                        + "    last_transition_time frozen<" + keyspace + ".analytics_time_with_zone>,\n"
                        + "    prev_state_id text,\n"
                        + "    state_id text,\n"
                        + "    PRIMARY KEY (\"consumerId\", dimensions)\n"
                        + ") WITH CLUSTERING ORDER BY (dimensions ASC);";
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(tableStr, keyspace, replicationFactor, Partitioner.Murmur3Partitioner,
                ImmutableSet.of(type1, type2, type3, type4));
        assertEquals("books_ltd_v3", table.table());
        assertEquals(keyspace, table.keyspace());
        assertEquals(7, table.fields().size());
        assertEquals(1, table.partitionKeys().size());
        assertEquals(1, table.clusteringKeys().size());

        List<CqlField> fields = table.fields();
        assertEquals(7, fields.size());
        assertEquals("consumerId", fields.get(0).name());
        assertEquals(bridge.text(), fields.get(0).type());
        CqlField clusteringKey = fields.get(1);
        assertEquals("dimensions", clusteringKey.name());
        assertEquals(CqlField.CqlType.InternalType.Frozen, clusteringKey.type().internalType());

        CqlField.CqlUdt clusteringUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) clusteringKey.type()).inner();
        assertEquals("first_last_seen_dimensions_v1", clusteringUDT.name());
        assertEquals(keyspace, clusteringUDT.keyspace());
        assertEquals("osMajorVersion", clusteringUDT.field(0).name());
        assertEquals(bridge.text(), clusteringUDT.field(0).type());
        assertEquals("storeFrontId", clusteringUDT.field(1).name());
        assertEquals(bridge.text(), clusteringUDT.field(1).type());
        assertEquals("platform", clusteringUDT.field(2).name());
        assertEquals(bridge.text(), clusteringUDT.field(2).type());
        assertEquals("time_range", clusteringUDT.field(3).name());
        assertEquals(bridge.text(), clusteringUDT.field(3).type());

        CqlField.CqlUdt fieldsUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals("first_last_seen_fields_v1", fieldsUDT.name());
        assertEquals("firstSeen", fieldsUDT.field(0).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(0).type()).inner().name());
        assertEquals("lastSeen", fieldsUDT.field(1).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(1).type()).inner().name());
        assertEquals("firstTransaction", fieldsUDT.field(2).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(2).type()).inner().name());
        assertEquals("lastTransaction", fieldsUDT.field(3).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(3).type()).inner().name());
        assertEquals("firstListening", fieldsUDT.field(4).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(4).type()).inner().name());
        assertEquals("lastListening", fieldsUDT.field(5).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(5).type()).inner().name());
        assertEquals("firstReading", fieldsUDT.field(6).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(6).type()).inner().name());
        assertEquals("lastReading", fieldsUDT.field(7).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(7).type()).inner().name());
        assertEquals("outputEvent", fieldsUDT.field(8).name());
        assertEquals(bridge.text(), fieldsUDT.field(8).type());
        assertEquals("eventHistory", fieldsUDT.field(9).name());
        assertEquals(bridge.bigint(),
                     ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).keyType());
        assertEquals(CqlField.CqlType.InternalType.Frozen,
                     ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).valueType().internalType());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNestedUDTs(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        String keyspace = "nested_udt_schema";
        CqlTable table = bridge.buildSchema("CREATE TABLE " + keyspace + ".udt_test (\n"
                                          + "    a uuid,\n"
                                          + "    b bigint,\n"
                                          + "    c frozen<a_udt>,\n"
                                          + "    PRIMARY KEY(a));", keyspace, replicationFactor, Partitioner.Murmur3Partitioner,
                                            ImmutableSet.of("CREATE TYPE " + keyspace + ".a_udt (col1 bigint, col2 text, col3 frozen<map<uuid, b_udt>>);",
                                                           "CREATE TYPE " + keyspace + ".b_udt (col1 timeuuid, col2 text, col3 frozen<set<c_udt>>);",
                                                           "CREATE TYPE " + keyspace + ".c_udt (col1 float, col2 uuid, col3 int);"));
        assertEquals(3, table.udts().size());
    }
}
