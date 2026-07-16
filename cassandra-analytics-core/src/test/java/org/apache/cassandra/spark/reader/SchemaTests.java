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
import java.util.Collections;
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

import static org.apache.cassandra.spark.utils.MapUtils.mapOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
                                        + "'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
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
        assertThat(fields).isNotNull();
        assertThat(fields).hasSize(3);
        assertThat(fields.get(0).name()).isEqualTo("account_id");
        assertThat(fields.get(1).name()).isEqualTo("balance");
        assertThat(fields.get(2).name()).isEqualTo("name");
        assertThat(table.createStatement()).isEqualTo(SCHEMA);
        assertThat(table.replicationFactor().getOptions().get("DC1")).isEqualTo(3);
        assertThat(table.replicationFactor().getOptions().get("DC2")).isEqualTo(3);
        assertThat(table.replicationFactor().getOptions().get("DC3")).isNull();
        assertThat(table.numPartitionKeys()).isEqualTo(1);
        assertThat(table.numClusteringKeys()).isEqualTo(0);
        assertThat(table.numStaticColumns()).isEqualTo(0);
        assertThat(table.numValueColumns()).isEqualTo(2);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEquality(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table1 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        CqlTable table2 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        assertThat(table1).isNotSameAs(table2);
        assertThat(table2).isNotEqualTo(null);
        assertThat(table1).isNotEqualTo(null);
        assertThat(table1).isNotEqualTo(new ArrayList<>());
        assertThat(table1).isEqualTo(table1);
        assertThat(table1).isEqualTo(table2);
        assertThat(table1.hashCode()).isEqualTo(table2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSameKeyspace(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table1 = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor);
        CqlTable table2 = bridge.buildSchema(SCHEMA.replace("sbr_test", "sbr_test2"), "backup_test", replicationFactor);
        assertThat(table1).isNotSameAs(table2);
        assertThat(table2.table()).isEqualTo("sbr_test2");
        assertThat(table1.table()).isEqualTo("sbr_test");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testHasher(CassandraBridge bridge)
    {
        // Casts to (ByteBuffer) required when compiling with Java 8
        assertThat(bridge.hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(992393994949L).flip()))
                .isEqualTo(BigInteger.valueOf(6747049197585865300L));
        assertThat(bridge.hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(999).flip()))
                .isEqualTo(BigInteger.valueOf(7071430368280192841L));
        assertThat(bridge.hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(34828288292L).flip()))
                .isEqualTo(new BigInteger("28812675363873787366858706534556752548"));
        assertThat(bridge.hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(1929239).flip()))
                .isEqualTo(new BigInteger("154860613751552680515987154638148676974"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUUID(CassandraBridge bridge)
    {
        assertThat(bridge.getTimeUUID().version()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCollections(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.collection_test (account_id uuid PRIMARY KEY, balance bigint, names set<text>);";
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(createStatement, "backup_test", replicationFactor);
        assertThat(table.getField("names").type().internalType()).isEqualTo(CqlField.CqlType.InternalType.Set);
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
        assertThatThrownBy(() -> bridge.buildSchema(createStatement, "backup_test", replicationFactor))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUnsupportedInnerType(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test (account_id uuid, transactions counter, PRIMARY KEY(account_id));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        assertThatThrownBy(() -> bridge.buildSchema(createStatement, "backup_test", replicationFactor))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testUnsupportedUdt(CassandraBridge bridge)
    {
        String createStatement = "CREATE TABLE backup_test.sbr_test (account_id uuid, transactions frozen<testudt>, PRIMARY KEY (account_id));";
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        assertThatThrownBy(() -> bridge.buildSchema(createStatement, "backup_test", replicationFactor, Partitioner.Murmur3Partitioner,
                                                    ImmutableSet.of("CREATE TYPE backup_test.testudt(birthday timestamp, count bigint, length counter);"),
                                                    null, Collections.emptySet(), false))
                .isInstanceOf(UnsupportedOperationException.class);
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
        assertThat(collection != null && first != null).isEqualTo(matcher.matches());
        if (matcher.matches())
        {
            assertThat(collection).isNotNull();
            assertThat(first).isNotNull();
            assertThat(matcher.group(1)).isEqualTo(collection);
            String[] types = CassandraTypes.splitInnerTypes(matcher.group(2));
            assertThat(bridge.nativeType(types[0].toUpperCase())).isEqualTo(first);
            if (isMap)
            {
                assertThat(bridge.nativeType(types[1].toUpperCase())).isEqualTo(second);
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
        assertThat(type).isNotNull();
        assertThat(type).isInstanceOf(CqlField.CqlMap.class);
        CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertThat(map.keyType()).isInstanceOf(CqlField.NativeType.class);
        assertThat(map.valueType()).isInstanceOf(CqlField.CqlFrozen.class);
        CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertThat(key).isSameAs(bridge.text());
        CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        CqlField.CqlSet inner = (CqlField.CqlSet) value.inner();
        assertThat(inner.type()).isSameAs(bridge.bigint());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNestedFrozenMap(CassandraBridge bridge)
    {
        String pattern = "map<text, frozen<map<bigint, text>>>";
        CqlField.CqlType type = bridge.parseType(pattern);
        assertThat(type).isNotNull();
        assertThat(type).isInstanceOf(CqlField.CqlMap.class);
        CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertThat(map.keyType()).isInstanceOf(CqlField.NativeType.class);
        assertThat(map.valueType()).isInstanceOf(CqlField.CqlFrozen.class);
        CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertThat(key).isSameAs(bridge.text());
        CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        CqlField.CqlMap inner = (CqlField.CqlMap) value.inner();
        assertThat(inner.keyType()).isSameAs(bridge.bigint());
        assertThat(inner.valueType()).isSameAs(bridge.text());
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
        assertThat(type).isNotNull();
        assertThat(type).isInstanceOf(CqlField.CqlFrozen.class);
        CqlField.CqlFrozen frozen = (CqlField.CqlFrozen) type;
        CqlField.CqlCollection inner = (CqlField.CqlCollection) frozen.inner();
        assertThat(inner).isNotNull();
        assertThat(collectionType.isInstance(inner)).isTrue();
        assertThat(inner.type()).isEqualTo(first);
        if (second != null)
        {
            CqlField.CqlMap map = (CqlField.CqlMap) inner;
            assertThat(map.valueType()).isEqualTo(second);
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
                                                            + ");"), null, Collections.emptySet(), false);
        assertThat(table.udts()).hasSize(1);
        CqlField.CqlUdt udt = table.udts().stream().findFirst().get();
        assertThat(udt.name()).isEqualTo(udtName);
        List<CqlField> udtFields = udt.fields();
        assertThat(udtFields).hasSize(4);
        assertThat(udtFields.get(0).type()).isEqualTo(bridge.timestamp());
        assertThat(udtFields.get(1).type()).isEqualTo(bridge.text());
        assertThat(udtFields.get(2).type()).isEqualTo(bridge.aFloat());
        assertThat(udtFields.get(3).type()).isEqualTo(bridge.aInt());

        List<CqlField> fields = table.fields();
        assertThat(fields.get(0).type()).isEqualTo(bridge.uuid());
        assertThat(fields.get(1).type()).isEqualTo(bridge.bigint());
        assertThat(fields.get(2).type().internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        assertThat(fields.get(3).type()).isEqualTo(bridge.text());

        CqlField.CqlFrozen frozenField = (CqlField.CqlFrozen) fields.get(2).type();
        assertThat(frozenField.inner().internalType()).isEqualTo(CqlField.CqlType.InternalType.Udt);

        CqlField.CqlUdt udtField = (CqlField.CqlUdt) frozenField.inner();
        assertThat(udtField.field(0).type()).isEqualTo(bridge.timestamp());
        assertThat(udtField.field(1).type()).isEqualTo(bridge.text());
        assertThat(udtField.field(2).type()).isEqualTo(bridge.aFloat());
        assertThat(udtField.field(3).type()).isEqualTo(bridge.aInt());
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
                                                            + ");"), null, Collections.emptySet(), false);
        List<CqlField> fields = table.fields();
        assertThat(fields.get(0).type()).isEqualTo(bridge.uuid());
        assertThat(fields.get(1).type()).isEqualTo(bridge.bigint());
        assertThat(fields.get(2).type().internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        assertThat(fields.get(3).type()).isEqualTo(bridge.text());

        CqlField.CqlMap mapField = (CqlField.CqlMap) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertThat(mapField.keyType()).isEqualTo(bridge.text());
        CqlField.CqlFrozen valueType = (CqlField.CqlFrozen) mapField.valueType();
        CqlField.CqlUdt udtField = (CqlField.CqlUdt) valueType.inner();
        assertThat(udtField.field(0).type()).isEqualTo(bridge.timestamp());
        assertThat(udtField.field(1).type()).isEqualTo(bridge.text());
        assertThat(udtField.field(2).type()).isEqualTo(bridge.aFloat());
        assertThat(udtField.field(3).type()).isEqualTo(bridge.aInt());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testParseUdt(CassandraBridge bridge)
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(
        ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = bridge.buildSchema(SCHEMA, "backup_test", replicationFactor, Partitioner.Murmur3Partitioner,
                                            ImmutableSet.of("CREATE TYPE backup_test.tuple_test (a int, b bigint, c blob, d text)"),
                                            null, Collections.emptySet(), false);
        assertThat(table.udts()).hasSize(1);
        CqlField.CqlUdt udt = table.udts().stream().findFirst().get();
        assertThat(udt.name()).isEqualTo("tuple_test");
        List<CqlField> fields = udt.fields();
        assertThat(fields).hasSize(4);
        assertThat(fields.get(0).type()).isEqualTo(bridge.aInt());
        assertThat(fields.get(1).type()).isEqualTo(bridge.bigint());
        assertThat(fields.get(2).type()).isEqualTo(bridge.blob());
        assertThat(fields.get(3).type()).isEqualTo(bridge.text());
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
        assertThat(fields).hasSize(4);
        assertThat(fields.get(0).type()).isEqualTo(bridge.uuid());
        assertThat(fields.get(1).type()).isEqualTo(bridge.bigint());
        assertThat(fields.get(3).type()).isEqualTo(bridge.text());

        assertThat(fields.get(2).type().internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlTuple tuple = (CqlField.CqlTuple) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertThat(tuple.type(0)).isEqualTo(bridge.bigint());
        assertThat(tuple.type(1)).isEqualTo(bridge.text());
        assertThat(tuple.type(2)).isEqualTo(bridge.aFloat());
        assertThat(tuple.type(3)).isEqualTo(bridge.bool());
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
                                            ImmutableSet.of(type1, type2, type3, type4),
                                            null, Collections.emptySet(), false);
        assertThat(table.table()).isEqualTo("books_ltd_v3");
        assertThat(table.keyspace()).isEqualTo(keyspace);
        assertThat(table.fields()).hasSize(7);
        assertThat(table.partitionKeys()).hasSize(1);
        assertThat(table.clusteringKeys()).hasSize(1);

        List<CqlField> fields = table.fields();
        assertThat(fields).hasSize(7);
        assertThat(fields.get(0).name()).isEqualTo("consumerId");
        assertThat(fields.get(0).type()).isEqualTo(bridge.text());
        CqlField clusteringKey = fields.get(1);
        assertThat(clusteringKey.name()).isEqualTo("dimensions");
        assertThat(clusteringKey.type().internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);

        CqlField.CqlUdt clusteringUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) clusteringKey.type()).inner();
        assertThat(clusteringUDT.name()).isEqualTo("first_last_seen_dimensions_v1");
        assertThat(clusteringUDT.keyspace()).isEqualTo(keyspace);
        assertThat(clusteringUDT.field(0).name()).isEqualTo("osMajorVersion");
        assertThat(clusteringUDT.field(0).type()).isEqualTo(bridge.text());
        assertThat(clusteringUDT.field(1).name()).isEqualTo("storeFrontId");
        assertThat(clusteringUDT.field(1).type()).isEqualTo(bridge.text());
        assertThat(clusteringUDT.field(2).name()).isEqualTo("platform");
        assertThat(clusteringUDT.field(2).type()).isEqualTo(bridge.text());
        assertThat(clusteringUDT.field(3).name()).isEqualTo("time_range");
        assertThat(clusteringUDT.field(3).type()).isEqualTo(bridge.text());

        CqlField.CqlUdt fieldsUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertThat(fieldsUDT.name()).isEqualTo("first_last_seen_fields_v1");
        assertThat(fieldsUDT.field(0).name()).isEqualTo("firstSeen");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(0).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(1).name()).isEqualTo("lastSeen");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(1).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(2).name()).isEqualTo("firstTransaction");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(2).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(3).name()).isEqualTo("lastTransaction");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(3).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(4).name()).isEqualTo("firstListening");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(4).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(5).name()).isEqualTo("lastListening");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(5).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(6).name()).isEqualTo("firstReading");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(6).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(7).name()).isEqualTo("lastReading");
        assertThat(((CqlField.CqlFrozen) fieldsUDT.field(7).type()).inner().name()).isEqualTo("field_with_timestamp");
        assertThat(fieldsUDT.field(8).name()).isEqualTo("outputEvent");
        assertThat(fieldsUDT.field(8).type()).isEqualTo(bridge.text());
        assertThat(fieldsUDT.field(9).name()).isEqualTo("eventHistory");
        assertThat(((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).keyType())
                .isEqualTo(bridge.bigint());
        assertThat(((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).valueType().internalType())
                .isEqualTo(CqlField.CqlType.InternalType.Frozen);
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
                                                            "CREATE TYPE " + keyspace + ".c_udt (col1 float, col2 uuid, col3 int);"),
                                            null, Collections.emptySet(), false);
        assertThat(table.udts()).hasSize(3);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSchemaOfTableChanges(CassandraBridge bridge)
    {
        ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, mapOf("DC1", 3));
        String createStatement1 = "CREATE TABLE test_ks.test_tbl1 (a int PRIMARY KEY, b int);";
        CqlTable schema1 = bridge.buildSchema(createStatement1, "test_ks", rf, Partitioner.Murmur3Partitioner);
        assertThat(schema1.fields()).hasSize(2);

        String createStatement2 = "CREATE TABLE test_ks.test_tbl2 (a int PRIMARY KEY, b int, c int);";
        CqlTable schema2 = bridge.buildSchema(createStatement2, "test_ks", rf, Partitioner.Murmur3Partitioner);
        assertThat(schema2.fields()).hasSize(3);
        assertThat(schema1).isNotEqualTo(schema2);
    }
}
