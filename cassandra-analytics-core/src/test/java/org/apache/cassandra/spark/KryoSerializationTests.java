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

package org.apache.cassandra.spark;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;

public class KryoSerializationTests extends VersionRunner
{
    private static final Kryo KRYO = new Kryo();

    static
    {
        new KryoRegister().registerClasses(KRYO);
    }

    public KryoSerializationTests(CassandraVersion version)
    {
        super(version);
    }

    private static Output serialize(Object object)
    {
        try (Output out = new Output(1024, -1))
        {
            KRYO.writeObject(out, object);
            return out;
        }
    }

    private static <T> T deserialize(Output output, Class<T> type)
    {
        try (Input in = new Input(output.getBuffer(), 0, output.position()))
        {
            return KRYO.readObject(in, type);
        }
    }

    @Test
    public void testCqlField()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, position) -> {
                CqlField field = new CqlField(isPartitionKey,
                                              isClusteringKey && !isPartitionKey,
                                              false,
                                              RandomStringUtils.randomAlphanumeric(5, 20),
                                              cqlType,
                                              position);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldSet()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, position) -> {
                CqlField.CqlSet setType = bridge.set(cqlType);
                CqlField field = new CqlField(isPartitionKey,
                                              isClusteringKey && !isPartitionKey,
                                              false,
                                              RandomStringUtils.randomAlphanumeric(5, 20),
                                              setType,
                                              position);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldList()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, position) -> {
                CqlField.CqlList listType = bridge.list(cqlType);
                CqlField field = new CqlField(isPartitionKey,
                                              isClusteringKey && !isPartitionKey,
                                              false,
                                              RandomStringUtils.randomAlphanumeric(5, 20),
                                              listType,
                                              position);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldMap()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((isPartitionKey, isClusteringKey, cqlType1, cqlType2) -> {
                CqlField.CqlMap mapType = bridge.map(cqlType1, cqlType2);
                CqlField field = new CqlField(isPartitionKey,
                                              isClusteringKey && !isPartitionKey,
                                              false,
                                              RandomStringUtils.randomAlphanumeric(5, 20),
                                              mapType,
                                              2);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlUdt()
    {
        qt().withExamples(25)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> {
                CqlField.CqlUdt udt = bridge.udt("keyspace", "testudt")
                                            .withField("a", type1)
                                            .withField("b", type2)
                                            .build();
                CqlField field = new CqlField(false, false, false, RandomStringUtils.randomAlphanumeric(5, 20), udt, 2);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(udt, deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlTuple()
    {
        qt().withExamples(25)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> {
                CqlField.CqlTuple tuple = bridge.tuple(type1,
                                                       bridge.blob(),
                                                       type2,
                                                       bridge.set(bridge.text()),
                                                       bridge.bigint(),
                                                       bridge.map(type2, bridge.timeuuid()));
                CqlField field = new CqlField(false, false, false, RandomStringUtils.randomAlphanumeric(5, 20), tuple, 2);
                Output out = serialize(field);
                CqlField deserialized = deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(tuple, deserialized.type());
                assertEquals(field.position(), deserialized.position());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlTable()
    {
        List<CqlField> fields = ImmutableList.of(new CqlField(true, false, false, "a", bridge.bigint(), 0),
                                                 new CqlField(true, false, false, "b", bridge.bigint(), 1),
                                                 new CqlField(false, true, false, "c", bridge.bigint(), 2),
                                                 new CqlField(false, false, false, "d", bridge.timestamp(), 3),
                                                 new CqlField(false, false, false, "e", bridge.text(), 4));
        ReplicationFactor replicationFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                    ImmutableMap.of("DC1", 3, "DC2", 3));
        CqlTable table = new CqlTable("test_keyspace",
                                      "test_table",
                                      "create table test_keyspace.test_table"
                                    + " (a bigint, b bigint, c bigint, d bigint, e bigint, primary key((a, b), c));",
                                      replicationFactor,
                                      fields);

        Output out = serialize(table);
        CqlTable deserialized = deserialize(out, CqlTable.class);
        assertNotNull(deserialized);
        assertEquals(table, deserialized);
    }

    @Test
    public void testCassandraInstance()
    {
        CassandraInstance instance = new CassandraInstance("-9223372036854775807", "local1-i1", "DC1");
        Output out = serialize(instance);
        CassandraInstance deserialized = deserialize(out, CassandraInstance.class);
        assertNotNull(deserialized);
        assertEquals(instance, deserialized);
    }

    @Test
    public void testCassandraRing()
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert(partitioner -> {
                CassandraRing ring = TestUtils.createRing(partitioner, ImmutableMap.of("DC1", 3, "DC2", 3));
                Output out = serialize(ring);
                CassandraRing deserialized = deserialize(out, CassandraRing.class);
                assertNotNull(deserialized);
                assertEquals(ring, deserialized);
                assertEquals(partitioner, deserialized.partitioner());
            });
    }

    @Test
    public void testLocalDataLayer()
    {
        String path1 = UUID.randomUUID().toString();
        String path2 = UUID.randomUUID().toString();
        String path3 = UUID.randomUUID().toString();
        LocalDataLayer localDataLayer = new LocalDataLayer(bridge.getVersion(),
                                                           "test_keyspace",
                                                           "create table test_keyspace.test_table"
                                                         + " (a int, b int, c int, primary key(a, b));",
                                                           path1,
                                                           path2,
                                                           path3);
        Output out = serialize(localDataLayer);
        LocalDataLayer deserialized = deserialize(out, LocalDataLayer.class);
        assertNotNull(deserialized);
        assertEquals(localDataLayer.version(), deserialized.version());
        assertEquals(localDataLayer, deserialized);
    }

    @Test
    public void testTokenPartitioner()
    {
        qt().forAll(TestUtils.partitioners(),
                    arbitrary().pick(Arrays.asList(3, 16, 128)),
                    arbitrary().pick(Arrays.asList(1, 4, 16)),
                    arbitrary().pick(Arrays.asList(4, 16, 64)))
            .checkAssert((partitioner, numInstances, defaultParallelism, numCores) -> {
                CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
                Output out = serialize(tokenPartitioner);
                TokenPartitioner deserialized = deserialize(out, TokenPartitioner.class);
                assertNotNull(deserialized);
                assertEquals(tokenPartitioner.numPartitions(), deserialized.numPartitions());
                assertEquals(tokenPartitioner.subRanges().size(), deserialized.subRanges().size());
                for (int index = 0; index < tokenPartitioner.subRanges().size(); index++)
                {
                    assertEquals(tokenPartitioner.subRanges().get(index), deserialized.subRanges().get(index));
                }
                assertEquals(tokenPartitioner.ring(), deserialized.ring());
            });
    }

    @Test
    public void testCqlUdtField()
    {
        CqlField.CqlUdt udt = bridge.udt("udt_keyspace", "udt_table")
                                    .withField("c", bridge.text())
                                    .withField("b", bridge.timestamp())
                                    .withField("a", bridge.bigint())
                                    .build();
        Output out = new Output(1024, -1);
        udt.write(out);
        out.close();
        Input in = new Input(out.getBuffer(), 0, out.position());
        CqlField.CqlUdt deserialized = (CqlField.CqlUdt) CqlField.CqlType.read(in, bridge);
        assertEquals(udt, deserialized);
        for (int index = 0; index < deserialized.fields().size(); index++)
        {
            assertEquals(udt.field(index), deserialized.field(index));
        }
    }

    @Test
    public void testSslConfig()
    {
        SslConfig config = new SslConfig.Builder<>()
                                     .keyStorePath("keyStorePath")
                                     .base64EncodedKeyStore("encodedKeyStore")
                                     .keyStorePassword("keyStorePassword")
                                     .keyStoreType("keyStoreType")
                                     .trustStorePath("trustStorePath")
                                     .base64EncodedTrustStore("encodedTrustStore")
                                     .trustStorePassword("trustStorePassword")
                                     .trustStoreType("trustStoreType")
                                     .build();
        Output out = serialize(config);
        SslConfig deserialized = deserialize(out, SslConfig.class);

        assertEquals(config.keyStorePath(), deserialized.keyStorePath());
        assertEquals(config.base64EncodedKeyStore(), deserialized.base64EncodedKeyStore());
        assertEquals(config.keyStorePassword(), deserialized.keyStorePassword());
        assertEquals(config.keyStoreType(), deserialized.keyStoreType());
        assertEquals(config.trustStorePath(), deserialized.trustStorePath());
        assertEquals(config.base64EncodedTrustStore(), deserialized.base64EncodedTrustStore());
        assertEquals(config.trustStorePassword(), deserialized.trustStorePassword());
        assertEquals(config.trustStoreType(), deserialized.trustStoreType());
    }
}
