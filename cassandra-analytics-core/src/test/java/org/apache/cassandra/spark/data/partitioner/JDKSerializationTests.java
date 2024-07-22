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

package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.jetbrains.annotations.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class JDKSerializationTests extends VersionRunner
{

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCassandraRing(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)))
            .checkAssert(((partitioner, numInstances) -> {
                CassandraRing ring;
                if (numInstances > 4)
                {
                    ring = TestUtils.createRing(partitioner, ImmutableMap.of("DC1", numInstances / 2, "DC2", numInstances / 2));
                }
                else
                {
                    ring = TestUtils.createRing(partitioner, numInstances);
                }
                byte[] bytes = bridge.javaSerialize(ring);
                CassandraRing deserialized = bridge.javaDeserialize(bytes, CassandraRing.class);
                assertNotNull(deserialized);
                assertNotNull(deserialized.rangeMap());
                assertNotNull(deserialized.tokenRanges());
                assertEquals(ring, deserialized);
            }));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testTokenPartitioner(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.partitioners(),
                    arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)),
                    arbitrary().pick(Arrays.asList(1, 4, 8, 16, 32, 1024)))
            .checkAssert(((partitioner, numInstances, numCores) -> {
                CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 4, numCores);
                byte[] bytes = bridge.javaSerialize(tokenPartitioner);
                TokenPartitioner deserialized = bridge.javaDeserialize(bytes, TokenPartitioner.class);
                assertEquals(tokenPartitioner.ring(), deserialized.ring());
                assertEquals(tokenPartitioner.numPartitions(), deserialized.numPartitions());
                assertEquals(tokenPartitioner.subRanges(), deserialized.subRanges());
                assertEquals(tokenPartitioner.partitionMap(), deserialized.partitionMap());
                assertEquals(tokenPartitioner.reversePartitionMap(), deserialized.reversePartitionMap());
                for (int partition = 0; partition < tokenPartitioner.numPartitions(); partition++)
                {
                    assertEquals(tokenPartitioner.getTokenRange(partition), deserialized.getTokenRange(partition));
                }
            }));
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testPartitionedDataLayer(CassandraBridge bridge)
    {
        CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 1024);
        TestSchema schema = TestSchema.basic(bridge);
        CqlTable cqlTable = new CqlTable(schema.keyspace, schema.table, schema.createStatement, ring.replicationFactor(), Collections.emptyList());
        DataLayer partitionedDataLayer = new TestPartitionedDataLayer(bridge, 4, 16, null, ring, cqlTable);
        byte[] bytes = bridge.javaSerialize(partitionedDataLayer);
        TestPartitionedDataLayer deserialized = bridge.javaDeserialize(bytes, TestPartitionedDataLayer.class);
        assertNotNull(deserialized);
        assertNotNull(deserialized.ring());
        assertNotNull(deserialized.partitioner());
        assertNotNull(deserialized.tokenPartitioner());
        assertEquals(Partitioner.Murmur3Partitioner, deserialized.partitioner());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCqlFieldSet(CassandraBridge bridge)
    {
        CqlField.CqlSet setType = bridge.set(bridge.text());
        CqlField field = new CqlField(true, false, false, RandomUtils.randomAlphanumeric(5, 20), setType, 10);
        byte[] bytes = bridge.javaSerialize(field);
        CqlField deserialized = bridge.javaDeserialize(bytes, CqlField.class);
        assertEquals(field, deserialized);
        assertEquals(field.name(), deserialized.name());
        assertEquals(field.type(), deserialized.type());
        assertEquals(field.position(), deserialized.position());
        assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
        assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCqlUdt(CassandraBridge bridge)
    {
        CqlField.CqlUdt udt1 = bridge
                               .udt("udt_keyspace", "udt_table")
                               .withField("c", bridge.text())
                               .withField("b", bridge.timestamp())
                               .withField("a", bridge.bigint())
                               .build();
        CqlField.CqlUdt udt2 = bridge
                               .udt("udt_keyspace", "udt_table")
                               .withField("a", bridge.bigint())
                               .withField("b", bridge.timestamp())
                               .withField("c", bridge.text())
                               .build();
        assertNotEquals(udt2, udt1);
        byte[] bytes = bridge.javaSerialize(udt1);
        CqlField.CqlUdt deserialized = bridge.javaDeserialize(bytes, CqlField.CqlUdt.class);
        assertEquals(udt1, deserialized);
        assertNotEquals(udt2, deserialized);
        for (int field = 0; field < deserialized.fields().size(); field++)
        {
            assertEquals(udt1.field(field), deserialized.field(field));
        }
    }

    public static class TestPartitionedDataLayer extends PartitionedDataLayer
    {
        private CassandraBridge bridge;
        private CassandraRing ring;
        private CqlTable cqlTable;
        private TokenPartitioner tokenPartitioner;
        private final String jobId;

        public TestPartitionedDataLayer(CassandraBridge bridge,
                                        int defaultParallelism,
                                        int numCores,
                                        String dc,
                                        CassandraRing ring,
                                        CqlTable cqlTable)
        {
            super(ConsistencyLevel.LOCAL_QUORUM, dc);
            this.bridge = bridge;
            this.ring = ring;
            this.cqlTable = cqlTable;
            this.tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
            this.jobId = UUID.randomUUID().toString();
        }

        public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                               @NotNull Range<BigInteger> range,
                                                               @NotNull CassandraInstance instance)
        {
            return CompletableFuture.completedFuture(Stream.of());
        }

        @Override
        public CassandraBridge bridge()
        {
            return bridge;
        }

        @Override
        public CassandraRing ring()
        {
            return ring;
        }

        public TokenPartitioner tokenPartitioner()
        {
            return tokenPartitioner;
        }

        protected ExecutorService executorService()
        {
            return SingleReplicaTests.EXECUTOR;
        }

        public String jobId()
        {
            return jobId;
        }

        public CassandraVersion version()
        {
            return CassandraVersion.FOURZERO;
        }

        public CqlTable cqlTable()
        {
            return cqlTable;
        }

        @Override
        public TimeProvider timeProvider()
        {
            return TimeProvider.DEFAULT;
        }

        @Override
        public ReplicationFactor replicationFactor(String keyspace)
        {
            return ring.replicationFactor();
        }

        private void writeObject(ObjectOutputStream out) throws IOException
        {
            // Falling back to JDK serialization
            out.writeObject(version());
            out.writeObject(consistencyLevel);
            out.writeObject(datacenter);
            out.writeObject(ring);
            bridge.javaSerialize(out, cqlTable);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
            out.writeObject(tokenPartitioner);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
        {
            // Falling back to JDK deserialization
            bridge = CassandraBridgeFactory.get((CassandraVersion) in.readObject());
            consistencyLevel = (ConsistencyLevel) in.readObject();
            datacenter = (String) in.readObject();
            ring = (CassandraRing) in.readObject();
            cqlTable = bridge.javaDeserialize(in, CqlTable.class);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
            tokenPartitioner = (TokenPartitioner) in.readObject();
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSecretsConfig(CassandraBridge bridge)
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
        byte[] bytes = bridge.javaSerialize(config);
        SslConfig deserialized = bridge.javaDeserialize(bytes, SslConfig.class);

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
