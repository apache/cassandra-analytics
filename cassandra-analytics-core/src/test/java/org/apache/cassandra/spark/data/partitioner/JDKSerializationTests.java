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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;

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
                assertThat(deserialized).isNotNull();
                assertThat(deserialized.rangeMap()).isNotNull();
                assertThat(deserialized.tokenRanges()).isNotNull();
                assertThat(deserialized).isEqualTo(ring);
            }));
    }

    private void testCassandraRingSerializationWithReplicasForRanges(CassandraBridge bridge,
                                                                     Partitioner partitioner,
                                                                     List<CassandraInstance> instances,
                                                                     RangeMap<BigInteger, List<CassandraInstance>> replicasForRanges,
                                                                     ReplicationFactor replicationFactor,
                                                                     int expectedRangeCount)
    {
        CassandraRing ring = new CassandraRing(partitioner, "test_keyspace", replicationFactor, instances, replicasForRanges);

        byte[] bytes = bridge.javaSerialize(ring);
        CassandraRing deserialized = bridge.javaDeserialize(bytes, CassandraRing.class);

        assertThat(deserialized).isNotNull();
        assertThat(deserialized.rangeMap()).isNotNull();
        assertThat(deserialized.rangeMap().asMapOfRanges()).hasSize(expectedRangeCount);
        assertThat(deserialized).isEqualTo(ring);
        assertThat(deserialized.rangeMap().asMapOfRanges()).isEqualTo(ring.rangeMap().asMapOfRanges());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCassandraRingWithReplicasForRanges(CassandraBridge bridge)
    {
        // Create test instances that will be reused across different configurations
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("-1000", "node1", "DC1"),
        new CassandraInstance("0", "node2", "DC1"),
        new CassandraInstance("1000", "node3", "DC2")
        );

        // Create different replicasForRanges configurations to test
        List<RangeMap<BigInteger, List<CassandraInstance>>> replicasForRangesList = new ArrayList<>();

        // Configuration 1: Basic two-range setup
        RangeMap<BigInteger, List<CassandraInstance>> config1 = TreeRangeMap.create();
        config1.put(Range.openClosed(new BigInteger("-1000"), BigInteger.ZERO),
                    Arrays.asList(instances.get(0), instances.get(1)));
        config1.put(Range.openClosed(BigInteger.ZERO, new BigInteger("1000")),
                    Arrays.asList(instances.get(1), instances.get(2)));
        replicasForRangesList.add(config1);

        // Configuration 2: Single range with all replicas
        RangeMap<BigInteger, List<CassandraInstance>> config2 = TreeRangeMap.create();
        config2.put(Range.openClosed(new BigInteger("-500"), new BigInteger("500")),
                    Arrays.asList(instances.get(0), instances.get(1), instances.get(2)));
        replicasForRangesList.add(config2);

        // Configuration 3: Range with empty replica list
        RangeMap<BigInteger, List<CassandraInstance>> config4 = TreeRangeMap.create();
        config4.put(Range.openClosed(new BigInteger("-100"), BigInteger.ZERO),
                    Collections.emptyList());
        config4.put(Range.openClosed(BigInteger.ZERO, new BigInteger("100")),
                    Arrays.asList(instances.get(1)));
        replicasForRangesList.add(config4);

        // Configuration 4: Token ranges with boundary values (Long.MAX_VALUE, Long.MIN_VALUE)
        RangeMap<BigInteger, List<CassandraInstance>> config5 = TreeRangeMap.create();
        config5.put(Range.openClosed(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.ZERO),
                    Arrays.asList(instances.get(0), instances.get(1)));
        config5.put(Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE)),
                    Arrays.asList(instances.get(1), instances.get(2)));
        replicasForRangesList.add(config5);

        qt().forAll(TestUtils.partitioners(), integers().between(0, replicasForRangesList.size() - 1))
            .checkAssert((partitioner, index) -> {
                ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                             ImmutableMap.of("DC1", 2, "DC2", 1));

                int expectedRangeCount = replicasForRangesList.get(index).asMapOfRanges().size();

                testCassandraRingSerializationWithReplicasForRanges(bridge, partitioner, instances, replicasForRangesList.get(index), rf, expectedRangeCount);
            });
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
                assertThat(deserialized.ring()).isEqualTo(tokenPartitioner.ring());
                assertThat(deserialized.numPartitions()).isEqualTo(tokenPartitioner.numPartitions());
                assertThat(deserialized.subRanges()).isEqualTo(tokenPartitioner.subRanges());
                assertThat(deserialized.partitionMap()).isEqualTo(tokenPartitioner.partitionMap());
                assertThat(deserialized.reversePartitionMap()).isEqualTo(tokenPartitioner.reversePartitionMap());
                for (int partition = 0; partition < tokenPartitioner.numPartitions(); partition++)
                {
                    assertThat(deserialized.getTokenRange(partition)).isEqualTo(tokenPartitioner.getTokenRange(partition));
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
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.ring()).isNotNull();
        assertThat(deserialized.partitioner()).isNotNull();
        assertThat(deserialized.tokenPartitioner()).isNotNull();
        assertThat(deserialized.partitioner()).isEqualTo(Partitioner.Murmur3Partitioner);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCqlFieldSet(CassandraBridge bridge)
    {
        CqlField.CqlSet setType = bridge.set(bridge.text());
        CqlField field = new CqlField(true, false, false, RandomUtils.randomAlphanumeric(5, 20), setType, 10);
        byte[] bytes = bridge.javaSerialize(field);
        CqlField deserialized = bridge.javaDeserialize(bytes, CqlField.class);
        assertThat(deserialized).isEqualTo(field);
        assertThat(deserialized.name()).isEqualTo(field.name());
        assertThat(deserialized.type()).isEqualTo(field.type());
        assertThat(deserialized.position()).isEqualTo(field.position());
        assertThat(deserialized.isPartitionKey()).isEqualTo(field.isPartitionKey());
        assertThat(deserialized.isClusteringColumn()).isEqualTo(field.isClusteringColumn());
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
        assertThat(udt2).isNotEqualTo(udt1);
        byte[] bytes = bridge.javaSerialize(udt1);
        CqlField.CqlUdt deserialized = bridge.javaDeserialize(bytes, CqlField.CqlUdt.class);
        assertThat(deserialized).isEqualTo(udt1);
        assertThat(deserialized).isNotEqualTo(udt2);
        for (int field = 0; field < deserialized.fields().size(); field++)
        {
            assertThat(deserialized.field(field)).isEqualTo(udt1.field(field));
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
            return bridge.getVersion();
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

        assertThat(deserialized.keyStorePath()).isEqualTo(config.keyStorePath());
        assertThat(deserialized.base64EncodedKeyStore()).isEqualTo(config.base64EncodedKeyStore());
        assertThat(deserialized.keyStorePassword()).isEqualTo(config.keyStorePassword());
        assertThat(deserialized.keyStoreType()).isEqualTo(config.keyStoreType());
        assertThat(deserialized.trustStorePath()).isEqualTo(config.trustStorePath());
        assertThat(deserialized.base64EncodedTrustStore()).isEqualTo(config.base64EncodedTrustStore());
        assertThat(deserialized.trustStorePassword()).isEqualTo(config.trustStorePassword());
        assertThat(deserialized.trustStoreType()).isEqualTo(config.trustStoreType());
    }
}
