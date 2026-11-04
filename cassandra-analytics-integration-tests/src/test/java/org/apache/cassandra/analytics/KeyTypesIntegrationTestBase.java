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

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * CASSANALYTICS-35: bulk writes tables whose partition and clustering keys use each type that Cassandra allows in a
 * primary key, after reading them with the bulk reader.
 * <p>
 * The cluster has three nodes and the keyspace a replication factor of one, so a row that the writer sends to a node
 * that does not own its token is missing from a scan of the table and cannot be looked up by its key. Cassandra also
 * refuses to import an SSTable with a key outside the ranges of the node. Every row is looked up with the CQL
 * literals it was inserted with.
 * <p>
 * The types are split between subclasses, which run in separate JVMs, to bound the number of bulk write jobs
 * that a single JVM runs.
 */
abstract class KeyTypesIntegrationTestBase extends SharedClusterSparkIntegrationTestBase
{
    protected static final int ROW_COUNT = 6;
    private static final String CREATE_TABLE =
    "CREATE TABLE IF NOT EXISTS %%s (pk %1$s, ck %2$s, value int, PRIMARY KEY (pk, ck)) WITH %3$sread_repair='NONE';";

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    /**
     * @return the types to write, each with the CQL literals of its values
     */
    abstract List<KeyType> keyTypes();

    /**
     * @return the user-defined types that the key types use, each as its name and fields, in the order to create them
     */
    protected List<String> userDefinedTypes()
    {
        return Arrays.asList("simple_udt (a int, b text)",
                             "nested_udt (id int, tags frozen<list<text>>, inner frozen<simple_udt>)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keyTypes")
    void partitionAndClusteringKeyOfType(KeyType keyType)
    {
        QualifiedName source = keyType.table("source");
        QualifiedName target = keyType.table("target");

        bulkWriteFromBulkRead(source, target, keyType.rowCount);

        // Two partitions, each with several rows
        for (int i = 0; i < keyType.rowCount; i++)
        {
            assertValue(String.format("SELECT value FROM %s WHERE pk = %s AND ck = %s",
                                      target, keyType.literal(i % 2), keyType.clusteringLiteral(i)),
                        i);
        }
    }

    protected void bulkWriteFromBulkRead(QualifiedName source, QualifiedName target, int rowCount)
    {
        Dataset<Row> data = bulkReaderDataFrame(source).load();
        bulkWriterDataFrameWriter(data, target).save();

        List<String> sourceRows = selectJson(source);
        assertThat(sourceRows).hasSize(rowCount);
        assertThat(selectJson(target)).containsExactlyInAnyOrderElementsOf(sourceRows);
    }

    protected void assertValue(String query, int expectedValue)
    {
        Object[][] rows = cluster.getFirstRunningInstance().coordinator().execute(query, ConsistencyLevel.ALL);
        assertThat(rows).as(query).hasDimensions(1, 1);
        assertThat(rows[0][0]).as(query).isEqualTo(expectedValue);
    }

    private List<String> selectJson(QualifiedName table)
    {
        return Arrays.stream(cluster.getFirstRunningInstance()
                                    .coordinator()
                                    .execute(String.format("SELECT JSON * FROM %s", table), ConsistencyLevel.ALL))
                     .map(row -> (String) row[0])
                     .collect(Collectors.toList());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        for (String userDefinedType : userDefinedTypes())
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format("CREATE TYPE IF NOT EXISTS %s.%s;",
                                                                       TEST_KEYSPACE, userDefinedType));
        }
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        for (KeyType keyType : keyTypes())
        {
            String createTable = String.format(CREATE_TABLE, keyType.cqlType, keyType.clusteringType(), keyType.tableOptions());
            createTestTable(keyType.table("source"), createTable);
            createTestTable(keyType.table("target"), createTable);
            for (int i = 0; i < keyType.rowCount; i++)
            {
                coordinator.execute(String.format("INSERT INTO %s (pk, ck, value) VALUES (%s, %s, %d)",
                                                  keyType.table("source"), keyType.literal(i % 2), keyType.clusteringLiteral(i), i),
                                    ConsistencyLevel.ALL);
            }
        }
    }

    protected CassandraVersion cassandraVersion()
    {
        return CassandraVersion.fromVersion(testVersion.version())
                               .orElseThrow(() -> new IllegalStateException("Unsupported Cassandra version: " + testVersion));
    }

    /**
     * A CQL type with the CQL literal of its i-th value. The values of a type are distinct. The type is used for
     * the partition key and, unless the type is only tested as a partition key, for the clustering key.
     */
    static final class KeyType
    {
        final String name;
        final String cqlType;
        final int rowCount;
        private final boolean isClusteringKey;
        private final boolean isDescending;
        private final IntFunction<String> literals;

        KeyType(String name, String cqlType, IntFunction<String> literals)
        {
            this(name, cqlType, ROW_COUNT, true, false, literals);
        }

        KeyType(String name, String cqlType, int rowCount, IntFunction<String> literals)
        {
            this(name, cqlType, rowCount, true, false, literals);
        }

        private KeyType(String name,
                        String cqlType,
                        int rowCount,
                        boolean isClusteringKey,
                        boolean isDescending,
                        IntFunction<String> literals)
        {
            this.name = name;
            this.cqlType = cqlType;
            this.rowCount = rowCount;
            this.isClusteringKey = isClusteringKey;
            this.isDescending = isDescending;
            this.literals = literals;
        }

        /**
         * @return a type used only for the partition key, with an {@code int} clustering key
         */
        static KeyType partitionKeyOnly(String name, String cqlType, IntFunction<String> literals)
        {
            return new KeyType(name, cqlType, ROW_COUNT, false, false, literals);
        }

        /**
         * @return a type used for the partition key and for a clustering key in descending order
         */
        static KeyType descending(String name, String cqlType, IntFunction<String> literals)
        {
            return new KeyType(name, cqlType, ROW_COUNT, true, true, literals);
        }

        String literal(int index)
        {
            return literals.apply(index);
        }

        String clusteringType()
        {
            return isClusteringKey ? cqlType : "int";
        }

        String clusteringLiteral(int index)
        {
            return isClusteringKey ? literal(index) : String.valueOf(index);
        }

        String tableOptions()
        {
            return isDescending ? "CLUSTERING ORDER BY (ck DESC) AND " : "";
        }

        QualifiedName table(String suffix)
        {
            return new QualifiedName(TEST_KEYSPACE, name + "_" + suffix);
        }

        @Override
        public String toString()
        {
            return cqlType + (isDescending ? " descending" : "");
        }
    }
}
