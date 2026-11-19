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

package org.apache.cassandra.spark.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.backup.BackupReaderRegistry;
import org.apache.cassandra.spark.data.backup.FakeBackupReader;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3DataSourceClientConfigTest
{
    private static final String TEST_CLUSTER_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    private static final String TEST_BACKUP_READER_TYPE = "fake";
    public static final Map<String, String> REQUIRED_CONFIG_OPTIONS = ImmutableMap.<String, String>builder()
                                                                                  .put("clusterName", TEST_CLUSTER_UUID)
                                                                                  .put("keyspace", "test_keyspace")
                                                                                  .put("table", "test_table")
                                                                                  .put("tableCreateStmt",
                                                                                       "CREATE TABLE test_keyspace.test_table (id uuid PRIMARY KEY, name text)")
                                                                                  .put("s3-region", "us-west-2")
                                                                                  .put("s3-bucket", "test-bucket")
                                                                                  .put("backupReaderType", TEST_BACKUP_READER_TYPE)
                                                                                  .build();

    @BeforeAll
    static void registerBackupReader()
    {
        BackupReaderRegistry.register(TEST_BACKUP_READER_TYPE, config -> new FakeBackupReader(config.s3Config(), config.s3Config().s3Bucket()));
    }

    @Test
    void testValidConfigurationWithRequiredOptionsOnly()
    {
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(new CaseInsensitiveStringMap(REQUIRED_CONFIG_OPTIONS));

        // Test Cassandra-related fields
        assertThat(config.clusterName()).isEqualTo(TEST_CLUSTER_UUID);
        assertThat(config.keyspace()).isEqualTo("test_keyspace");
        assertThat(config.table()).isEqualTo("test_table");
        assertThat(config.tableCreateStmt()).isEqualTo("CREATE TABLE test_keyspace.test_table (id uuid PRIMARY KEY, name text)");

        // Test S3-related fields
        assertThat(config.s3Region()).isEqualTo("us-west-2");
        assertThat(config.s3Bucket()).isEqualTo("test-bucket");
        assertThat(config.s3EndpointOverride()).isNull();
        assertThat(config.s3AccessKeyId()).isNull();
        assertThat(config.s3SecretAccessKey()).isNull();
        assertThat(config.s3Config().s3HttpMaxConcurrency()).isEqualTo(0);

        // Test defaults
        assertThat(config.datacenter()).isNull();
        assertThat(config.defaultParallelism()).isEqualTo(1);
        assertThat(config.numCores()).isEqualTo(1);
        assertThat(config.consistencyLevel()).isNull();
        assertThat(config.enableStats()).isTrue();
        assertThat(config.readIndexOffset()).isTrue();
        assertThat(config.sizing()).isEqualTo("default");
        assertThat(config.maxPartitionSize()).isEqualTo(1);
        assertThat(config.numberSplits()).isEqualTo(-1);
        assertThat(config.sstableTokenIndexEnabled()).isFalse();
        assertThat(config.sstableTokenIndexPrebuildPartitions()).isEqualTo(0);
        assertThat(config.sstableTokenIndexPrebuildPerTaskConcurrency()).isEqualTo(4);
        assertThat(config.resolveSSTableTokenIndexPrebuildPartitions(1_400_000, 16_000)).isEqualTo(140);
        assertThat(config.lastModifiedTimestampField()).isNull();
        assertThat(config.udts()).isEmpty();
        assertThat(config.cassandraVersion()).isEqualTo(CassandraVersion.FOURZERO);
    }

    @Test
    void testValidConfigurationWithAllOptions()
    {
        Map<String, String> allOptions = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        allOptions.put("dc", "dc1");
        allOptions.put("defaultParallelism", "4");
        allOptions.put("numCores", "8");
        allOptions.put("consistencyLevel", "LOCAL_QUORUM");
        allOptions.put("enableStats", "false");
        allOptions.put("readIndexOffset", "false");
        allOptions.put("sizing", "dynamic");
        allOptions.put("maxPartitionSize", "5");
        allOptions.put("number_splits", "10");
        allOptions.put("sstableTokenIndexEnabled", "true");
        allOptions.put("sstableTokenIndexPrebuildPartitions", "80");
        allOptions.put("sstableTokenIndexPrebuildPerTaskConcurrency", "0");
        allOptions.put("lastModifiedColumnName", "last_updated");
        allOptions.put("udts", "type1\ntype2\ntype3");
        allOptions.put("cassandraVersion", "THREEZERO");
        allOptions.put("s3-endpoint-override", "http://localhost:9000");
        allOptions.put("s3-access-key-id", "test-access-key");
        allOptions.put("s3-secret-access-key", "test-secret-key");
        allOptions.put("s3-http-max-concurrency", "333");
        allOptions.put("replicationStrategy", "SimpleStrategy");
        allOptions.put("replicationFactor", "2");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(allOptions);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        // Test Cassandra-related fields
        assertThat(config.datacenter()).isEqualTo("dc1");
        assertThat(config.defaultParallelism()).isEqualTo(4);
        assertThat(config.numCores()).isEqualTo(8);
        assertThat(config.consistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
        assertThat(config.enableStats()).isFalse();
        assertThat(config.readIndexOffset()).isFalse();
        assertThat(config.sizing()).isEqualTo("dynamic");
        assertThat(config.maxPartitionSize()).isEqualTo(5);
        assertThat(config.numberSplits()).isEqualTo(10);
        assertThat(config.sstableTokenIndexEnabled()).isTrue();
        assertThat(config.sstableTokenIndexPrebuildPartitions()).isEqualTo(80);
        assertThat(config.sstableTokenIndexPrebuildPerTaskConcurrency()).isEqualTo(1);
        assertThat(config.resolveSSTableTokenIndexPrebuildPartitions(1_400_000, 16_000)).isEqualTo(80);
        assertThat(config.lastModifiedTimestampField()).isEqualTo("last_updated");
        assertThat(config.udts()).isEqualTo("type1\ntype2\ntype3");
        assertThat(config.cassandraVersion()).isEqualTo(CassandraVersion.THREEZERO);

        // Test S3-related fields
        assertThat(config.s3EndpointOverride()).isEqualTo("http://localhost:9000");
        assertThat(config.s3AccessKeyId()).isEqualTo("test-access-key");
        assertThat(config.s3SecretAccessKey()).isEqualTo("test-secret-key");
        assertThat(config.s3Config().s3HttpMaxConcurrency()).isEqualTo(333);
    }

    @Test
    void testRejectsNegativeS3HttpMaxConcurrency()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("s3-http-max-concurrency", "-1");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        assertThatThrownBy(() -> S3DataSourceClientConfig.create(caseInsensitiveOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("s3-http-max-concurrency");
    }

    @Test
    void testParsedUdts()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("udts", "type1\ntype2\n\ntype3\n");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        Set<String> parsedUdts = config.parsedUdts();

        assertThat(parsedUdts).containsExactlyInAnyOrder("type1", "type2", "type3");
    }

    @Test
    void testParsedUdtsEmpty()
    {
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(new CaseInsensitiveStringMap(REQUIRED_CONFIG_OPTIONS));
        Set<String> parsedUdts = config.parsedUdts();
        assertThat(parsedUdts).isEmpty();
    }

    @Test
    void testRequestedFeaturesWithLastModifiedColumn()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("lastModifiedColumnName", "updated_at");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.requestedFeatures()).isNotNull();
        assertThat(config.requestedFeatures()).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "clusterName", "keyspace", "table", "tableCreateStmt", "s3-region", "s3-bucket" })
    void testMissingRequiredFields(String requiredField)
    {
        Map<String, String> incompleteOptions = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        incompleteOptions.remove(requiredField);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(incompleteOptions);

        assertThatThrownBy(() -> S3DataSourceClientConfig.create(caseInsensitiveOptions))
        .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testClusterIdOnlyWithoutClusterName()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.remove("clusterName");
        options.put("clusterId", "61dbe241-d5c7-4fa0-b127-9aa2c0b99e4c");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.clusterName()).isEqualTo("61dbe241-d5c7-4fa0-b127-9aa2c0b99e4c");
    }

    @Test
    void testClusterIdPrioritizedOverClusterName()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("clusterId", "explicit-cluster-uuid");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.clusterName()).isEqualTo("explicit-cluster-uuid");
    }

    @ParameterizedTest
    @CsvSource({
    "defaultParallelism, 0, 0",
    "defaultParallelism, 10, 10",
    "numCores, 0, 0",
    "numCores, 16, 16",
    "maxPartitionSize, 0, 0",
    "maxPartitionSize, 10, 10"
    })
    void testIntegerOptions(String optionKey, String optionValue, int expectedValue)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put(optionKey, optionValue);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        switch (optionKey)
        {
            case "defaultParallelism":
                assertThat(config.defaultParallelism()).isEqualTo(expectedValue);
                break;
            case "numCores":
                assertThat(config.numCores()).isEqualTo(expectedValue);
                break;
            case "maxPartitionSize":
                assertThat(config.maxPartitionSize()).isEqualTo(expectedValue);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + optionKey);
        }
    }

    @ParameterizedTest
    @CsvSource({
    "-1, -1",  // default value
    "1, 1",    // small positive value
    "100, 100" // large positive value
    })
    void testNumberSplitsConfiguration(String inputValue, int expectedValue)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("number_splits", inputValue);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.numberSplits()).isEqualTo(expectedValue);
    }

    @Test
    void testNumberSplitsDefaultValue()
    {
        // Test that when number_splits is not specified, it defaults to -1
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(new CaseInsensitiveStringMap(REQUIRED_CONFIG_OPTIONS));
        assertThat(config.numberSplits()).isEqualTo(-1);
    }

    @ParameterizedTest
    @CsvSource({
    "enableStats, true, true",
    "enableStats, false, false",
    "readIndexOffset, true, true",
    "readIndexOffset, false, false"
    })
    void testBooleanOptions(String optionKey, String optionValue, boolean expectedValue)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put(optionKey, optionValue);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        switch (optionKey)
        {
            case "enableStats":
                assertThat(config.enableStats()).isEqualTo(expectedValue);
                break;
            case "readIndexOffset":
                assertThat(config.readIndexOffset()).isEqualTo(expectedValue);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + optionKey);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "LOCAL_ONE", "LOCAL_QUORUM", "QUORUM", "ALL" })
    void testValidConsistencyLevels(String consistencyLevel)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("consistencyLevel", consistencyLevel);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.consistencyLevel()).isEqualTo(ConsistencyLevel.valueOf(consistencyLevel));
    }

    @ParameterizedTest
    @ValueSource(strings = { "THREEZERO", "FOURZERO" })
    void testValidCassandraVersions(String version)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("cassandraVersion", version);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.cassandraVersion()).isEqualTo(CassandraVersion.valueOf(version));
    }

    @ParameterizedTest
    @ValueSource(strings = { "default", "dynamic" })
    void testValidSizingOptions(String sizing)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("sizing", sizing);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.sizing()).isEqualTo(sizing);
    }

    @Test
    void testS3EndpointOverrideOptional()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("s3-endpoint-override", "http://custom-endpoint.com");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.s3EndpointOverride()).isEqualTo("http://custom-endpoint.com");
    }

    @Test
    void testS3CredentialsOptional()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("s3-access-key-id", "access-key");
        options.put("s3-secret-access-key", "secret-key");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.s3AccessKeyId()).isEqualTo("access-key");
        assertThat(config.s3SecretAccessKey()).isEqualTo("secret-key");
    }

    @Test
    void testCaseInsensitiveOptions()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("DC", "dc1");           // Should work case-insensitive
        options.put("UDTS", "type1\ntype2"); // Should work case-insensitive
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.datacenter()).isEqualTo("dc1");
        assertThat(config.udts()).isEqualTo("type1\ntype2");
    }

    @Test
    void testLastModifiedTimestampFieldConfiguresFeatures()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("lastModifiedColumnName", "updated_at");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);
        assertThat(config.lastModifiedTimestampField()).isEqualTo("updated_at");
        assertThat(config.requestedFeatures()).isNotEmpty();

        // Verify that the last modified timestamp feature is configured with the correct alias
        boolean hasLastModifiedFeature = config.requestedFeatures().stream()
                                               .anyMatch(feature -> "updated_at".equals(feature.fieldName()));
        assertThat(hasLastModifiedFeature).isTrue();
    }

    @Test
    void testDefaultReplicationFactorSettings()
    {
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(new CaseInsensitiveStringMap(REQUIRED_CONFIG_OPTIONS));

        // Test that default parsing works
        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(rf.getOptions()).containsEntry("usw2", 3);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(3);
    }

    @Test
    void testCustomReplicationFactorNetworkTopologyStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "NetworkTopologyStrategy");
        options.put("replicationFactor", "usw2:3,euw1:2");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(rf.getOptions()).containsEntry("usw2", 3);
        assertThat(rf.getOptions()).containsEntry("euw1", 2);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(5);
    }

    @Test
    void testCustomReplicationFactorSimpleStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "SimpleStrategy");
        options.put("replicationFactor", "3");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.SimpleStrategy);
        assertThat(rf.getOptions()).containsEntry("replication_factor", 3);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(3);
    }

    @Test
    void testReplicationFactorLocalStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "LocalStrategy");
        options.put("replicationFactor", "");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.LocalStrategy);
        assertThat(rf.getOptions()).isEmpty();
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(0);
    }

    @Test
    void testReplicationFactorSingleDatacenter()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "NetworkTopologyStrategy");
        options.put("replicationFactor", "dc1:5");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(rf.getOptions()).containsEntry("dc1", 5);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(5);
    }

    @ParameterizedTest
    @ValueSource(strings = { "invalid", "dc1", "dc1:abc", "dc1:3:extra", ":3", "dc1:", "" })
    void testInvalidReplicationFactorFormatNetworkTopology(String invalidReplicationFactor)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "NetworkTopologyStrategy");
        options.put("replicationFactor", invalidReplicationFactor);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        assertThatThrownBy(config::getParsedReplicationFactor)
        .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "abc", "3.5", "", "dc1:3" })
    void testInvalidReplicationFactorFormatSimpleStrategy(String invalidReplicationFactor)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "SimpleStrategy");
        options.put("replicationFactor", invalidReplicationFactor);
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        assertThatThrownBy(config::getParsedReplicationFactor)
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReplicationFactorWithWhitespace()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("replicationStrategy", "NetworkTopologyStrategy");
        options.put("replicationFactor", " usw2 : 3 , euw1 : 2 ");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(rf.getOptions()).containsEntry("usw2", 3);
        assertThat(rf.getOptions()).containsEntry("euw1", 2);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(5);
    }

    @Test
    void testReplicationFactorCaseInsensitiveStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CONFIG_OPTIONS);
        options.put("REPLICATIONSTRATEGY", "SimpleStrategy");
        options.put("REPLICATIONFACTOR", "2");
        Map<String, String> caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(caseInsensitiveOptions);

        ReplicationFactor rf = config.getParsedReplicationFactor();
        assertThat(rf.getReplicationStrategy()).isEqualTo(ReplicationFactor.ReplicationStrategy.SimpleStrategy);
        assertThat(rf.getOptions()).containsEntry("replication_factor", 2);
    }
}
