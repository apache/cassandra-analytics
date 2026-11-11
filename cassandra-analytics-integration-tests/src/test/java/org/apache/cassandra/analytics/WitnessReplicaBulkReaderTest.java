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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Bulk reads against keyspaces that use mutation tracking (CEP-45/CEP-46) and witness replicas.
 *
 * <p>A witness-enabled keyspace is declared with the {@code <replicas>/<transient>} replication factor form plus
 * {@code replication_type = 'tracked'}:</p>
 *
 * <pre>
 * CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3/1'}
 *                     AND replication_type = 'tracked';
 * </pre>
 *
 * <p>{@code '3/1'} means three replicas of which one is a witness, which does not hold complete data. The reader
 * excludes witnesses from replica selection, which needs the Sidecar to report which replica of each range is a
 * witness (CASSSIDECAR-444). Until it does, a read from a witness keyspace must fail with a clear error rather than
 * silently return incomplete results, which is what these tests check. Reads returning every row at each consistency
 * level follow once the Sidecar reports witnesses.</p>
 *
 * <p>Requires Cassandra 6.0+ built from a cep-45-mutation-tracking commit; see CASSANALYTICS-200 and
 * {@code scripts/install-cassandra-all-local.sh}. Skips cleanly on older versions.</p>
 *
 * @see <a href="https://issues.apache.org/jira/browse/CASSANALYTICS-164">CASSANALYTICS-164</a>
 */
class WitnessReplicaBulkReaderTest extends SharedClusterSparkIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WitnessReplicaBulkReaderTest.class);

    private static final int ROW_COUNT = 200;

    /** 3 replicas, 1 of them a witness -> 2 full replicas. */
    private static final String WITNESS_KEYSPACE = "witness_ks";
    /** 3 full replicas, mutation tracking on, no witnesses - isolates 'tracked' from '3/1'. */
    private static final String TRACKED_KEYSPACE = "tracked_ks";

    private final QualifiedName witnessTable = uniqueTestTableFullName(WITNESS_KEYSPACE);
    private final QualifiedName trackedTable = uniqueTestTableFullName(TRACKED_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    // '3/1' needs three replicas to be meaningful.
                    .nodesPerDc(3)
                    // Cassandra's ReplicationFactor.validate() requires this before it will accept a
                    // transient replica count. Witness replicas reuse the transient replication machinery.
                    .additionalInstanceConfig(Map.of("transient_replication_enabled", true));
        // Deliberately NOT setting tokenCount: it defaults to 1, and Cassandra rejects transient
        // replicas on vnode clusters ("Transient nodes are not allowed with multiple tokens").
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        // Mutation tracking only exists on 6.0 built from cep-45. An aborted @BeforeAll produces no test
        // event, so log the reason as well as skipping - otherwise the run looks silently empty.
        String version = testVersion.version();
        boolean supported = CassandraVersion.fromVersion(version)
                                            .map(v -> v.versionNumber() >= CassandraVersion.SIXZERO.versionNumber())
                                            .orElse(false);
        if (!supported)
        {
            LOGGER.warn("Skipping {}: witness replicas require mutation tracking (CEP-45), which needs "
                        + "Cassandra 6.0+ from a cep-45 build. Test version is {}", getClass().getSimpleName(), version);
        }
        assumeTrue(supported, "Witness replicas require Cassandra 6.0+ with mutation tracking; version is " + version);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // createTestKeyspace() takes a Map<String, Integer>, which cannot express either '3/1' or
        // replication_type, so issue the CQL directly.
        createKeyspace(WITNESS_KEYSPACE, "'datacenter1': '3/1'", "tracked");
        createKeyspace(TRACKED_KEYSPACE, "'datacenter1': '3'", "tracked");

        createTestTable(witnessTable, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");
        createTestTable(trackedTable, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");

        populate(witnessTable);
        populate(trackedTable);
    }

    /**
     * A read from a witness keyspace must fail at job setup, at every consistency level, while the Sidecar does not
     * report which replicas are witnesses. Before CASSANALYTICS-164 it succeeded and silently returned a subset of the
     * rows.
     */
    @ParameterizedTest
    @ValueSource(strings = {"ONE", "QUORUM", "LOCAL_QUORUM", "ALL"})
    void bulkReadFromWitnessKeyspaceFailsWhenSidecarDoesNotReportWitnesses(String consistencyLevel)
    {
        assertThatThrownBy(() -> bulkReaderDataFrame(witnessTable).option("consistencyLevel", consistencyLevel)
                                                                  .load()
                                                                  .collectAsList())
        .hasStackTraceContaining("did not report which replicas")
        .hasStackTraceContaining("witnessesByDatacenter");
    }

    /**
     * Mutation tracking without witnesses. Separates "the reader copes with replication_type = 'tracked'"
     * from "the reader copes with witnesses", so a failure points at one or the other.
     */
    @Test
    void bulkReadFromTrackedKeyspaceWithoutWitnessesReturnsEveryRow()
    {
        Dataset<Row> df = bulkReaderDataFrame(trackedTable).load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(ROW_COUNT);
        assertRowsAreExactlyTheDataset(rows);
    }

    private void createKeyspace(String keyspace, String replicationEntry, String replicationType)
    {
        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                      + "{'class': 'NetworkTopologyStrategy', %s} AND replication_type = '%s';",
                      keyspace, replicationEntry, replicationType));
    }

    private void populate(QualifiedName table)
    {
        IInstance instance = cluster.getFirstRunningInstance();
        for (int i = 0; i < ROW_COUNT; i++)
        {
            // ALL, so every full replica has the row before the snapshot is taken
            instance.coordinator().execute(String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');",
                                                         table, i, "value_" + i),
                                           ConsistencyLevel.ALL);
        }
    }

    private void assertRowsAreExactlyTheDataset(List<Row> rows)
    {
        List<Integer> ids = rows.stream().map(row -> row.getInt(0)).sorted().collect(Collectors.toList());
        assertThat(ids).containsExactlyElementsOf(IntStream.range(0, ROW_COUNT).boxed().collect(Collectors.toList()));
        rows.forEach(row -> assertThat(row.getString(1)).isEqualTo("value_" + row.getInt(0)));
    }
}
