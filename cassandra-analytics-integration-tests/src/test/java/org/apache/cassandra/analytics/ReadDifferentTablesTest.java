package org.apache.cassandra.analytics;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.JvmDTestSharedClassesPredicate;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that reads different tables with different schemas within the same test
 */
class ReadDifferentTablesTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);
    QualifiedName table2 = uniqueTestTableFullName(TEST_KEYSPACE);

    @Test
    void testReadingFromTwoDifferentTables()
    {
        Dataset<Row> dataForTable1 = bulkReaderDataFrame(table1).load();
        Dataset<Row> dataForTable2 = bulkReaderDataFrame(table2).load();

        assertThat(dataForTable1.count()).isEqualTo(DATASET.size());
        assertThat(dataForTable2.count()).isEqualTo(DATASET.size());

        List<Row> rowList1 = dataForTable1.collectAsList().stream()
                                          .sorted(Comparator.comparing(row -> row.getInt(0)))
                                          .collect(Collectors.toList());

        List<Row> rowList2 = dataForTable2.collectAsList().stream()
                                          .sorted(Comparator.comparing(row -> row.getLong(1)))
                                          .collect(Collectors.toList());

        for (int i = 0; i < DATASET.size(); i++)
        {
            assertThat(rowList1.get(i).getInt(0)).isEqualTo(i);
            assertThat(rowList1.get(i).getString(1)).isEqualTo(DATASET.get(i));
            assertThat(rowList2.get(i).getString(0)).isEqualTo(DATASET.get(i));
            assertThat(rowList2.get(i).getLong(1)).isEqualTo(i);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");
        createTestTable(table2, "CREATE TABLE IF NOT EXISTS %s (name text PRIMARY KEY, value bigint);");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < DATASET.size(); i++)
        {
            String value = DATASET.get(i);
            String query1 = String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');", table1, i, value);
            String query2 = String.format("INSERT INTO %s (name, value) VALUES ('%s', %d);", table2, value, i);

            firstRunningInstance.coordinator().execute(query1, ConsistencyLevel.ALL);
            firstRunningInstance.coordinator().execute(query2, ConsistencyLevel.ALL);
        }
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(), Semver.SemverType.LOOSE));

        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(1)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDCs(1)
                          .withDataDirCount(1)
                          .withSharedClasses(JvmDTestSharedClassesPredicate.INSTANCE)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(1, clusterBuilder.getTokenCount());
        clusterBuilder.withTokenSupplier(tokenSupplier);
        return clusterBuilder.start();
    }
}
