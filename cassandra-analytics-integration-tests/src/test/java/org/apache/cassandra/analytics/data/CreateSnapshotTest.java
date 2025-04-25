package org.apache.cassandra.analytics.data;

import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.cassandra.spark.data.ClientConfig.*;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

public class CreateSnapshotTest extends SharedClusterSparkIntegrationTestBase {
    static final QualifiedName TABLE_NAME_FOR_CREATE_SNAPSHOT_TEST
            = new QualifiedName(TEST_KEYSPACE, "test_create_snapshot");

    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e");

//    @Test
//    void testDefaultCreateSnapshotWithVNodes() {
//        DataFrameReader readDf = bulkReaderDataFrame(TABLE_NAME_FOR_CREATE_SNAPSHOT_TEST)
//                .option(SNAPSHOT_NAME_KEY, "CreateSnapshotTestWithVNodes")
//                .option(CREATE_SNAPSHOT_FILTER_DISTINCT_INSTANCES_KEY, "false")
//                .option(CONSISTENCY_LEVEL_KEY, "ALL")
//                .option(CLEAR_SNAPSHOT_STRATEGY_KEY, "TTL 60s");
//
//        Throwable thrown = catchThrowable(readDf::load);
//
//        assertThat(thrown).isInstanceOf(RuntimeException.class);
//    }

    @Test
    void testCreateSnapshotFilterDistinctInstancesWithVNodes() {
        DataFrameReader readDf = bulkReaderDataFrame(TABLE_NAME_FOR_CREATE_SNAPSHOT_TEST)
                .option(SNAPSHOT_NAME_KEY, "CreateSnapshotTestWithVNodes")
                .option(CREATE_SNAPSHOT_FILTER_DISTINCT_INSTANCES_KEY, "true")
                .option(CONSISTENCY_LEVEL_KEY, "ALL")
                .option(CLEAR_SNAPSHOT_STRATEGY_KEY, "TTL 60s");

        List<Row> rows = readDf.load().collectAsList();
        assertThat(rows.size()).isEqualTo(5);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().tokenCount(4).nodesPerDc(6);
    }

    @Override
    protected void initializeSchemaForTest() {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s (c1 int, c2 text, PRIMARY KEY(c1));";
        createTestTable(TABLE_NAME_FOR_CREATE_SNAPSHOT_TEST, createTableStatement);
        populateTable(TABLE_NAME_FOR_CREATE_SNAPSHOT_TEST);
    }

    void populateTable(QualifiedName tableName)
    {
        for (int i = 0; i < DATASET.size(); i++)
        {
            String value = DATASET.get(i);
            String query = String.format("INSERT INTO %s (c1, c2) VALUES (%d, '%s');", tableName, i, value);
            cluster.get(1)
                    .coordinator()
                    .execute(query, ConsistencyLevel.ALL);
        }
    }
}
