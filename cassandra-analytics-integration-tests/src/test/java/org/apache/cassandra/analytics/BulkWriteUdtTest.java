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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.JvmDTestSharedClassesPredicate;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import shaded.com.datastax.driver.core.ResultSet;

import static org.apache.cassandra.testing.CassandraTestTemplate.fixDistributedSchemas;
import static org.apache.cassandra.testing.CassandraTestTemplate.waitForHealthyRing;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteUdtTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName UDT_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_udt");
    static final QualifiedName NESTED_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_nested_udt");
    public static final String TWO_FIELD_UDT_NAME = "two_field_udt";
    public static final String NESTED_FIELD_UDT_NAME = "nested_udt";
    public static final String UDT_TABLE_CREATE = "CREATE TABLE " + UDT_TABLE_NAME + " (\n"
                                                  + "          id BIGINT PRIMARY KEY,\n"
                                                  + "          udtfield " + TWO_FIELD_UDT_NAME + ");";
    public static final String TWO_FIELD_UDT_DEF = "CREATE TYPE " + UDT_TABLE_NAME.keyspace() + "."
                                                   + TWO_FIELD_UDT_NAME + " (\n"
                                                   + "            f1 text,\n"
                                                   + "            f2 int);";
    public static final String NESTED_UDT_DEF = "CREATE TYPE " + NESTED_TABLE_NAME.keyspace() + "."
                                                + NESTED_FIELD_UDT_NAME + " (\n"
                                                + "            n1 BIGINT,\n"
                                                + "            n2 frozen<" + TWO_FIELD_UDT_NAME + ">"
                                                + ");";
    public static final String NESTED_TABLE_CREATE = "CREATE TABLE " + NESTED_TABLE_NAME + "(\n"
                                                     + "           id BIGINT PRIMARY KEY,\n"
                                                     + "           nested " + NESTED_FIELD_UDT_NAME + ");";
    @Test
    void testWriteWithUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateUdtData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(df, UDT_TABLE_NAME).save();

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + UDT_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWrites(df.collectAsList(), queryAllDataWithDriver(cluster, UDT_TABLE_NAME, shaded.com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM));
    }

    @Test
    void testWriteWithNestedUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateNestedUdtData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(df, NESTED_TABLE_NAME).save();

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + NESTED_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWrites(df.collectAsList(), queryAllDataWithDriver(cluster, NESTED_TABLE_NAME, shaded.com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM));
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(), Semver.SemverType.LOOSE));

        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(3)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDCs(1)
                          .withDataDirCount(1)
                          .withSharedClasses(JvmDTestSharedClassesPredicate.INSTANCE)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(3, clusterBuilder.getTokenCount());
        clusterBuilder.withTokenSupplier(tokenSupplier);
        UpgradeableCluster cluster = clusterBuilder.createWithoutStarting();
        cluster.startup();

        waitForHealthyRing(cluster);
        fixDistributedSchemas(cluster);
        return cluster;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(UDT_TABLE_NAME, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances(TWO_FIELD_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_TABLE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_TABLE_CREATE);
    }


    public static void validateWrites(List<Row> sourceData, ResultSet queriedData)
    {
        Set<String> actualEntries = new HashSet<>();
        queriedData.forEach(row ->
            actualEntries.add(getFormattedData(row)));

        // Number of entries in Cassandra must match the original datasource
        assertThat(actualEntries.size()).isEqualTo(sourceData.size());

        // remove from actual entries to make sure that the data read is the same as the data written
        Set<String> sourceEntries = sourceData.stream().map(BulkWriteUdtTest::getFormatedSourceEntry)
                           .collect(Collectors.toSet());
        assertThat(actualEntries).as("All entries are expected to be read from database")
                                 .containsExactlyInAnyOrderElementsOf(sourceEntries);
    }

    private static String getFormatedSourceEntry(Row row)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++)
        {
            maybeFormatSparkRow(sb, row.get(i));
            if (i != (row.size() - 1))
            {
                sb.append(":");
            }
        }
        return sb.toString();
    }

    // Format a Spark row to look like what the toString on a UDT looks like
    // Unfortunately not _quite_ json, so we need to do this manually.
    private static void maybeFormatSparkRow(StringBuilder sb, Object o)
    {
        if (o instanceof Row)
        {
            Row r = (Row)o;
            sb.append("{");
            StructField[] fields = r.schema().fields();
            for (int i=0;i<r.size();i++) {
                sb.append(fields[i].name());
                sb.append(":");
                maybeFormatSparkRow(sb, r.get(i));
                if (i != r.size()-1)
                {
                    sb.append(',');
                }
            }
            sb.append("}");
        }
        else if (o instanceof String)
        {
            sb.append(String.format("'%s'", o));
        }
        else
        {
            sb.append(String.format("%s", o));
        }
    }

    private static String getFormattedData(shaded.com.datastax.driver.core.Row row)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(row.getLong(0));
        sb.append(":");
        sb.append(row.getUDTValue(1)); // Formats as field:value with no whitespaces, and strings quoted
        return sb.toString();
    }
}
