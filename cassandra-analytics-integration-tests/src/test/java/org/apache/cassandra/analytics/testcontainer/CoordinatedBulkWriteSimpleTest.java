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

package org.apache.cassandra.analytics.testcontainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.util.Modules;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.testcontainer.BulkWriteS3CompatModeSimpleTest.S3MockProxyConfigurationImpl;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl.Builder;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase.IntegrationTestModule;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.analytics.SparkTestUtils.sidecarInstancesOptionStream;
import static org.apache.cassandra.analytics.testcontainer.BulkWriteS3CompatModeSimpleTest.BUCKET_NAME;
import static org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl.DEFAULT_API_CALL_TIMEOUT_MILLIS;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes to 2 cassandra clusters, each with the exact same table schema
 * Coordinated by LocalCoordinatedStorageTransportExtension
 */
public class CoordinatedBulkWriteSimpleTest extends CoordinatedWriteTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedBulkWriteSimpleTest.class);

    @Test
    void testCoordinatedWriteToTwoClusters() throws Exception
    {
        try (IClusterExtension<? extends IInstance> cluster1 = classLoaderWrapper.loadCluster(testVersion.version(), clusterConfiguration());
             IClusterExtension<? extends IInstance> cluster2 = classLoaderWrapper.loadCluster(testVersion.version(), clusterConfiguration());
             S3MockContainer s3 = new S3MockContainer("2.17.0").withInitialBuckets(BUCKET_NAME))
        {
            LOGGER.info("Both Cassandra clusters are up");

            s3.start();
            assertThat(s3.isRunning()).isTrue();
            LOGGER.info("S3Mock started");

            QualifiedName tableName = new QualifiedName("ks", "coordinatedwrite");
            createSchema(tableName, cluster1, cluster2);
            LOGGER.info("Test schema created on both clusters");

            Server sidecar1 = startSidecarWithInstances(cluster1, s3);
            Server sidecar2 = startSidecarWithInstances(cluster2, s3);
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS); // wait additional time
            LOGGER.info("Both Sidecars are up. sidecar1 port1: {}, sidecar2 port1: {}", sidecar1.actualPort(), sidecar2.actualPort());

            SparkSession spark = SparkSession
                                 .builder()
                                 .config(sparkTestUtils.defaultSparkConf())
                                 .getOrCreate();
            Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
            String coordinatedConf = coordinatedWriteConfiguration(cluster1, sidecar1, cluster2, sidecar2);
            Map<String, String> s3CompatOptions = ImmutableMap.of(
            "data_transport", "S3_COMPAT",
            "data_transport_extension_class", LocalCoordinatedStorageTransportExtension.class.getCanonicalName(),
            "storage_client_endpoint_override", s3.getHttpEndpoint(), // point to s3Mock server
            "coordinated_write_config", coordinatedConf);
            // start bulk write
            sparkTestUtils
            .coordinatedBulkWriterDataFrameWriter(df, tableName, s3CompatOptions)
            .save();

            // validate by comparing the written data (in each cluster) with input rows
            List<Row> rows = df.collectAsList();
            sparkTestUtils.validateWrites(rows, queryAllData(cluster1, tableName, ConsistencyLevel.ALL));
            sparkTestUtils.validateWrites(rows, queryAllData(cluster2, tableName, ConsistencyLevel.ALL));

            stopSidecarServers(sidecar1, sidecar2);
        }
    }

    private void createSchema(QualifiedName tableName, IClusterExtension... clusters)
    {
        String createKs = "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };";
        String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (id int, course text, marks int, PRIMARY KEY (id)) WITH read_repair='NONE';";
        for (IClusterExtension cluster : clusters)
        {
            cluster.schemaChangeIgnoringStoppedInstances(createKs);
            cluster.schemaChangeIgnoringStoppedInstances(createTable);
        }
    }

    private Server startSidecarWithInstances(Iterable<? extends IInstance> instances, S3MockContainer s3Mock) throws Exception
    {
        VertxTestContext context = new VertxTestContext();
        Function<Builder, Builder> sidecarConfigurator = builder -> {
            S3MockProxyConfigurationImpl s3MockProxyConfiguration = new S3MockProxyConfigurationImpl(s3Mock.getHttpEndpoint());
            S3ClientConfiguration s3ClientConfig = new S3ClientConfigurationImpl("s3-client", 4, 60L,
                                                                                 5242880, DEFAULT_API_CALL_TIMEOUT_MILLIS,
                                                                                 s3MockProxyConfiguration);
            builder.s3ClientConfiguration(s3ClientConfig);
            return builder;
        };
        AbstractModule testModule = new IntegrationTestModule(instances,
                                                              classLoaderWrapper,
                                                              mtlsTestHelper,
                                                              dnsResolver,
                                                              sidecarConfigurator);
        sidecarServerInjector = Guice.createInjector(Modules.override(new MainModule()).with(testModule));
        Server sidecarServer = sidecarServerInjector.getInstance(Server.class);
        sidecarServer.start()
                     .onSuccess(s -> context.completeNow())
                     .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
        return sidecarServer;
    }

    private ClusterBuilderConfiguration clusterConfiguration()
    {
        ClusterBuilderConfiguration conf = new ClusterBuilderConfiguration();
        conf.nodesPerDc(3);
        conf.dcCount(1);
        return conf;
    }

    private String coordinatedWriteConfiguration(IClusterExtension<? extends IInstance> cluster1, Server sidecar1,
                                                 IClusterExtension<? extends IInstance> cluster2, Server sidecar2)
    {
        String cluster1Instances = sidecarInstancesOptionStream(cluster1, dnsResolver)
                                   .map(ip -> ip + ':' + sidecar1.actualPort())
                                   .collect(Collectors.joining("\", \"", "\"", "\""));
        String cluster2Instances = sidecarInstancesOptionStream(cluster2, dnsResolver)
                                   .map(ip -> ip + ':' + sidecar2.actualPort())
                                   .collect(Collectors.joining("\",\"", "\"", "\""));

        return "{" +
               "\"cluster1\":" +
               "{\"sidecarContactPoints\":[" + cluster1Instances + "], \"localDc\":\"datacenter1\"}," +
               "\"cluster2\":" +
               "{\"sidecarContactPoints\":[" + cluster2Instances + "], \"localDc\":\"datacenter1\"}" +
               "}";
    }

    private Object[][] queryAllData(IClusterExtension cluster, QualifiedName table, ConsistencyLevel consistencyLevel)
    {
        return cluster.getFirstRunningInstance()
                      .coordinator()
                      .execute(String.format("SELECT * FROM %s;", table), consistencyLevel);
    }
}
