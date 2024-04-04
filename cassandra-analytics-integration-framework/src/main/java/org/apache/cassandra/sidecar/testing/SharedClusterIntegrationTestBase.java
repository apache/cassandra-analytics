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

package org.apache.cassandra.sidecar.testing;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.IsolatedDTestClassLoaderWrapper;
import org.apache.cassandra.testing.TestUtils;
import org.apache.cassandra.testing.TestVersion;
import org.apache.cassandra.testing.TestVersionSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class provides an opinionated way to run integration tests. The {@link #setup()} method runs once at the
 * beginning of all the tests in the implementation, as well as the {@link #tearDown()} method. The tests will share
 * the same cluster throughout the lifetime of the tests, which means that implementers must be aware that any cluster
 * alteration will have an impact on subsequent test runs, so it is recommended that tests run in isolated
 * keyspaces/tables when required. Additionally, the state of the cluster should ideally remain the same for all
 * tests, so ideally tests should not alter the state of the cluster in a way that would affect other tests.
 *
 * <p>The setup will run the following steps:
 *
 * <ol>
 *     <li>Find the first version from the {@link TestVersionSupplier#testVersions()}
 *     <li>(Optional) Before cluster provisioning (implementer can supply)
 *     <li>Provision a cluster for the test using the version from the previous step (implementer must supply)
 *     <li>(Optional) After cluster provisioned (implementer can supply)
 *     <li>Initialize schemas required for the test (implementer must supply)
 *     <li>Start sidecar that talks to the provisioned cluster
 *     <li>(Optional) Run the before test start method (implementer can supply)
 * </ol>
 *
 * <p>The above order guarantees that the cluster and Sidecar are both ready by the time the test
 * setup completes. Removing the need to wait for schema propagation from the cluster to Sidecar,
 * and removing the need to poll for schema changes to propagate. This helps in improving test
 * time.
 *
 * <p>For the teardown of the test the steps are the following:
 *
 * <ol>
 *     <li>(Optional) Before sidecar stops (implementer can supply)
 *     <li>Stop sidecar
 *     <li>(Optional) Before cluster shutdowns (implementer can supply)
 *     <li>Close cluster
 *     <li>(Optional) Before tear down ends (implementer can supply)
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
public abstract class SharedClusterIntegrationTestBase
{
    protected final Logger logger = LoggerFactory.getLogger(SharedClusterIntegrationTestBase.class);
    private static final int MAX_CLUSTER_PROVISION_RETRIES = 5;

    protected Vertx vertx;
    protected DnsResolver dnsResolver;
    protected IClusterExtension<? extends IInstance> cluster;
    protected Server server;
    protected Injector injector;
    protected TestVersion testVersion;
    private IsolatedDTestClassLoaderWrapper classLoaderWrapper;

    static
    {
        // Initialize defaults to configure the in-jvm dtest
        TestUtils.configureDefaultDTestJarProperties();
    }

    @BeforeAll
    protected void setup() throws InterruptedException
    {
        Optional<TestVersion> testVersionGet = TestVersionSupplier.testVersions().findFirst();
        assertThat(testVersionGet).isPresent();
        testVersion = testVersionGet.get();
        logger.info("Testing with version={}", testVersion);

        classLoaderWrapper = new IsolatedDTestClassLoaderWrapper();
        classLoaderWrapper.initializeDTestJarClassLoader(testVersion, SharedClusterIntegrationTestBase.class);

        beforeClusterProvisioning();
        cluster = provisionClusterWithRetries(testVersion);
        assertThat(cluster).isNotNull();
        afterClusterProvisioned();
        initializeSchemaForTest();
        startSidecar(cluster, classLoaderWrapper);
        beforeTestStart();
    }

    @AfterAll
    protected void tearDown() throws Exception
    {
        try
        {
            beforeSidecarStop();
            stopSidecar();
            beforeClusterShutdown();
            closeCluster();
            afterClusterShutdown();
        }
        finally
        {
            if (classLoaderWrapper != null)
            {
                classLoaderWrapper.closeDTestJarClassLoader();
            }
        }
    }

    /**
     * Returns the configuration for the test cluster. The default configuration for the cluster has 1
     * node, 1 DC, 1 data directory per node, with the {@link org.apache.cassandra.distributed.api.Feature#GOSSIP},
     * {@link org.apache.cassandra.distributed.api.Feature#JMX}, and
     * {@link org.apache.cassandra.distributed.api.Feature#NATIVE_PROTOCOL} features enabled. It uses dynamic port
     * allocation for the Cassandra service ports. This method can be overridden to provide a different configuration
     * for the cluster.
     *
     * @return the configuration for the test cluster
     */
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return new ClusterBuilderConfiguration();
    }

    /**
     * Override to perform an action before the cluster provisioning
     */
    protected void beforeClusterProvisioning()
    {
    }

    /**
     * Provisions a cluster with the provided {@link TestVersion}. Up to {@link #MAX_CLUSTER_PROVISION_RETRIES}
     * attempts will be made to provision a cluster when it fails to provision.
     *
     * @param testVersion the version for the test
     * @return the provisioned cluster
     */
    private IClusterExtension<? extends IInstance> provisionClusterWithRetries(TestVersion testVersion)
    {
        for (int retry = 0; retry < MAX_CLUSTER_PROVISION_RETRIES; retry++)
        {
            try
            {
                return classLoaderWrapper.loadCluster(testVersion.version(), testClusterConfiguration());
            }
            catch (Exception exception)
            {
                boolean addressAlreadyInUse =
                ThrowableUtils.getCause(exception, ex -> ex instanceof BindException &&
                                                         ex.getMessage() != null &&
                                                         ex.getMessage().contains("Address already in use")) != null;
                if (addressAlreadyInUse)
                {
                    logger.warn("Failed to provision cluster after {} retries", retry, exception);
                }
                else if (exception instanceof RuntimeException)
                {
                    throw (RuntimeException) exception;
                }
                else
                {
                    throw new RuntimeException(exception);
                }
            }
        }
        throw new RuntimeException("Unable to provision cluster after " + MAX_CLUSTER_PROVISION_RETRIES + " retries");
    }

    /**
     * Override to perform an action after the cluster has been successfully provisioned
     */
    protected void afterClusterProvisioned()
    {
    }

    /**
     * Initialize required schemas for the tests upfront before the test starts
     */
    protected abstract void initializeSchemaForTest();

    /**
     * Override to perform an action before the tests start
     */
    protected void beforeTestStart()
    {
    }

    /**
     * Override to perform an action before Sidecar stops
     */
    protected void beforeSidecarStop()
    {
    }

    /**
     * Override to perform an action before the cluster stops
     */
    protected void beforeClusterShutdown()
    {
    }

    /**
     * Override to perform an action after the cluster has shutdown
     */
    protected void afterClusterShutdown()
    {
    }

    protected void createTestKeyspace(QualifiedName name, Map<String, Integer> rf)
    {
        createTestKeyspace(name.maybeQuotedKeyspace(), rf);
    }

    protected void createTestKeyspace(String keyspace, Map<String, Integer> rf)
    {
        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
                                                     generateRfString(rf) + " };");
    }

    protected void createTestTable(QualifiedName name, String createTableStatement)
    {
        cluster.schemaChangeIgnoringStoppedInstances(String.format(createTableStatement, name));
    }

    /**
     * Starts Sidecar configured to run against the provided Cassandra {@code cluster}.
     *
     * @param cluster the cluster to use
     * @param wrapper the classloader helper class
     * @throws InterruptedException when the time exceeds to start Sidecar
     */
    protected void startSidecar(ICluster<? extends IInstance> cluster,
                                IsolatedDTestClassLoaderWrapper wrapper) throws InterruptedException
    {
        VertxTestContext context = new VertxTestContext();
        injector = Guice.createInjector(Modules.override(new MainModule()).with(new IntegrationTestModule(cluster, wrapper)));
        dnsResolver = injector.getInstance(DnsResolver.class);
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    /**
     * Stops the Sidecar service
     *
     * @throws InterruptedException when stopping sidecar times out
     */
    protected void stopSidecar() throws InterruptedException
    {
        if (server == null)
        {
            return;
        }
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
        {
            logger.info("Close event received before timeout.");
        }
        else
        {
            logger.error("Close event timed out.");
        }
    }

    /**
     * Closes the cluster and its resources
     *
     * @throws Exception on an exception generated during cluster shutdown
     */
    protected void closeCluster() throws Exception
    {
        if (cluster == null)
        {
            return;
        }
        logger.info("Closing cluster={}", cluster);
        try
        {
            cluster.close();
        }
        // ShutdownException may be thrown from a different classloader, and therefore the standard
        // `catch (ShutdownException)` won't always work - compare the canonical names instead.
        catch (Throwable t)
        {
            if (Objects.equals(t.getClass().getCanonicalName(),
                               "org.apache.cassandra.distributed.shared.ShutdownException"))
            {
                logger.debug("Encountered shutdown exception which closing the cluster", t);
            }
            else
            {
                throw t;
            }
        }
    }

    protected String generateRfString(Map<String, Integer> rf)
    {
        return rf.entrySet()
                 .stream()
                 .map(entry -> String.format("'%s':%d", entry.getKey(), entry.getValue()))
                 .collect(Collectors.joining(","));
    }

    /**
     * Convenience method to query all data from the provided {@code table} at consistency level {@code LOCAL_QUORUM}.
     *
     * @param table the qualified Cassandra table name
     * @return all the data queried from the table
     */
    protected Object[][] queryAllData(QualifiedName table)
    {
        return queryAllData(table, ConsistencyLevel.LOCAL_QUORUM);
    }

    /**
     * Convenience method to query all data from the provided {@code table} at the specified consistency level.
     *
     * @param table            the qualified Cassandra table name
     * @param consistencyLevel the consistency level to use for querying the data
     * @return all the data queried from the table
     */
    protected Object[][] queryAllData(QualifiedName table, ConsistencyLevel consistencyLevel)
    {
        return cluster.coordinator(1).execute(String.format("SELECT * FROM %s;", table), consistencyLevel);
    }

    /**
     * Convenience method to query all data from the provided {@code table} at consistency level ALL.
     *
     * @param table the qualified Cassandra table name
     * @return all the data queried from the table
     */
    protected ResultSet queryAllDataWithDriver(QualifiedName table)
    {
        return queryAllDataWithDriver(table, com.datastax.driver.core.ConsistencyLevel.ALL);
    }

    /**
     * Convenience method to query all data from the provided {@code table} at the specified consistency level.
     *
     * @param table       the qualified Cassandra table name
     * @param consistency the consistency level to use for querying the data
     * @return all the data queried from the table
     */
    protected ResultSet queryAllDataWithDriver(QualifiedName table,
                                               com.datastax.driver.core.ConsistencyLevel consistency)
    {
        Cluster driverCluster = createDriverCluster(cluster.delegate());
        Session session = driverCluster.connect();
        SimpleStatement statement = new SimpleStatement(String.format("SELECT * FROM %s;", table));
        statement.setConsistencyLevel(consistency);
        return session.execute(statement);
    }

    // Utility methods

    public static Cluster createDriverCluster(ICluster<? extends IInstance> dtest)
    {
        dtest.stream().forEach((i) -> {
            if (!i.config().has(Feature.NATIVE_PROTOCOL) || !i.config().has(Feature.GOSSIP))
            {
                throw new IllegalStateException("Java driver requires Feature.NATIVE_PROTOCOL and Feature.GOSSIP; " +
                                                "but one or more is missing");
            }
        });
        Cluster.Builder builder = Cluster.builder();
        dtest.stream().forEach((i) -> {
            InetSocketAddress address = new InetSocketAddress(i.broadcastAddress().getAddress(),
                                                              i.config().getInt("native_transport_port"));
            builder.addContactPointsWithPorts(address);
        });

        return builder.build();
    }

    static class IntegrationTestModule extends AbstractModule
    {
        private final ICluster<? extends IInstance> cluster;
        private final IsolatedDTestClassLoaderWrapper wrapper;

        IntegrationTestModule(ICluster<? extends IInstance> cluster, IsolatedDTestClassLoaderWrapper wrapper)
        {
            this.cluster = cluster;
            this.wrapper = wrapper;
        }

        @Provides
        @Singleton
        public InstancesConfig instancesConfig(Vertx vertx,
                                               SidecarConfiguration configuration,
                                               CassandraVersionProvider cassandraVersionProvider,
                                               SidecarVersionProvider sidecarVersionProvider,
                                               DnsResolver dnsResolver)
        {
            JmxConfiguration jmxConfiguration = configuration.serviceConfiguration().jmxConfiguration();

            List<InetSocketAddress> contactPoints = buildContactPoints();
            CQLSessionProvider cqlSessionProvider = new TemporaryCqlSessionProvider(contactPoints,
                                                                                    SharedExecutorNettyOptions.INSTANCE);

            List<InstanceMetadata> instanceMetadataList =
            IntStream.rangeClosed(1, cluster.size())
                     .mapToObj(i -> buildInstanceMetadata(vertx,
                                                          cluster.get(i),
                                                          cassandraVersionProvider,
                                                          sidecarVersionProvider.sidecarVersion(),
                                                          jmxConfiguration,
                                                          cqlSessionProvider,
                                                          dnsResolver,
                                                          wrapper))
                     .collect(Collectors.toList());
            return new InstancesConfigImpl(instanceMetadataList, dnsResolver);
        }

        @Provides
        @Singleton
        public SidecarConfiguration sidecarConfiguration()
        {
            ServiceConfiguration conf = ServiceConfigurationImpl.builder()
                                                                .host("0.0.0.0") // binds to all interfaces, potential security issue if left running for long
                                                                .port(0) // let the test find an available port
                                                                .build();
            return SidecarConfigurationImpl.builder()
                                           .serviceConfiguration(conf)
                                           .build();
        }

        @Provides
        @Singleton
        public DnsResolver dnsResolver()
        {
            return new LocalhostResolver();
        }

        private List<InetSocketAddress> buildContactPoints()
        {
            return cluster.stream()
                          .map(instance -> new InetSocketAddress(instance.config().broadcastAddress().getAddress(),
                                                                 tryGetIntConfig(instance.config(), "native_transport_port", 9042)))
                          .collect(Collectors.toList());
        }

        static int tryGetIntConfig(IInstanceConfig config, String configName, int defaultValue)
        {
            try
            {
                return config.getInt(configName);
            }
            catch (NullPointerException npe)
            {
                return defaultValue;
            }
        }

        static InstanceMetadata buildInstanceMetadata(Vertx vertx,
                                                      IInstance cassandraInstance,
                                                      CassandraVersionProvider versionProvider,
                                                      String sidecarVersion,
                                                      JmxConfiguration jmxConfiguration,
                                                      CQLSessionProvider session,
                                                      DnsResolver dnsResolver,
                                                      IsolatedDTestClassLoaderWrapper wrapper)
        {
            IInstanceConfig config = cassandraInstance.config();
            String ipAddress = JMXUtil.getJmxHost(config);
            String hostName;
            try
            {
                hostName = dnsResolver.reverseResolve(ipAddress);
            }
            catch (UnknownHostException e)
            {
                hostName = ipAddress;
            }
            int port = tryGetIntConfig(config, "native_transport_port", 9042);
            String[] dataDirectories = (String[]) config.get("data_file_directories");
            String stagingDir = stagingDir(dataDirectories);

            JmxClient jmxClient = new JmxClientProxy(wrapper,
                                                     JmxClient.builder()
                                                              .host(ipAddress)
                                                              .port(config.jmxPort())
                                                              .connectionMaxRetries(jmxConfiguration.maxRetries())
                                                              .connectionRetryDelayMillis(jmxConfiguration.retryDelayMillis()));
            CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                             config.num(),
                                                                             versionProvider,
                                                                             session,
                                                                             jmxClient,
                                                                             new DriverUtils(),
                                                                             sidecarVersion,
                                                                             ipAddress,
                                                                             port);
            return InstanceMetadataImpl.builder()
                                       .id(config.num())
                                       .host(hostName)
                                       .port(port)
                                       .dataDirs(Arrays.asList(dataDirectories))
                                       .stagingDir(stagingDir)
                                       .delegate(delegate)
                                       .globalMetricRegistryName("test")
                                       .build();
        }

        private static String stagingDir(String[] dataDirectories)
        {
            // Use the parent of the first data directory as the staging directory
            Path dataDirParentPath = Paths.get(dataDirectories[0]).getParent();
            // If the cluster has not started yet, the node's root directory doesn't exist yet
            assertThat(dataDirParentPath).isNotNull();
            Path stagingPath = dataDirParentPath.resolve("staging");
            return stagingPath.toFile().getAbsolutePath();
        }
    }

    protected JmxClient wrapJmxClient(JmxClient.Builder builder)
    {
        return new JmxClientProxy(classLoaderWrapper, builder);
    }

    /**
     * Runs JMXClient calls with the dtest classloader
     */
    static class JmxClientProxy extends JmxClient
    {
        private final IsolatedDTestClassLoaderWrapper wrapper;

        protected JmxClientProxy(IsolatedDTestClassLoaderWrapper wrapper, Builder builder)
        {
            super(Objects.requireNonNull(builder, "builder must be provided"));
            this.wrapper = wrapper;
        }

        /**
         * Connects to JMX by running inside the wrapped classloader. The connection to JMX must be established in the
         * dtest classloader to be able to communicate to Cassandra's JMX.
         *
         * @param currentAttempt the current attempt to connect
         * @throws IOException if the underlying operation throws an IOException
         */
        @Override
        protected void connectInternal(int currentAttempt) throws IOException
        {
            wrapper.executeExceptionableActionOnDTestClassLoader(() -> {
                super.connectInternal(currentAttempt);
                return null;
            });
        }
    }
}
