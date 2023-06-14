/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.assertj.core.api.Assertions;

/**
 * Base class for integration test.
 * Start an in-jvm dtest cluster at the beginning of each test, and
 * teardown the cluster after each test.
 */
public abstract class IntegrationTestBase
{
    private static final int MAX_KEYSPACE_TABLE_WAIT_ATTEMPTS = 100;
    private static final long MAX_KEYSPACE_TABLE_TIME = 100L;
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Vertx vertx;
    protected HttpServer server;

    protected static final String TEST_KEYSPACE = "testkeyspace";
    private static final String TEST_TABLE_PREFIX = "testtable";

    protected static final int DEFAULT_RF = 3;
    private static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);
    protected CassandraSidecarTestContext sidecarTestContext;

    @BeforeEach
    void setup(AbstractCassandraTestContext cassandraTestContext) throws InterruptedException
    {
        sidecarTestContext = CassandraSidecarTestContext.from(cassandraTestContext, DnsResolver.DEFAULT);
        Injector injector = Guice.createInjector(Modules
                                                 .override(new MainModule())
                                                 .with(new IntegrationTestModule(this.sidecarTestContext)));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.listen(0, context.succeeding(p -> {
            if (sidecarTestContext.isClusterBuilt())
            {
                healthCheck(sidecarTestContext.instancesConfig());
            }
            sidecarTestContext.registerInstanceConfigListener(IntegrationTestBase::healthCheck);
            context.completeNow();
        }));

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
        {
            logger.info("Close event received before timeout.");
        }
        else
        {
            logger.error("Close event timed out.");
        }
        sidecarTestContext.close();
    }

    protected void testWithClient(VertxTestContext context, Consumer<WebClient> tester) throws Exception
    {
        WebClient client = WebClient.create(vertx);

        tester.accept(client);

        // wait until the test completes
        Assertions.assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    protected void createTestKeyspace()
    {
        createTestKeyspace(ImmutableMap.of("datacenter1", 1));
    }

    protected void createTestKeyspace(Map<String, Integer> rf)
    {
        Session session = maybeGetSession();
        session.execute("CREATE KEYSPACE " + TEST_KEYSPACE +
                        " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " + generateRfString(rf) + " };");
    }

    private String generateRfString(Map<String, Integer> dcToRf)
    {
        return dcToRf.entrySet().stream().map(e -> String.format("'%s':%d", e.getKey(), e.getValue()))
                     .collect(Collectors.joining(","));
    }

    protected QualifiedTableName createTestTable(String createTableStatement)
    {
        Session session = maybeGetSession();
        QualifiedTableName tableName = uniqueTestTableFullName();
        session.execute(String.format(createTableStatement, tableName));
        return tableName;
    }

    protected Session maybeGetSession()
    {
        Session session = sidecarTestContext.session();
        Assertions.assertThat(session).isNotNull();
        return session;
    }

    private static QualifiedTableName uniqueTestTableFullName()
    {
        return new QualifiedTableName(TEST_KEYSPACE, TEST_TABLE_PREFIX + TEST_TABLE_ID.getAndIncrement());
    }

    public List<Path> findChildFile(CassandraSidecarTestContext context, String hostname, String target)
    {
        InstanceMetadata instanceConfig = context.instancesConfig().instanceFromHost(hostname);
        List<String> parentDirectories = instanceConfig.dataDirs();

        return parentDirectories.stream().flatMap(s -> findChildFile(Paths.get(s), target).stream())
                                .collect(Collectors.toList());
    }

    private List<Path> findChildFile(Path path, String target)
    {
        try (Stream<Path> walkStream = Files.walk(path))
        {
            return walkStream.filter(p -> p.toString().endsWith(target)
                                          || p.toString().contains("/" + target + "/"))
                             .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            return Collections.emptyList();
        }
    }

    private static void healthCheck(InstancesConfig instancesConfig)
    {
        instancesConfig.instances()
                       .forEach(instanceMetadata -> instanceMetadata.delegate().healthCheck());
    }

    /**
     * Waits for the specified keyspace/table to be available.
     * Emperically, this loop usually executes either zero or one time before completing.
     * However, we set a fairly high number of retries to account for variability in build machines.
     *
     * @param keyspaceName the keyspace for which to wait
     * @param tableName    the table in the keyspace for which to wait
     */
    protected void waitForKeyspaceAndTable(String keyspaceName, String tableName)
    {
        int numInstances = sidecarTestContext.instancesConfig().instances().size();
        int retries = MAX_KEYSPACE_TABLE_WAIT_ATTEMPTS;
        boolean sidecarMissingSchema = true;
        while (sidecarMissingSchema && retries-- > 0)
        {
            sidecarMissingSchema = false;
            for (int i = 0; i < numInstances; i++)
            {
                KeyspaceMetadata keyspace = sidecarTestContext.session(i)
                                                              .getCluster()
                                                              .getMetadata()
                                                              .getKeyspace(keyspaceName);
                sidecarMissingSchema |= (keyspace == null || keyspace.getTable(tableName) == null);
            }
            if (sidecarMissingSchema)
            {
                logger.info("Keyspace/table {}/{} not yet available - waiting...", keyspaceName, tableName);
                Uninterruptibles.sleepUninterruptibly(MAX_KEYSPACE_TABLE_TIME, TimeUnit.MILLISECONDS);
            }
            else
            {
                return;
            }
        }
        throw new RuntimeException(
        String.format("Keyspace/table %s/%s did not become visible on all sidecar instances",
                      keyspaceName, tableName));
    }
}
