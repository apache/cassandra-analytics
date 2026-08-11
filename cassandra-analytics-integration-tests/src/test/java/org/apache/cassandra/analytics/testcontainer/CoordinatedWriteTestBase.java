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

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import org.apache.cassandra.analytics.SparkTestUtils;
import org.apache.cassandra.analytics.SparkTestUtilsProvider;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.LocalhostMultiSubnetResolver;
import org.apache.cassandra.sidecar.testing.MtlsTestHelper;
import org.apache.cassandra.testing.IsolatedDTestClassLoaderWrapper;
import org.apache.cassandra.testing.TestVersion;
import org.apache.cassandra.testing.TestVersionSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Non-opinionated base class for in-jvm dtest. It only sets up a few necessary utilities.
 * The subclasses create C* clusters et la., according to its needs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class CoordinatedWriteTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedWriteTestBase.class);

    @TempDir
    static Path secretsPath;
    protected MtlsTestHelper mtlsTestHelper;
    protected IsolatedDTestClassLoaderWrapper classLoaderWrapper;
    protected DnsResolver dnsResolver1 = new LocalhostMultiSubnetResolver(0);
    protected DnsResolver dnsResolver2 = new LocalhostMultiSubnetResolver(1);
    protected Injector sidecarServerInjector;
    protected SparkTestUtils sparkTestUtils = SparkTestUtilsProvider.utils();
    protected TestVersion testVersion;

    @BeforeAll
    protected void setup() throws Exception
    {
        System.setProperty("cassandra.analytics.bridges.sstable_format", "bti");
        Optional<TestVersion> maybeTestVersion = TestVersionSupplier.testVersions().findFirst();
        assertThat(maybeTestVersion).isPresent();
        this.testVersion = maybeTestVersion.get();
        LOGGER.info("Testing with version={}", testVersion);

        classLoaderWrapper = new IsolatedDTestClassLoaderWrapper();
        classLoaderWrapper.initializeDTestJarClassLoader(testVersion, TestVersion.class);
        mtlsTestHelper = new MtlsTestHelper(secretsPath, false);
        sparkTestUtils.setMtlsTestHelper(mtlsTestHelper);
    }

    protected void stopSidecarServers(Server... servers) throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(servers.length);
        for (Server server : servers)
        {
            server.close().onSuccess(res -> closeLatch.countDown());
        }
        if (closeLatch.await(60, TimeUnit.SECONDS))
        {
            LOGGER.info("Close event received before timeout.");
        }
        else
        {
            LOGGER.error("Close event timed out.");
        }
    }
}
