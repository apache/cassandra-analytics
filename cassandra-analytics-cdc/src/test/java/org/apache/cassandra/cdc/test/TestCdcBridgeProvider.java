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

package org.apache.cassandra.cdc.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.bridge.BridgeInitializationParameters;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.msg.jdk.JdkMessageConverter;

/**
 * Test utility for managing CDC bridge instances per version. Call {@link #setup()} explicitly
 * before using any getter methods (e.g. from a {@code @BeforeAll} hook). Call {@link #tearDown()}
 * to release all cached bridges, temp directories, and classloaders.
 */
public class TestCdcBridgeProvider
{
    private static final ConcurrentMap<CassandraVersion, CdcOptions> OPTIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, Path> COMMIT_LOG_DIRS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, CassandraBridge> BRIDGES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, CdcBridge> CDC_BRIDGES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, JdkMessageConverter> MESSAGE_CONVERTERS = new ConcurrentHashMap<>();
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    private TestCdcBridgeProvider()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Initializes bridge instances for all test versions. Idempotent via {@link AtomicBoolean};
     * only the first call performs initialization. Must be called from {@code @BeforeAll} in test classes.
     */
    public static void setup()
    {
        if (initialized.compareAndSet(false, true))
        {
            TestVersionSupplier.testVersions().forEach(v -> {
                try
                {
                    setupVersion(v);
                }
                catch (IOException e)
                {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    /**
     * Releases all cached bridges, temp commit log directories, and resets the bridge factory.
     * Call from {@code @AfterAll} in test base classes when explicit cleanup is needed.
     */
    public static void tearDown()
    {
        OPTIONS.clear();
        CDC_BRIDGES.clear();
        BRIDGES.clear();
        MESSAGE_CONVERTERS.clear();
        COMMIT_LOG_DIRS.forEach((version, dir) -> {
            try
            {
                Files.deleteIfExists(dir);
            }
            catch (IOException ignored)
            {
                // best-effort cleanup
            }
        });
        COMMIT_LOG_DIRS.clear();
        CdcBridgeFactory.close();
        initialized.set(false);
    }

    private static void setupVersion(CassandraVersion version) throws IOException
    {
        OPTIONS.put(version, new CdcOptions()
        {
            public int minimumReplicas(String keyspace)
            {
                return 1;
            }

            public CassandraVersion version()
            {
                return version;
            }
        });
        Path commitLogDir = Files.createTempDirectory(UUID.randomUUID().toString());
        COMMIT_LOG_DIRS.put(version, commitLogDir);

        BRIDGES.put(version, CdcBridgeFactory.get(version));
        MESSAGE_CONVERTERS.put(version, new JdkMessageConverter(BRIDGES.get(version).cassandraTypes()));

        // When the bridge jar is shared across versions (e.g. FOURONE uses the four-zero bridge,
        // whose getVersion() returns FOURZERO), register under the bridge's reported version too.
        // This ensures lookups via bridge.getVersion() find the correct entries.
        CassandraBridge bridgeInstance = BRIDGES.get(version);
        CassandraVersion bridgeVersion = bridgeInstance.getVersion();
        if (bridgeVersion != version)
        {
            OPTIONS.putIfAbsent(bridgeVersion, OPTIONS.get(version));
            COMMIT_LOG_DIRS.putIfAbsent(bridgeVersion, commitLogDir);
            BRIDGES.putIfAbsent(bridgeVersion, bridgeInstance);
            MESSAGE_CONVERTERS.putIfAbsent(bridgeVersion, MESSAGE_CONVERTERS.get(version));
        }
    }

    public static CdcOptions getCdcOptions(CassandraVersion version)
    {
        return OPTIONS.get(version);
    }

    public static Path getCommitLogDir(CassandraVersion version)
    {
        return COMMIT_LOG_DIRS.get(version);
    }

    public static CassandraBridge getCassandraBridge(CassandraVersion version)
    {
        return BRIDGES.get(version);
    }

    public static JdkMessageConverter getMessageConverter(CassandraVersion version)
    {
        return MESSAGE_CONVERTERS.get(version);
    }

    public static CdcBridge getTestCdcBridge(CassandraVersion version)
    {
        return CDC_BRIDGES.computeIfAbsent(version, v -> createCdcBridge(version, COMMIT_LOG_DIRS.get(version), 32, false));
    }

    public static CdcBridge createCdcBridge(CassandraVersion version,
                                            Path directory,
                                            int commitLogSegmentSize,
                                            boolean enableCompression)
    {
        CdcBridge bridge = CdcBridgeFactory.getCdcBridge(version);
        try
        {
            // TODO: Refactor static initialization to instance method.
            // use reflection to execute static initialization
            Method setupMethod = bridge.getClass().getMethod("setup", Path.class, int.class, boolean.class, BridgeInitializationParameters.class);
            setupMethod.invoke(null, directory, commitLogSegmentSize, enableCompression, BridgeInitializationParameters.fromEnvironment());
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Failed to setup CdcBridge", e);
        }
        return bridge;
    }
}
