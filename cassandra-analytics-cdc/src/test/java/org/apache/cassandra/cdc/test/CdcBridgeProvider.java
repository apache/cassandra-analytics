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

import org.apache.cassandra.bridge.BridgeInitializationParameters;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.msg.jdk.JdkMessageConverter;

public class CdcBridgeProvider
{
    private static final ConcurrentMap<CassandraVersion, CdcOptions> OPTIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, Path> COMMIT_LOG_DIRS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, CassandraBridge> BRIDGES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, CdcBridge> CDC_BRIDGES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<CassandraVersion, JdkMessageConverter> MESSAGE_CONVERTERS = new ConcurrentHashMap<>();

    static
    {
        setup();
    }

    private CdcBridgeProvider()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static void setup()
    {
        TestVersionSupplier.testVersions().forEach(v -> {
            try
            {
                setup(v);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        });
    }

    private static void setup(CassandraVersion version) throws IOException
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
