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

package org.apache.cassandra.bridge;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.NotNull;

public final class CdcBridgeFactory extends BaseCassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge and SparkSqlTypeConverter
    private static final Map<String, VersionSpecificBridge> CASSANDRA_BRIDGES =
    new ConcurrentHashMap<>(CassandraVersion.values().length);

    public static class VersionSpecificBridge
    {
        public final CassandraBridge cassandraBridge;
        public final CdcBridge cdcBridge;

        public VersionSpecificBridge(CassandraBridge cassandraBridge, CdcBridge cdcBridge)
        {
            this.cassandraBridge = cassandraBridge;
            this.cdcBridge = cdcBridge;
        }
    }

    private CdcBridgeFactory()
    {
        super();
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @NotNull
    public static CassandraBridge get(@NotNull String version)
    {
        return get(getCassandraVersion(version));
    }

    @NotNull
    public static CassandraBridge get(@NotNull CassandraVersionFeatures features)
    {
        return get(getCassandraVersion(features));
    }

    @NotNull
    public static CassandraBridge get(@NotNull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        if (jarBaseName == null)
        {
            throw new NullPointerException("Cassandra version " + version + " is not supported");
        }
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CdcBridgeFactory::create).cassandraBridge;
    }

    @NotNull
    public static CdcBridge getCdcBridge(@NotNull CassandraVersionFeatures features)
    {
        return getCdcBridge(getCassandraVersion(features));
    }

    @NotNull
    public static CdcBridge getCdcBridge(@NotNull CassandraBridge bridge)
    {
        return getCdcBridge(bridge.getVersion());
    }

    @NotNull
    public static CdcBridge getCdcBridge(@NotNull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        if (jarBaseName == null)
        {
            throw new NullPointerException("Cassandra version " + version + " is not supported");
        }
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CdcBridgeFactory::create).cdcBridge;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static VersionSpecificBridge create(@NotNull String label)
    {
        try
        {
            ClassLoader loader = buildClassLoader(
            cassandraResourceName(label),
            bridgeResourceName(label),
            typesResourceName(label));
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            CassandraBridge bridgeInstance = constructor.newInstance();

            Class<CdcBridge> cdcBridgeClass = (Class<CdcBridge>)
                                             loader
                                             .loadClass(CdcBridge.IMPLEMENTATION_FQCN);
            Constructor<CdcBridge> cdcBridgeConstructor = cdcBridgeClass.getConstructor();
            CdcBridge cdcBridgeInstance = cdcBridgeConstructor.newInstance();
            return new VersionSpecificBridge(bridgeInstance, cdcBridgeInstance);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }
}
