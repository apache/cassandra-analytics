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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Preconditions;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.spark.utils.Throwing;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class CdcBridgeFactory extends BaseCassandraBridgeFactory
{
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge and SparkSqlTypeConverter
    private static final Map<String, VersionSpecificBridge> CASSANDRA_BRIDGES =
        new ConcurrentHashMap<>(CassandraVersion.values().length);

    public static class VersionSpecificBridge
    {
        public final CassandraBridge cassandraBridge;
        public final CdcBridge cdcBridge;
        @Nullable
        final CqlToAvroSchemaConverter avroSchemaConverter;
        final ClassLoader classLoader;

        public VersionSpecificBridge(CassandraBridge cassandraBridge,
                                     CdcBridge cdcBridge,
                                     @Nullable CqlToAvroSchemaConverter avroSchemaConverter,
                                     ClassLoader classLoader)
        {
            this.cassandraBridge = cassandraBridge;
            this.cdcBridge = cdcBridge;
            this.avroSchemaConverter = avroSchemaConverter;
            this.classLoader = classLoader;
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

    private static CdcBridgeFactory.VersionSpecificBridge getVersionSpecificBridge(@NotNull CassandraVersion version)
    {
        maybeRegisterShutdownHook();
        String jarBaseName = version.jarBaseName();
        Preconditions.checkNotNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CdcBridgeFactory::create);
    }

    @NotNull
    public static CassandraBridge get(@NotNull CassandraVersion version)
    {
        return getVersionSpecificBridge(version).cassandraBridge;
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
        return getVersionSpecificBridge(version).cdcBridge;
    }

    @NotNull
    public static CqlToAvroSchemaConverter getCqlToAvroSchemaConverter(@NotNull CassandraBridge bridge)
    {
        return getCqlToAvroSchemaConverter(bridge.getVersion());
    }

    @Nullable
    public static CqlToAvroSchemaConverter getCqlToAvroSchemaConverter(@NotNull CassandraVersion version)
    {
        return getVersionSpecificBridge(version).avroSchemaConverter;
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
            typesResourceName(label),
            avroResourceName(label)
            );
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            CassandraBridge bridgeInstance = constructor.newInstance();

            Class<CdcBridge> cdcBridgeClass = (Class<CdcBridge>)
                                              loader
                                              .loadClass(CdcBridge.IMPLEMENTATION_FQCN);
            Constructor<CdcBridge> cdcBridgeConstructor = cdcBridgeClass.getConstructor();
            CdcBridge cdcBridgeInstance = cdcBridgeConstructor.newInstance();

            CqlToAvroSchemaConverter cqlToAvroSchemaConverter = null;
            try
            {
                Class<CqlToAvroSchemaConverter> avroBridgeClass = (Class<CqlToAvroSchemaConverter>)
                                                                  loader
                                                                  .loadClass(CdcBridge.CONVERTER_IMPLEMENTATION_FQCN);
                Constructor<CqlToAvroSchemaConverter> cqlToAvroSchemaConverterConstructor = avroBridgeClass.getConstructor();
                cqlToAvroSchemaConverter = cqlToAvroSchemaConverterConstructor.newInstance();
            }
            catch (ClassNotFoundException ignore)
            {
            }

            return new VersionSpecificBridge(bridgeInstance, cdcBridgeInstance, cqlToAvroSchemaConverter, loader);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }

    /**
     * Returns whether the shutdown hook has been registered.
     */
    @VisibleForTesting
    static boolean isShutdownHookRegistered()
    {
        return SHUTDOWN_HOOK_REGISTERED.get();
    }

    /**
     * Registers a JVM shutdown hook that calls {@link #close()} to release classloader resources.
     * Clears the {@link TypeCache} first so type references are released before classloaders close.
     * Uses CAS to ensure the hook is registered at most once.
     */
    @VisibleForTesting
    static void maybeRegisterShutdownHook()
    {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true))
        {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try
                {
                    TypeCache.clear();
                    close();
                }
                catch (Exception e)
                {
                    // best-effort cleanup during JVM shutdown
                }
            }, CdcBridgeFactory.class.getSimpleName() + "-shutdown"));
        }
    }

    /**
     * Closes all cached bridge classloaders and clears the cache. Call during application shutdown
     * to release classloader resources and delete temp JAR files.
     *
     * Do not use this method outside testing; rely on the shutdown hook logic to clean things up in prod.
     */
    @VisibleForTesting
    public static void close()
    {
        closeBridges(CASSANDRA_BRIDGES, bridge -> bridge.classLoader);
    }

    @VisibleForTesting
    public static boolean areBridgesClosed()
    {
        return CASSANDRA_BRIDGES.isEmpty();
    }

    @VisibleForTesting
    public static <T> T executeActionOnBridgeClassLoader(@NotNull CassandraVersion version, Throwing.Function<ClassLoader, T> action)
    {
        ClassLoader bridgeLoader = getVersionSpecificBridge(version).classLoader;
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        try
        {
            currentThread.setContextClassLoader(bridgeLoader);
            try
            {
                return action.apply(bridgeLoader);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to execute function on bridge classloader", e);
            }
        }
        finally
        {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }
}
