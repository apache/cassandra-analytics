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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import org.jetbrains.annotations.NotNull;

public final class CassandraBridgeFactory
{
    private static final Map<String, CassandraBridge> CASSANDRA_BRIDGES = new ConcurrentHashMap<>(CassandraVersion.values().length);

    private CassandraBridgeFactory()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @NotNull
    public static CassandraVersion getCassandraVersion(@NotNull String version)
    {
        CassandraVersionFeatures features = CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version);
        Preconditions.checkArgument(features != null, "Cassandra version " + version + " is not supported");
        return getCassandraVersion(features);
    }

    @NotNull
    public static CassandraVersion getCassandraVersion(@NotNull CassandraVersionFeatures features)
    {
        Optional<CassandraVersion> version = Arrays.stream(CassandraVersion.values())
                .filter(value -> value.versionNumber() == features.getMajorVersion())
                .findAny();
        Preconditions.checkArgument(version.isPresent(), "Cassandra features " + features + " are not supported");
        return version.get();
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
        Preconditions.checkNotNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create);
    }

    /**
     * Ensures that every supported Cassandra version has a corresponding Cassandra bridge implementation embedded
     * into this library's binary as a separate JAR file in the {@code bridges} directory, fails fast otherwise
     *
     * @throws IllegalStateException If a Cassandra bridge implementation is missing for a supported Cassandra version
     */
    public static void validateBridges()
    {
        for (CassandraVersion version : CassandraVersion.implementedVersions())
        {
            String jarBaseName = version.jarBaseName();
            String name = resourceName(jarBaseName);
            URL locator = CassandraBridgeFactory.class.getResource(name);
            if (locator == null)
            {
                throw new IllegalStateException("Missing Cassandra bridge implementation for version " + version);
            }
        }
    }

    @NotNull
    private static String resourceName(@NotNull String label)
    {
        return "/bridges/" + label + ".jar";
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static CassandraBridge create(@NotNull String label)
    {
        try
        {
            String name = resourceName(label);
            InputStream contents = CassandraBridgeFactory.class.getResourceAsStream(name);
            File jar = Files.createTempFile(null, ".jar").toFile();
            FileUtils.copyInputStreamToFile(contents, jar);

            ClassLoader loader = new PostDelegationClassLoader(jar, Thread.currentThread().getContextClassLoader());
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass("org.apache.cassandra.bridge.CassandraBridgeImplementation");
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            return constructor.newInstance();

        }
        catch (IOException | ClassNotFoundException | NoSuchMethodException | InstantiationException
             | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }
}
