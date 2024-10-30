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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Optional;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;

public class BaseCassandraBridgeFactory
{
    protected BaseCassandraBridgeFactory()
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

    /**
     * Ensures that every supported Cassandra version has a corresponding Cassandra bridge implementation embedded
     * into this library's binary as a separate JAR file in the {@code bridges} directory, fails fast otherwise
     *
     * @param expectedCassandraVersions the expected cassandra versions
     * @throws IllegalStateException If a Cassandra bridge implementation is missing for a supported Cassandra version
     */
    public static void validateBridges(CassandraVersion[] expectedCassandraVersions)
    {
        for (CassandraVersion version : expectedCassandraVersions)
        {
            String jarBaseName = version.jarBaseName();
            String cassandraResourceName = cassandraResourceName(jarBaseName);
            URL locator = BaseCassandraBridgeFactory.class.getResource(cassandraResourceName);
            if (locator == null)
            {
                throw new IllegalStateException("Missing Cassandra implementation for version " + version);
            }

            String bridgeResourceName = bridgeResourceName(jarBaseName);
            locator = BaseCassandraBridgeFactory.class.getResource(bridgeResourceName);

            if (locator == null)
            {
                throw new IllegalStateException("Missing Cassandra bridge implementation for version " + version);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static CassandraBridge loadCassandraBridge(String label)
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException
    {
        ClassLoader loader = buildClassLoader(cassandraResourceName(label), bridgeResourceName(label), typesResourceName(label));
        Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
        Constructor<CassandraBridge> constructor = bridge.getConstructor();
        return constructor.newInstance();
    }

    public static ClassLoader buildClassLoader(String... resourceNames)
    {
        URL[] urls = Arrays.stream(resourceNames)
                           .map(BaseCassandraBridgeFactory::copyClassResourceToFile)
                           .map(jar -> {
                               try
                               {
                                   return jar.toURI().toURL();
                               }
                               catch (MalformedURLException e)
                               {
                                   throw new RuntimeException(e);
                               }
                           }).toArray(URL[]::new);

        return new PostDelegationClassLoader(urls, Thread.currentThread().getContextClassLoader());
    }

    public static File copyClassResourceToFile(String resource)
    {
        try (InputStream contents = BaseCassandraBridgeFactory.class.getResourceAsStream(resource))
        {
            if (contents == null)
            {
                throw new NullPointerException("Could not find resource: " + resource);
            }
            Path jarPath = Files.createTempFile(null, ".jar");
            Files.copy(contents, jarPath, StandardCopyOption.REPLACE_EXISTING);
            return jarPath.toFile();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    static String cassandraResourceName(@NotNull String label)
    {
        return "/bridges/" + label + ".jar";
    }

    @NotNull
    static String bridgeResourceName(@NotNull String label)
    {
        return jarResourceName(label, "bridge");
    }

    @NotNull
    static String typesResourceName(@NotNull String label)
    {
        return jarResourceName(label, "types");
    }

    static String jarResourceName(String... parts)
    {
        return "/bridges/" + String.join("-", parts) + ".jar";
    }
}
