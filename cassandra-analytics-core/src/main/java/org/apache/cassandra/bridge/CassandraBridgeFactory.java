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
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.jetbrains.annotations.NotNull;

public final class CassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge and SparkSqlTypeConverter
    private static final Map<String, Pair<CassandraBridge, SparkSqlTypeConverter>> CASSANDRA_BRIDGES =
    new ConcurrentHashMap<>(CassandraVersion.values().length);

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
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create).getLeft();
    }

    @NotNull
    public static SparkSqlTypeConverter getSparkSql(@NotNull CassandraVersionFeatures features)
    {
        return getSparkSql(getCassandraVersion(features));
    }

    @NotNull
    public static SparkSqlTypeConverter getSparkSql(@NotNull CassandraBridge bridge)
    {
        return getSparkSql(bridge.getVersion());
    }

    @NotNull
    public static SparkSqlTypeConverter getSparkSql(@NotNull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        Preconditions.checkNotNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create).getRight();
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
            URL locator = CassandraBridgeFactory.class.getResource(cassandraResourceName);
            if (locator == null)
            {
                throw new IllegalStateException("Missing Cassandra implementation for version " + version);
            }

            String bridgeResourceName = bridgeResourceName(jarBaseName);
            locator = CassandraBridgeFactory.class.getResource(bridgeResourceName);

            if (locator == null)
            {
                throw new IllegalStateException("Missing Cassandra bridge implementation for version " + version);
            }
        }
    }

    @NotNull
    private static String cassandraResourceName(@NotNull String label)
    {
        return "/bridges/" + label + ".jar";
    }

    @NotNull
    private static String bridgeResourceName(@NotNull String label)
    {
        return jarResourceName(label, "bridge");
    }

    @NotNull
    private static String typesResourceName(@NotNull String label)
    {
        return jarResourceName(label, "types");
    }

    @NotNull
    private static String sparkSqlResourceName(@NotNull String label)
    {
        return jarResourceName(label, "sparksql");
    }

    private static String jarResourceName(String... parts)
    {
        return "/bridges/" + String.join("-", parts) + ".jar";
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static Pair<CassandraBridge, SparkSqlTypeConverter> create(@NotNull String label)
    {
        try
        {
            String name = cassandraResourceName(label);
            InputStream contents = CassandraBridgeFactory.class.getResourceAsStream(name);
            File casandraJar = Files.createTempFile(null, ".jar").toFile();
            FileUtils.copyInputStreamToFile(contents, casandraJar);

            name = bridgeResourceName(label);
            contents = CassandraBridgeFactory.class.getResourceAsStream(name);
            File bridgeJar = Files.createTempFile(null, ".jar").toFile();
            FileUtils.copyInputStreamToFile(contents, bridgeJar);

            name = typesResourceName(label);
            contents = CassandraBridgeFactory.class.getResourceAsStream(name);
            File typesJar = Files.createTempFile(null, ".jar").toFile();
            FileUtils.copyInputStreamToFile(contents, typesJar);

            name = sparkSqlResourceName(label);
            contents = CassandraBridgeFactory.class.getResourceAsStream(name);
            File sparkSqlJar = Files.createTempFile(null, ".jar").toFile();
            FileUtils.copyInputStreamToFile(contents, sparkSqlJar);

            URL[] urls = {casandraJar.toURI().toURL(), bridgeJar.toURI().toURL(), typesJar.toURI().toURL(), sparkSqlJar.toURI().toURL()};
            ClassLoader loader = new PostDelegationClassLoader(urls, Thread.currentThread().getContextClassLoader());
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass("org.apache.cassandra.bridge.CassandraBridgeImplementation");
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            CassandraBridge bridgeInstance = constructor.newInstance();

            Class<SparkSqlTypeConverter> typeConverter = (Class<SparkSqlTypeConverter>)
                                                  loader
                                                  .loadClass("org.apache.cassandra.spark.data.converter.SparkSqlTypeConverterImplementation");
            Constructor<SparkSqlTypeConverter> typeConverterConstructor = typeConverter.getConstructor();
            SparkSqlTypeConverter typeConverterInstance = typeConverterConstructor.newInstance();
            return Pair.of(bridgeInstance, typeConverterInstance);
        }
        catch (IOException | ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }

    /***
     * Returns the quoted name when the {@code quoteIdentifiers} parameter is {@code true} <i>AND</i> the
     * {@code unquotedName} needs to be quoted (i.e. it uses mixed case, or it is a Cassandra reserved word).
     * @param bridge the Cassandra bridge
     * @param quoteIdentifiers whether identifiers should be quoted
     * @param unquotedName the unquoted name to maybe be quoted
     * @return the quoted name when the conditions are met
     */
    public static String maybeQuotedIdentifier(CassandraBridge bridge, boolean quoteIdentifiers, String unquotedName)
    {
        return quoteIdentifiers ? bridge.maybeQuoteIdentifier(unquotedName) : unquotedName;
    }
}
