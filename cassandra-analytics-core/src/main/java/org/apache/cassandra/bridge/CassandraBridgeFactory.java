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

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.jetbrains.annotations.NotNull;

public final class CassandraBridgeFactory extends BaseCassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge and SparkSqlTypeConverter
    private static final Map<String, VersionSpecificBridge> CASSANDRA_BRIDGES =
    new ConcurrentHashMap<>(CassandraVersion.values().length);

    public static class VersionSpecificBridge
    {
        public final CassandraBridge cassandraBridge;
        public final SparkSqlTypeConverter sparkSqlTypeConverter;

        public VersionSpecificBridge(CassandraBridge cassandraBridge, SparkSqlTypeConverter sparkSqlTypeConverter)
        {
            this.cassandraBridge = cassandraBridge;
            this.sparkSqlTypeConverter = sparkSqlTypeConverter;
        }
    }

    private CassandraBridgeFactory()
    {
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
        Preconditions.checkNotNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create).cassandraBridge;
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
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create).sparkSqlTypeConverter;
    }

    @NotNull
    private static String sparkSqlResourceName(@NotNull String label)
    {
        return jarResourceName(label, "sparksql");
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static VersionSpecificBridge create(@NotNull String label)
    {
        try
        {
            ClassLoader loader = buildClassLoader(cassandraResourceName(label), bridgeResourceName(label), typesResourceName(label), sparkSqlResourceName(label));
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            CassandraBridge bridgeInstance = constructor.newInstance();

            Class<SparkSqlTypeConverter> typeConverter = (Class<SparkSqlTypeConverter>)
                                                         loader
                                                         .loadClass(SparkSqlTypeConverter.IMPLEMENTATION_FQCN);
            Constructor<SparkSqlTypeConverter> typeConverterConstructor = typeConverter.getConstructor();
            SparkSqlTypeConverter typeConverterInstance = typeConverterConstructor.newInstance();
            return new VersionSpecificBridge(bridgeInstance, typeConverterInstance);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
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
