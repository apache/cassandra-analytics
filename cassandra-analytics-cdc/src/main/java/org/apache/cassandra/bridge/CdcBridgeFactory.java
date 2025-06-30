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

import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class CdcBridgeFactory extends BaseCassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge and SparkSqlTypeConverter
    private static final Map<String, VersionSpecificBridge> CASSANDRA_BRIDGES =
    new ConcurrentHashMap<>(CassandraVersion.values().length);

    public static class VersionSpecificBridge
    {
        public final CassandraBridge cassandraBridge;
        public final CdcBridge cdcBridge;
        @Nullable
        final CqlToAvroSchemaConverter avroSchemaConverter;

        public VersionSpecificBridge(CassandraBridge cassandraBridge,
                                     CdcBridge cdcBridge,
                                     @Nullable CqlToAvroSchemaConverter avroSchemaConverter)
        {
            this.cassandraBridge = cassandraBridge;
            this.cdcBridge = cdcBridge;
            this.avroSchemaConverter = avroSchemaConverter;
        }
    }

    private CdcBridgeFactory()
    {
        super();
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @Nonnull
    public static CassandraBridge get(@Nonnull String version)
    {
        return get(getCassandraVersion(version));
    }

    @Nonnull
    public static CassandraBridge get(@Nonnull CassandraVersionFeatures features)
    {
        return get(getCassandraVersion(features));
    }

    private static CdcBridgeFactory.VersionSpecificBridge getVersionSpecificBridge(@Nonnull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        if (jarBaseName == null)
        {
            throw new NullPointerException("Cassandra version " + version + " is not supported");
        }
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CdcBridgeFactory::create);
    }

    @Nonnull
    public static CassandraBridge get(@Nonnull CassandraVersion version)
    {
        return getVersionSpecificBridge(version).cassandraBridge;
    }

    @Nonnull
    public static CdcBridge getCdcBridge(@Nonnull CassandraVersionFeatures features)
    {
        return getCdcBridge(getCassandraVersion(features));
    }

    @Nonnull
    public static CdcBridge getCdcBridge(@Nonnull CassandraBridge bridge)
    {
        return getCdcBridge(bridge.getVersion());
    }

    @Nonnull
    public static CdcBridge getCdcBridge(@Nonnull CassandraVersion version)
    {
        return getVersionSpecificBridge(version).cdcBridge;
    }

    @Nonnull
    public static CqlToAvroSchemaConverter getCqlToAvroSchemaConverter(@Nonnull CassandraBridge bridge)
    {
        return getCqlToAvroSchemaConverter(bridge.getVersion());
    }

    @Nullable
    public static CqlToAvroSchemaConverter getCqlToAvroSchemaConverter(@Nonnull CassandraVersion version)
    {
        return getVersionSpecificBridge(version).avroSchemaConverter;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static VersionSpecificBridge create(@Nonnull String label)
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

            return new VersionSpecificBridge(bridgeInstance, cdcBridgeInstance, cqlToAvroSchemaConverter);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }
}
