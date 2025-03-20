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

package org.apache.cassandra.cdc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;

/**
 * Caches Cassandra CqlField.CqlType objects, so they don't need to be re-created everytime. Keyed on keyspace and type to permit per keyspace UDT definitions.
 */
public class TypeCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeCache.class);
    protected volatile Cache<KeyspaceTypeKey, CqlField.CqlType> cqlTypeCache = null; // volatile needed for lazy-initialization
    private static final int CACHE_CAPACITY = 1000;
    private static final ConcurrentHashMap<CassandraVersion, TypeCache> VERSION_TYPE_CACHE = new ConcurrentHashMap<>(2);

    private final Supplier<CassandraTypes> cassandraTypesSupplier;

    protected TypeCache(Supplier<CassandraTypes> cassandraTypesSupplier)
    {
        this.cassandraTypesSupplier = cassandraTypesSupplier;
    }

    public CqlField.CqlType getType(String keyspace, String typeString)
    {
        maybeInit();
        CqlField.CqlType result;
        KeyspaceTypeKey key = KeyspaceTypeKey.of(keyspace, typeString);
        try
        {
            result = cqlTypeCache.get(key, () -> getTypes().parseType(keyspace, typeString));
        }
        catch (CacheLoader.InvalidCacheLoadException | ExecutionException e)
        {
            LOGGER.warn("Unable to get the CQL type from cache.", e);
            result = getTypes().parseType(typeString);
            if (result == null)
            {
                throw new RuntimeException("Unable to parse type: " + typeString);
            }
            cqlTypeCache.put(key, result);
        }
        return result;
    }

    public CassandraTypes getTypes()
    {
        return cassandraTypesSupplier.get();
    }

    public static TypeCache get(CassandraVersion version)
    {
        return VERSION_TYPE_CACHE
               .compute(version,
                        (key, previous) ->
                        new TypeCache(() -> CdcBridgeFactory.get(key).cassandraTypes())
               );
    }

    private void maybeInit()
    {
        if (cqlTypeCache != null)
        {
            return;
        }

        synchronized (this)
        {
            if (cqlTypeCache != null)
            {
                return;
            }

            // using a manual cache since the parser is version dependent, see getCassandraBridge()
            cqlTypeCache = CacheBuilder.newBuilder()
                                       .initialCapacity(CACHE_CAPACITY / 2)
                                       .maximumSize(CACHE_CAPACITY)
                                       .removalListener(notification -> {
                                           // Log at the info level: the event is not expected, but could happen.
                                           // With the logs, we can check how frequent it happens.
                                           LOGGER.info("Type is evicted from cache. type='{}'", notification.getKey());
                                       })
                                       .build();
        }
    }
}
