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
@SuppressWarnings("UnstableApiUsage")
public class TypeCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeCache.class);
    protected volatile Cache<KeyspaceTypeKey, CqlField.CqlType> cqlTypeCache = null; // volatile needed for lazy-initialization
    private static final int CACHE_CAPACITY = 1000;
    private static final ConcurrentHashMap<CassandraVersion, TypeCache> VERSION_TYPE_CACHE = new ConcurrentHashMap<>(2);

    private final Supplier<CassandraTypes> cassandraTypesSupplier;

    /** We have contention between instance level cache access and the global {@link #clear} method; we go ahead and
     * lock at the class level since the clearing operation is expected to happen very infrequently leaving cache
     * access uncontended and as predominantly a very fast null check.
     */
    private static final Object creationLock = new Object();

    protected TypeCache(Supplier<CassandraTypes> cassandraTypesSupplier)
    {
        this.cassandraTypesSupplier = cassandraTypesSupplier;
    }

    public CqlField.CqlType getType(String keyspace, String typeString)
    {
        Cache<KeyspaceTypeKey, CqlField.CqlType> localCache = maybeInit();
        CqlField.CqlType result;
        KeyspaceTypeKey key = KeyspaceTypeKey.of(keyspace, typeString);
        try
        {
            result = localCache.get(key, () -> getTypes().parseType(keyspace, typeString));
        }
        catch (CacheLoader.InvalidCacheLoadException | ExecutionException e)
        {
            LOGGER.warn("Unable to get the CQL type from cache.", e);
            result = getTypes().parseType(typeString);
            if (result == null)
            {
                throw new RuntimeException("Unable to parse type: " + typeString);
            }
            localCache.put(key, result);
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
               .computeIfAbsent(version,
                                key ->
                                new TypeCache(() -> CdcBridgeFactory.get(key).cassandraTypes())
               );
    }

    /**
     * Globally clears all cached TypeCache instances. Each entry's Guava cache is invalidated and
     * nullified to release references to CQL types (and transitively, bridge classloaders).
     *
     * This is a "do it once" kind of shutdown action; while _in theory_ it should be unnecessary (i.e. we let JVM
     * shutdown just nuke everything), in practice better to be deliberate about this, especially in test environments
     * where things are spun up and spun down and global resources hanging around can be problematic.
     */
    public static void clear()
    {
        synchronized (creationLock)
        {
            VERSION_TYPE_CACHE.forEach((version, typeCache) -> {
                if (typeCache.cqlTypeCache != null)
                {
                    typeCache.cqlTypeCache.invalidateAll();
                    typeCache.cqlTypeCache = null;
                }
            });
            VERSION_TYPE_CACHE.clear();
        }
    }

    /**
     * We use a manual cache since the parser is version dependent
     * see {@link org.apache.cassandra.cdc.test.TestCdcBridgeProvider#getCassandraBridge}
     *
     * Since there's a real risk of race between this and {@link #clear}, we lock on {@link #creationLock} to enforce
     * clear boundaries between the two.
     *
     * Since most calls here will jump right into the "cqlTypeCache != null" path, the risk of contention on a simple
     * variable null check should be very _very_ low. We can't rely on {@link #cqlTypeCache} being volatile since we
     * have the "check for null then return" pattern and we could race with teardown leading to NPE.
     *
     * @return Reference to the cache so subsequent clear calls don't NPE us.
     */
    private Cache<KeyspaceTypeKey, CqlField.CqlType> maybeInit()
    {
        synchronized (creationLock)
        {
            if (cqlTypeCache != null)
            {
                return cqlTypeCache;
            }

            cqlTypeCache = CacheBuilder.newBuilder()
                    .initialCapacity(CACHE_CAPACITY / 2)
                    .maximumSize(CACHE_CAPACITY)
                    .removalListener(notification -> {
                        // Log at the info level: the event is not expected, but could happen.
                        // With the logs, we can check how frequent it happens.
                        LOGGER.info("Type is evicted from cache. type='{}'", notification.getKey());
                    })
                    .build();
            return cqlTypeCache;
        }
    }
}
