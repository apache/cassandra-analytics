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

package org.apache.cassandra.spark.data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * A helper class for the CassandraBulkDataSource
 */
public final class CassandraDataSourceHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataSourceHelper.class);
    public static final String CACHE_DATA_LAYER_KEY = "cacheDataLayer";
    private static Cache<Map<String, String>, CassandraDataLayer> cassandraDataLayerCache;

    public static final int CACHE_HOURS = 12;

    private CassandraDataSourceHelper()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static DataLayer getDataLayer(
            Map<String, String> options,
            BiConsumer<CassandraDataLayer, ClientConfig> initializeDataLayerFn)
    {
        ClientConfig config = ClientConfig.create(options);

        if (MapUtils.getBoolean(options, CACHE_DATA_LAYER_KEY, false))
        {
            // If cacheDataLayer=true, only generate a new CassandraDataLayer.
            // If any of the DataSourceOptions have changed otherwise use previously cached value.
            Map<String, String> key = new HashMap<>(options);
            // Exclude createSnapshot as user may change createSnapshot=false for a snapshotName already created
            key.remove(ClientConfig.CREATE_SNAPSHOT_KEY);

            Cache<Map<String, String>, CassandraDataLayer> cassandraDataLayerCache = getCassandraDataLayerCache();
            CassandraDataLayer cached;
            try
            {
                cached = cassandraDataLayerCache.get(key, () ->
                        // First thread wins
                        createAndInitCassandraDataLayer(config, options, initializeDataLayerFn, SparkContext.getOrCreate().getConf()));
            }
            catch (ExecutionException exception)
            {
                throw new RuntimeException("Unable to initialize CassandraDataLayer", exception);
            }

            // Return cached value
            return cached;
        }
        else
        {
            return createAndInitCassandraDataLayer(config, options, initializeDataLayerFn, SparkContext.getOrCreate().getConf());
        }
    }

    protected static Cache<Map<String, String>, CassandraDataLayer> getCassandraDataLayerCache()
    {
        if (cassandraDataLayerCache == null)
        {
            initCassandraDataSourceCache(Ticker.systemTicker());
        }
        return cassandraDataLayerCache;
    }

    /**
     * Builds and returns the {@link Cache} for the {@link CassandraDataLayer} instances
     * with the provided {@code ticker}
     *
     * @param ticker the ticker to use for the cache
     */
    protected static void initCassandraDataSourceCache(Ticker ticker)
    {
        cassandraDataLayerCache = CacheBuilder
                .newBuilder()
                .ticker(ticker)
                .expireAfterWrite(CACHE_HOURS, TimeUnit.HOURS)
                .removalListener((RemovalListener<Map<String, String>, CassandraDataLayer>) notification ->
                        LOGGER.debug("Removed entry '{}' from CassandraDataSourceCache", notification.getValue()))
                .build();
    }

    protected static CassandraDataLayer createAndInitCassandraDataLayer(
            ClientConfig config,
            Map<String, String> options,
            BiConsumer<CassandraDataLayer, ClientConfig> initializeDataLayerFn,
            SparkConf conf)
    {
        CassandraDataLayer dataLayer = new CassandraDataLayer(config,
                                                              Sidecar.ClientConfig.create(options),
                                                              SslConfig.create(options));
        initializeDataLayerFn.accept(dataLayer, config);

        dataLayer.startupValidate();


        return dataLayer;
    }
}
