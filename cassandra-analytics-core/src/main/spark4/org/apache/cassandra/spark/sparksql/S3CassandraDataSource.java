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

package org.apache.cassandra.spark.sparksql;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.S3CassandraDataLayer;
import org.apache.cassandra.spark.data.S3DataSourceClientConfig;
import org.apache.cassandra.spark.data.SSTableTokenIndex;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CassandraDataSource extends CassandraTableProvider implements SessionConfigSupport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3CassandraDataSource.class);

    /**
     * Driver-local handle produced by {@link S3CassandraTokenIndexPrebuilder}. This is intentionally
     * not a durable DataSource option: saved/replayed plans and remote client/server boundaries must
     * rebuild the context in the JVM that plans the read.
     */
    public static final String READ_CONTEXT_ID_KEY = "s3CassandraReadContextId";

    @Override
    public String shortName()
    {
        return "s3CassandraBulkRead";
    }

    /*
    * Spark will propagate session configs spark.datasource.s3CassandraBulkRead.* to the getTable options.
    * */
    @Override
    public String keyPrefix()
    {
        return shortName();
    }

    @Override
    public DataLayer getDataLayer(CaseInsensitiveStringMap options)
    {
        S3CassandraPrebuiltReadContext context = getPrebuiltReadContext(options);
        if (context != null)
        {
            return context.dataLayer();
        }
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);
        return new S3CassandraDataLayer(config);
    }

    @Nullable
    @Override
    public Broadcast<SSTableTokenIndex> getSSTableTokenIndexBroadcast(CaseInsensitiveStringMap options)
    {
        S3CassandraPrebuiltReadContext context = getPrebuiltReadContext(options);
        return context == null ? null : context.sstableTokenIndexBroadcast();
    }

    @Nullable
    private S3CassandraPrebuiltReadContext getPrebuiltReadContext(CaseInsensitiveStringMap options)
    {
        String contextId = options.get(READ_CONTEXT_ID_KEY);
        if (contextId == null || contextId.trim().isEmpty())
        {
            return null;
        }
        S3CassandraPrebuiltReadContext context = S3CassandraPrebuiltReadContextRegistry.get(contextId);
        if (context == null)
        {
            throw new IllegalArgumentException("No S3 Cassandra prebuilt read context found for id=" + contextId);
        }
        LOGGER.info("Resolved S3 Cassandra prebuilt read context id={}", contextId);
        return context;
    }
}
