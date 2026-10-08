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

import org.apache.cassandra.spark.data.S3CassandraDataLayer;
import org.apache.cassandra.spark.data.SSTableTokenIndex;
import org.apache.spark.broadcast.Broadcast;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class S3CassandraPrebuiltReadContext implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3CassandraPrebuiltReadContext.class);

    private final String id;
    private final S3CassandraDataLayer dataLayer;
    @Nullable
    private final Broadcast<SSTableTokenIndex> sstableTokenIndexBroadcast;
    private boolean closed;

    S3CassandraPrebuiltReadContext(String id,
                                   S3CassandraDataLayer dataLayer,
                                   @Nullable Broadcast<SSTableTokenIndex> sstableTokenIndexBroadcast)
    {
        this.id = id;
        this.dataLayer = dataLayer;
        this.sstableTokenIndexBroadcast = sstableTokenIndexBroadcast;
    }

    public String id()
    {
        return id;
    }

    S3CassandraDataLayer dataLayer()
    {
        return dataLayer;
    }

    @Nullable
    Broadcast<SSTableTokenIndex> sstableTokenIndexBroadcast()
    {
        return sstableTokenIndexBroadcast;
    }

    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        try
        {
            S3CassandraPrebuiltReadContextRegistry.remove(id);
            if (sstableTokenIndexBroadcast != null)
            {
                sstableTokenIndexBroadcast.destroy(false);
            }
        }
        finally
        {
            dataLayer.close();
        }
        LOGGER.info("Closed S3 Cassandra prebuilt read context id={}", id);
    }
}
