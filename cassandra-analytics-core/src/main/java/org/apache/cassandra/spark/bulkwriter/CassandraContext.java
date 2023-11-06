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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.spark.validation.CassandraValidation;
import org.apache.cassandra.spark.validation.SidecarValidation;
import org.apache.cassandra.spark.validation.StartupValidatable;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.jetbrains.annotations.NotNull;

public class CassandraContext implements StartupValidatable, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraContext.class);
    @NotNull
    protected Set<? extends SidecarInstance> clusterConfig;
    private final BulkSparkConf conf;
    private final transient SidecarClient sidecarClient;

    protected CassandraContext(BulkSparkConf conf)
    {
        this.conf = conf;
        this.clusterConfig = createClusterConfig();
        this.sidecarClient = initializeSidecarClient(conf);
        LOGGER.debug("[{}] Created Cassandra Context", Thread.currentThread().getName());
    }

    public static CassandraContext create(BulkSparkConf conf)
    {
        return new CassandraContext(conf);
    }

    public Set<? extends SidecarInstance> getCluster()
    {
        return clusterConfig;
    }

    public void refreshClusterConfig()
    {
        // DO NOTHING
    }

    @Override
    public void close()
    {
        try
        {
            sidecarClient.close();
            LOGGER.debug("[{}] Closed Cassandra Context", Thread.currentThread().getName());
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Could not shut down CassandraContext.", throwable);
        }
    }

    protected SidecarClient initializeSidecarClient(BulkSparkConf conf)
    {
        return Sidecar.from(new SimpleSidecarInstancesProvider(new ArrayList<>(clusterConfig)), conf);
    }

    protected Set<? extends SidecarInstance> createClusterConfig()
    {
        return conf.sidecarInstances;
    }

    public SidecarClient getSidecarClient()
    {
        return sidecarClient;
    }

    public int sidecarPort()
    {
        return conf.getEffectiveSidecarPort();
    }

    protected BulkSparkConf conf()
    {
        return conf;
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        StartupValidator.instance().register(new SidecarValidation(sidecarClient));
        StartupValidator.instance().register(new CassandraValidation(sidecarClient));
        StartupValidator.instance().perform();
    }
}
