/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.testing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.jetbrains.annotations.Nullable;

public class TemporaryCqlSessionProvider extends CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TemporaryCqlSessionProvider.class);
    private Session localSession;
    private final InetSocketAddress inet;
    private final NettyOptions nettyOptions;
    private final ReconnectionPolicy reconnectionPolicy;
    private final List<InetSocketAddress> addresses = new ArrayList<>();

    public TemporaryCqlSessionProvider(InetSocketAddress target, NettyOptions options, List<InetSocketAddress> addresses)
    {
        super(target, options);
        inet = target;
        nettyOptions = options;
        reconnectionPolicy = new ExponentialReconnectionPolicy(100, 1000);
        this.addresses.addAll(addresses);
    }

    @Override
    public synchronized @Nullable Session localCql()
    {
        Cluster cluster = null;
        try
        {
            if (localSession == null)
            {
                logger.info("Connecting to {}", inet);
                cluster = Cluster.builder()
                                 .addContactPointsWithPorts(addresses)
                                 .withReconnectionPolicy(reconnectionPolicy)
                                 .withoutMetrics()
                                 // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                                 // event thread pools for each we have the override
                                 .withNettyOptions(nettyOptions)
                                 .build();
                localSession = cluster.connect();
                logger.info("Successfully connected to Cassandra instance!");
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to reach Cassandra", e);
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Exception ex)
                {
                    logger.error("Failed to close cluster in cleanup", ex);
                }
            }
        }
        return localSession;
    }

    @Override
    public Session close()
    {
        Session localSession;
        synchronized (this)
        {
            localSession = this.localSession;
            this.localSession = null;
        }

        if (localSession != null)
        {
            try
            {
                localSession.getCluster().closeAsync().get(1, TimeUnit.MINUTES);
                localSession.closeAsync().get(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e)
            {
                logger.warn("Unable to close session after 1 minute for provider {}", this, e);
            }
            catch (ExecutionException e)
            {
                throw propagateCause(e);
            }
        }
        return localSession;
    }

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();
        if (cause instanceof Error)
        {
            throw (Error) cause;
        }
        else if (cause instanceof DriverException)
        {
            throw ((DriverException) cause).copy();
        }
        else
        {
            throw new DriverInternalError("Unexpected exception thrown", cause);
        }
    }
}
