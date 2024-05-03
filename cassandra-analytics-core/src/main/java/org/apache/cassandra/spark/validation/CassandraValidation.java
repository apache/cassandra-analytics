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

package org.apache.cassandra.spark.validation;

import java.util.concurrent.TimeUnit;

import o.a.c.sidecar.client.shaded.common.response.HealthResponse;
import org.apache.cassandra.sidecar.client.SidecarClient;

/**
 * A startup validation that checks the connectivity and health of Cassandra
 */
public class CassandraValidation implements StartupValidation
{
    private final SidecarClient sidecar;
    private final int timeoutSeconds;

    public CassandraValidation(SidecarClient sidecar, int timeoutSeconds)
    {
        this.sidecar = sidecar;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public void validate()
    {
        HealthResponse health;
        try
        {
            health = sidecar.cassandraHealth().get(timeoutSeconds, TimeUnit.SECONDS);
        }
        catch (Throwable throwable)
        {
            throw new RuntimeException("Sidecar is unreachable", throwable);
        }
        if (!health.isOk())
        {
            throw new RuntimeException("Cassandra is not healthy: " + health.status());
        }
    }
}
