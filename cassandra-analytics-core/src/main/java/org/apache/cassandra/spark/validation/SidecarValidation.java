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

import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.common.data.HealthResponse;

/**
 * A strartup validation that checks the connectivity and health of Sidecar
 */
public class SidecarValidation implements StartupValidation
{
    private static final int TIMEOUT_SECONDS = 30;

    private final SidecarClient sidecar;

    public SidecarValidation(SidecarClient sidecar)
    {
        this.sidecar = sidecar;
    }

    @Override
    public void validate()
    {
        HealthResponse health;
        try
        {
            health = sidecar.sidecarHealth().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (Throwable throwable)
        {
            throw new RuntimeException("Sidecar is unreachable", throwable);
        }
        if (!health.isOk())
        {
            throw new RuntimeException("Sidecar is unhealthy: " + health.status());
        }
    }
}
