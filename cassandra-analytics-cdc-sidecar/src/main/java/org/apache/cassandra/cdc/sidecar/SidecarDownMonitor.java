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

package org.apache.cassandra.cdc.sidecar;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

/**
 * This interface provides information about the health of other Sidecar instances.
 * CDC attempts to read from all replicas and is slowed by retrying requests on Sidecar instances that are already known to be DOWN,
 * therefore we skip DOWN instances and only retry when they are known to be healthy again via some asynchronous mechanism.
 */
public interface SidecarDownMonitor
{
    SidecarDownMonitor STUB = new SidecarDownMonitor()
    {
        @Override
        public boolean isDown(CassandraInstance instance)
        {
            return false;
        }

        @Override
        public void hintDown(CassandraInstance instance)
        {

        }
    };

    /**
     * @param instance the CassandraInstance
     * @return true if the CassandraInstance is known to be not DOWN.
     */
    default boolean isUp(CassandraInstance instance)
    {
        return !isDown(instance);
    }

    /**
     * @param instance the CassandraInstance
     * @return true if the CassandraInstance is known to be DOWN.
     */
    boolean isDown(CassandraInstance instance);

    /**
     * Pass a hint down to the implementation that the CassandraInstance might be DOWN.
     * It is up to the implementation to take this as canonical or take further action to detect the health.
     *
     * @param instance the CassandraInstance
     */
    void hintDown(CassandraInstance instance);
}
