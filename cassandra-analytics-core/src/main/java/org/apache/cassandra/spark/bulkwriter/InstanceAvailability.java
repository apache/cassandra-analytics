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

public enum InstanceAvailability
{
    AVAILABLE("is available"),
    UNAVAILABLE_DOWN("is down"),
    /**
     * This node is listed in the cluster block list
     */
    UNAVAILABLE_BLOCKED("is blocked"),
    /**
     * This node is JOINING, and it's gossip state is BOOT_REPLACE, which means it's a host replacement.
     * We can support SBW jobs with host replacements ongoing, but we treat them as UNAVAILABLE so the
     * job's success or failure depends on its consistency level requirements.
     */
    UNAVAILABLE_REPLACEMENT("is a host replacement"),
    /**
     * INVALID_STATE is true when a node is in an unknown state
     */
    INVALID_STATE("is in an invalid state");

    private final String message;

    InstanceAvailability(String message)
    {
        this.message = message;
    }

    public String getMessage()
    {
        return message;
    }
}
