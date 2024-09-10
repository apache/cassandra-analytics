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

import org.apache.cassandra.spark.common.model.NodeState;
import org.apache.cassandra.spark.common.model.NodeStatus;

/**
 * Availability of a node to take writes
 */
public enum WriteAvailability
{
    AVAILABLE("is available"),
    UNAVAILABLE_DOWN("is down"),
    /**
     * INVALID_STATE is true when a node is in an unknown state
     */
    INVALID_STATE("is in an invalid state");

    private final String message;

    WriteAvailability(String message)
    {
        this.message = message;
    }

    public String getMessage()
    {
        return message;
    }

    public static WriteAvailability determineFromNodeState(NodeState nodeState, NodeStatus nodeStatus)
    {
        if (nodeStatus != NodeStatus.UP)
        {
            return WriteAvailability.UNAVAILABLE_DOWN;
        }
        // pending and normal nodes are available for write
        if (nodeState == NodeState.NORMAL || nodeState.isPending)
        {
            return WriteAvailability.AVAILABLE;
        }
        // If it's not one of the above, it's INVALID.
        return WriteAvailability.INVALID_STATE;
    }
}
