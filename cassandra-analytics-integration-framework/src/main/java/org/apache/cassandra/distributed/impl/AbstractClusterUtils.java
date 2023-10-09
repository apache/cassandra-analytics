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

package org.apache.cassandra.distributed.impl;

/**
 * Utility class to interact with protected methods in AbstractCluster
 */
public class AbstractClusterUtils
{
    private AbstractClusterUtils()
    {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    /**
     * Creates the instance configuration object for the specified node
     * @param cluster the cluster to which this instance will belong
     * @param nodeNumber the number of the node for which a configuration should be created
     * @return the instance configuration for the specified node number
     */
    public static InstanceConfig createInstanceConfig(AbstractCluster cluster, int nodeNumber)
    {
        return cluster.createInstanceConfig(nodeNumber);
    }
}
