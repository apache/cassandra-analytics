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

package org.apache.cassandra.spark.bulkwriter.coordinatedwrite;

import java.util.List;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provider for multiple ClusterInfo and lookup
 */
public interface MultiClusterInfoProvider
{
    /**
     * @return the list of ClusterInfo
     */
    List<ClusterInfo> clusters();

    /**
     * Look up a ClusterInfo based on clusterId
     * @param clusterId cluster id
     * @return the ClusterInfo associated with the clusterId, or null if not found
     */
    @Nullable
    ClusterInfo cluster(@NotNull String clusterId);
}
