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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.validation.StartupValidatable;

public interface ClusterInfo extends StartupValidatable, Serializable
{
    void refreshClusterInfo();

    TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached);

    String getLowestCassandraVersion();

    Map<RingInstance, InstanceAvailability> getInstanceAvailability();

    boolean instanceIsAvailable(RingInstance ringInstance);

    InstanceState getInstanceState(RingInstance instance);

    Partitioner getPartitioner();

    void checkBulkWriterIsEnabledOrThrow();

    TimeSkewResponse getTimeSkew(List<RingInstance> replicas);

    String getKeyspaceSchema(boolean cached);
}
