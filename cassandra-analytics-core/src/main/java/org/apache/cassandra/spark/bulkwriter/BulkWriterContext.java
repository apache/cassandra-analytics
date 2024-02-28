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

import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.bridge.CassandraBridge;

public interface BulkWriterContext extends Serializable
{
    ClusterInfo cluster();

    JobInfo job();

    JobStatsPublisher jobStats();

    SchemaInfo schema();

    DataTransferApi transfer();

    CassandraBridge bridge();

    // NOTE: This interface intentionally does *not* implement AutoClosable as Spark can close Broadcast variables
    //       that implement AutoClosable while they are still in use, causing the underlying object to become unusable
    void shutdown();
}
