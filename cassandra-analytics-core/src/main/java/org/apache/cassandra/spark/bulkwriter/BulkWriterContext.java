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

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraCoordinatedBulkWriterContext;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.bridge.CassandraBridge;

/**
 * Context for bulk write operations, providing access to cluster, job, schema, and transport information.
 * <p>
 * Serialization Architecture:
 * This interface does NOT extend Serializable. BulkWriterContext instances are never broadcast to executors.
 * Instead, {@link BulkWriterConfig} is broadcast, and executors reconstruct BulkWriterContext instances
 * from the config using the factory method {@link #from(BulkWriterConfig)}.
 * <p>
 * The implementations ({@link CassandraBulkWriterContext}, {@link CassandraCoordinatedBulkWriterContext})
 * do NOT have serialVersionUID fields as they are never serialized.
 */
public interface BulkWriterContext
{
    ClusterInfo cluster();

    JobInfo job();

    JobStatsPublisher jobStats();

    SchemaInfo schema();

    CassandraBridge bridge();

    // NOTE: This interface intentionally does *not* implement AutoClosable as Spark can close Broadcast variables
    //       that implement AutoClosable while they are still in use, causing the underlying object to become unusable
    void shutdown();

    TransportContext transportContext();

    /**
     * Factory method to create a BulkWriterContext from a BulkWriterConfig on executors.
     * This method reconstructs context instances on executors from the broadcast configuration.
     * The driver creates contexts directly using constructors, not this method.
     *
     * @param config the immutable configuration object broadcast from driver
     * @return a new BulkWriterContext instance
     */
    static BulkWriterContext from(BulkWriterConfig config)
    {
        BulkSparkConf conf = config.getConf();
        if (conf.isCoordinatedWriteConfigured())
        {
            return new CassandraCoordinatedBulkWriterContext(config);
        }
        else
        {
            return new CassandraBulkWriterContext(config);
        }
    }
}
